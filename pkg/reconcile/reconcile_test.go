//   Copyright 2026 BoxBuild Inc DBA CodeCargo
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package reconcile

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/proxy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakemcp.EnvFlag) == "1" {
		fakemcp.Main()
		return
	}
	os.Exit(m.Run())
}

func srv(command string, args ...string) config.Server {
	return config.Server{
		Protocol:  mcpspec.ProtocolVersion,
		Transport: "stdio",
		Command:   command,
		Args:      args,
	}
}

func cfg(servers map[string]config.Server) *config.Config {
	return &config.Config{Servers: servers}
}

func TestDiffServers(t *testing.T) {
	base := cfg(map[string]config.Server{
		"a": srv("cmd-a"),
		"b": srv("cmd-b"),
	})
	tests := []struct {
		name              string
		old, next         *config.Config
		add, rem, changed []string
	}{
		{"first apply is all added", nil, base, []string{"a", "b"}, nil, nil},
		{"no change", base, cfg(map[string]config.Server{"a": srv("cmd-a"), "b": srv("cmd-b")}), nil, nil, nil},
		{"add one", base, cfg(map[string]config.Server{"a": srv("cmd-a"), "b": srv("cmd-b"), "c": srv("cmd-c")}), []string{"c"}, nil, nil},
		{"remove one", base, cfg(map[string]config.Server{"a": srv("cmd-a")}), nil, []string{"b"}, nil},
		{"env change", base, cfg(map[string]config.Server{
			"a": {Protocol: mcpspec.ProtocolVersion, Transport: "stdio", Command: "cmd-a", Env: map[string]string{"K": "v"}},
			"b": srv("cmd-b"),
		}), nil, nil, []string{"a"}},
		{"arg change", base, cfg(map[string]config.Server{"a": srv("cmd-a", "--flag"), "b": srv("cmd-b")}), nil, nil, []string{"a"}},
		{"remove all", base, cfg(map[string]config.Server{}), nil, []string{"a", "b"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := diffServers(tt.old, tt.next)
			assert.Equal(t, tt.add, d.Added, "added")
			assert.Equal(t, tt.rem, d.Removed, "removed")
			assert.Equal(t, tt.changed, d.Changed, "changed")
		})
	}
}

// stack wires embedded NATS + pool + proxy + wire + reconciler, the way the
// gateway command does, with the pool factory reading through the reconciler.
type stack struct {
	nc   *nats.Conn
	rec  *Reconciler
	ws   *wire.Server
	pool *backend.Pool
}

func newStack(t *testing.T, log *slog.Logger) *stack {
	t.Helper()
	nc, _ := natstest.Run(t, nil)

	st := &stack{nc: nc}
	st.pool = backend.NewPool(backend.PoolConfig{}, func(key backend.Key) (backend.Backend, error) {
		cur := st.rec.Current()
		sc := cur.Servers[key.Server]
		env := map[string]string{fakemcp.EnvFlag: "1"}
		for k, v := range sc.Env {
			env[k] = v
		}
		return &backend.StdioBackend{Command: sc.Command, Args: sc.Args, Env: env}, nil
	}, nil)
	t.Cleanup(st.pool.Shutdown)

	ws, err := wire.Serve(nc, wire.ServerConfig{KeepAlive: 50 * time.Millisecond}, proxy.New(st.pool, nil).Handler())
	require.NoError(t, err)
	st.ws = ws
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})
	st.rec = New(ws, st.pool, log)
	return st
}

// logCapture records what an operator would actually see, so a test can assert
// on a warning that has no other observable effect.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *logCapture) WithGroup(string) slog.Handler { return c }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

// warns renders each warn-or-worse record as message plus attributes — the
// whole line, because the field names the warning carries are half of what
// makes it actionable.
func (c *logCapture) warns() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, r := range c.records {
		if r.Level < slog.LevelWarn {
			continue
		}
		line := r.Message
		r.Attrs(func(a slog.Attr) bool {
			line += " " + a.String()
			return true
		})
		out = append(out, line)
	}
	return out
}

func (st *stack) call(t *testing.T, tenant, serverName, tool string) wire.Frame {
	t.Helper()
	c, err := wire.NewClient(st.nc, wire.ClientConfig{Tenant: tenant, Inactivity: 3 * time.Second})
	require.NoError(t, err)
	params, _ := json.Marshal(map[string]any{
		"name": tool, "arguments": map[string]any{},
		"_meta": map[string]any{mcpspec.MetaProtocolVersion: mcpspec.ProtocolVersion},
	})
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": "1", "method": "tools/call",
		"params": json.RawMessage(params),
	})
	s, err := c.Do(context.Background(), &wire.Request{
		Server: serverName, Method: "tools/call", Name: tool,
		ProtocolVersion: mcpspec.ProtocolVersion, Body: body,
	})
	require.NoError(t, err)
	var last wire.Frame
	for f := range s.C {
		last = f
	}
	return last
}

func pidOf(t *testing.T, f wire.Frame) float64 {
	t.Helper()
	require.Equal(t, wire.FrameEnd, f.Kind)
	m, err := jsonrpc.Decode(f.Body)
	require.NoError(t, err)
	var r struct {
		PID float64 `json:"pid"`
	}
	require.NoError(t, json.Unmarshal(m.Result, &r))
	return r.PID
}

func TestApplyAddChangeRemove(t *testing.T) {
	st := newStack(t, nil)

	// Boot with server "a".
	d, err := st.rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, d.Added)

	pidA := pidOf(t, st.call(t, "acme", "a", "echo"))

	// Reload: change a's env (forces respawn) and add b.
	d, err = st.rec.Apply(cfg(map[string]config.Server{
		"a": fakeSrv(map[string]string{"EXTRA": "1"}),
		"b": fakeSrv(nil),
	}))
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, d.Added)
	assert.Equal(t, []string{"a"}, d.Changed)

	// b serves now; a respawned with a new pid.
	assert.Equal(t, wire.FrameEnd, st.call(t, "acme", "b", "echo").Kind)
	assert.NotEqual(t, pidA, pidOf(t, st.call(t, "acme", "a", "echo")), "changed server must respawn")

	// Reload: drop a entirely.
	d, err = st.rec.Apply(cfg(map[string]config.Server{"b": fakeSrv(nil)}))
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, d.Removed)

	start := time.Now()
	f := st.call(t, "acme", "a", "echo")
	require.NotNil(t, f.Err)
	assert.Equal(t, wire.ErrCodeNoGateway, f.Err.Code)
	assert.Less(t, time.Since(start), 2*time.Second, "removed server must fail fast")
}

func TestApplyUnchangedIsNoop(t *testing.T) {
	st := newStack(t, nil)
	c := cfg(map[string]config.Server{"a": fakeSrv(nil)})
	_, err := st.rec.Apply(c)
	require.NoError(t, err)
	pid := pidOf(t, st.call(t, "acme", "a", "echo"))

	// Re-apply an identical config: no eviction, same process.
	d, err := st.rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)
	assert.True(t, d.Empty())
	assert.Equal(t, pid, pidOf(t, st.call(t, "acme", "a", "echo")), "no-op reload must not respawn")
}

// Editing the file's nats block and reloading changes nothing: the connection
// and the wire's scope were fixed before the Reconciler existed. Today that is
// also SILENT — the diff covers servers only, so an operator who drops
// nats.tenant and HUPs sees "config unchanged" while the gateway goes on
// serving every tenant.
func TestApplyWarnsWhenNatsBlockDriftsFromBoot(t *testing.T) {
	logs := &logCapture{}
	st := newStack(t, slog.New(logs))

	boot := config.NATS{Tenant: "acme", QueueGroup: "gw"}
	_, err := st.rec.Apply(&config.Config{NATS: boot, Servers: map[string]config.Server{"a": fakeSrv(nil)}})
	require.NoError(t, err)
	require.Empty(t, logs.warns(), "the first revision is what the process booted on")

	// A reload that leaves the block alone is not drift, however many times it
	// happens.
	_, err = st.rec.Apply(&config.Config{
		NATS:    boot,
		Servers: map[string]config.Server{"a": fakeSrv(nil), "b": fakeSrv(nil)},
	})
	require.NoError(t, err)
	require.Empty(t, logs.warns(), "an unchanged nats block must not warn on every reload")

	// Drop the tenant. Nothing in the server set moved, so this warning is the
	// only signal the operator gets that the scope did not narrow.
	d, err := st.rec.Apply(&config.Config{
		NATS:    config.NATS{QueueGroup: "gw"},
		Servers: map[string]config.Server{"a": fakeSrv(nil), "b": fakeSrv(nil)},
	})
	require.NoError(t, err)
	assert.True(t, d.Empty(), "the server set is all the diff covers, and it did not change")
	got := logs.warns()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "tenant")
	assert.NotContains(t, got[0], "queueGroup", "only the fields that actually moved")

	// A later, unrelated edit reports the drift again: it is measured against
	// what is RUNNING, so a second revision must not make it look settled.
	_, err = st.rec.Apply(&config.Config{
		NATS:    config.NATS{QueueGroup: "gw"},
		Servers: map[string]config.Server{"a": fakeSrv(nil)},
	})
	require.NoError(t, err)
	assert.Len(t, logs.warns(), 2, "the block is still diverged from the running connection")
}

// nats.url carries its password in the userinfo form and credsFile names a path
// worth nothing to an attacker but everything to a log scraper, so the warning
// names the fields that moved and never their values.
func TestNatsDriftWarningNamesFieldsNotValues(t *testing.T) {
	logs := &logCapture{}
	st := newStack(t, slog.New(logs))

	_, err := st.rec.Apply(&config.Config{NATS: config.NATS{URL: "nats://gw:s3cret@old:4222"}})
	require.NoError(t, err)
	_, err = st.rec.Apply(&config.Config{NATS: config.NATS{URL: "nats://gw:rotated@new:4222"}})
	require.NoError(t, err)

	got := logs.warns()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "url")
	assert.NotContains(t, got[0], "s3cret")
	assert.NotContains(t, got[0], "rotated")
}

// config.NATS is all plain-tagged strings today, so marshalling emits every key
// on both sides. The day a field is tagged omitempty it drops out of whichever
// side it is empty on, and comparing over the boot side's keys alone would miss
// it in exactly the direction that matters: booted without the field, reloaded
// with it set — the narrowing edit. The warning would still fire (the struct
// compare catches it) while naming nothing, so the comparison covers the union.
func TestDriftFieldsCoversKeysPresentOnOneSideOnly(t *testing.T) {
	boot := map[string]json.RawMessage{"same": []byte(`"x"`), "bootOnly": []byte(`"y"`)}
	next := map[string]json.RawMessage{"same": []byte(`"x"`), "nextOnly": []byte(`"z"`)}
	assert.Equal(t, []string{"bootOnly", "nextOnly"}, driftFields(boot, next))
}

func fakeSrv(extraEnv map[string]string) config.Server {
	return config.Server{
		Protocol:  mcpspec.ProtocolVersion,
		Transport: "stdio",
		Command:   os.Args[0],
		Env:       extraEnv,
	}
}
