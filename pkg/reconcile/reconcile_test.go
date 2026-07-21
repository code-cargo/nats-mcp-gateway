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
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
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

func newStack(t *testing.T) *stack {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, MaxPayload: 8 * 1024 * 1024}
	s, err := server.NewServer(opts)
	require.NoError(t, err)
	go s.Start()
	require.True(t, s.ReadyForConnections(5*time.Second))
	t.Cleanup(s.Shutdown)

	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

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
	st.rec = New(ws, st.pool, nil)
	return st
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
	st := newStack(t)

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
	st := newStack(t)
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

func fakeSrv(extraEnv map[string]string) config.Server {
	return config.Server{
		Protocol:  mcpspec.ProtocolVersion,
		Transport: "stdio",
		Command:   os.Args[0],
		Env:       extraEnv,
	}
}
