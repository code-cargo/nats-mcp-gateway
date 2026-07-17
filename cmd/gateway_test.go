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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
	"github.com/code-cargo/nats-mcp-gateway/pkg/configsource"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/proxy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/reconcile"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakemcp.EnvFlag) == "1" {
		fakemcp.Main()
		return
	}
	os.Exit(m.Run())
}

// testLogger discards output (tests assert on behavior, not logs).
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// assembled is the gateway core wired exactly as runGateway does, minus the
// process-level signal handling — the shape an embedder would use, and what
// makes the fetch-mode reload path testable end to end.
type assembled struct {
	nc       *nats.Conn // gateway/responder connection (micro services live here)
	clientNC *nats.Conn // separate connection for wire client calls
	rec      *reconcile.Reconciler
}

func assemble(t *testing.T, nc *nats.Conn, url string) *assembled {
	t.Helper()
	// The wire client mutates/reads the connection's async error handler
	// (permission-violation fast-fail), which micro's per-service Add/Stop
	// also touches. In production the gateway and the shim are separate
	// processes with separate connections; mirror that here with a dedicated
	// client connection so the two don't race on one conn's callbacks.
	clientNC, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(clientNC.Close)

	a := &assembled{nc: nc, clientNC: clientNC}
	pool := backend.NewPool(backend.PoolConfig{}, func(key backend.Key) (backend.Backend, error) {
		cur := a.rec.Current()
		if cur == nil {
			return nil, fmt.Errorf("no config yet")
		}
		s, ok := cur.Servers[key.Server]
		if !ok {
			return nil, fmt.Errorf("unknown server %q", key.Server)
		}
		return buildBackend(key, s, testLogger()), nil
	}, testLogger())
	t.Cleanup(pool.Shutdown)

	ws, err := wire.Serve(nc, wire.ServerConfig{KeepAlive: 50 * time.Millisecond},
		proxy.New(pool, testLogger()).Handler())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})
	a.rec = reconcile.New(ws, pool, testLogger())
	return a
}

func (a *assembled) call(t *testing.T, serverName, tool string) wire.Frame {
	t.Helper()
	c, err := wire.NewClient(a.clientNC, wire.ClientConfig{Tenant: "demo", Inactivity: 3 * time.Second})
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

func mustPID(t *testing.T, f wire.Frame) float64 {
	t.Helper()
	require.Equal(t, wire.FrameEnd, f.Kind, "expected a result, got %+v", f)
	m, err := jsonrpc.Decode(f.Body)
	require.NoError(t, err)
	var r struct {
		PID float64 `json:"pid"`
	}
	require.NoError(t, json.Unmarshal(m.Result, &r))
	return r.PID
}

func fetchNATS(t *testing.T) (*nats.Conn, string) {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true}
	s, err := server.NewServer(opts)
	require.NoError(t, err)
	go s.Start()
	require.True(t, s.ReadyForConnections(5*time.Second))
	t.Cleanup(s.Shutdown)
	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc, s.ClientURL()
}

// mutableConfig serves a config over request/reply and publishes change events.
type mutableConfig struct {
	nc  *nats.Conn
	mu  sync.Mutex
	raw []byte
}

func fakeServerJSON(name string, env map[string]string) string {
	e, _ := json.Marshal(env)
	return fmt.Sprintf(`%q:{"protocol":%q,"transport":"stdio","command":%q,"env":%s}`,
		name, mcpspec.ProtocolVersion, os.Args[0], e)
}

func (mc *mutableConfig) set(entries ...string) {
	body := `{"servers":{`
	for i, e := range entries {
		if i > 0 {
			body += ","
		}
		body += e
	}
	body += `}}`
	mc.mu.Lock()
	mc.raw = []byte(body)
	mc.mu.Unlock()
}

func (mc *mutableConfig) serve(t *testing.T) {
	t.Helper()
	sub, err := mc.nc.Subscribe("mcp.cfg.request", func(m *nats.Msg) {
		mc.mu.Lock()
		defer mc.mu.Unlock()
		_ = m.Respond(mc.raw)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

func (mc *mutableConfig) publishChange(t *testing.T) {
	t.Helper()
	require.NoError(t, mc.nc.Publish("mcp.cfg.changed", nil))
}

// env with the fakemcp re-exec flag plus optional extras.
func fakeEnv(extra map[string]string) map[string]string {
	m := map[string]string{fakemcp.EnvFlag: "1"}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func TestGatewayFetchModeHotReload(t *testing.T) {
	nc, url := fetchNATS(t)
	a := assemble(t, nc, url)

	mc := &mutableConfig{nc: nc}
	mc.set(fakeServerJSON("a", fakeEnv(nil)))
	mc.serve(t)

	src := &configsource.NATS{
		Conn:           nc,
		RequestSubject: "mcp.cfg.request",
		EventSubject:   "mcp.cfg.changed",
		Refetch:        time.Hour, // isolate the event-driven path
		Logger:         testLogger(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = configsource.Run(ctx, testLogger(), src, func(cfg *config.Config) error {
			_, err := a.rec.Apply(cfg)
			return err
		})
	}()

	// Wait for the initial config to be applied, then call server a.
	waitServing(t, a, "a")
	pidA := mustPID(t, a.call(t, "a", "echo"))

	// Reload: change a's env (forces respawn) and add b; drop nothing yet.
	mc.set(
		fakeServerJSON("a", fakeEnv(map[string]string{"EXTRA": "1"})),
		fakeServerJSON("b", fakeEnv(nil)),
	)
	mc.publishChange(t)

	// b appears without a restart, and a respawns with a new pid.
	waitServing(t, a, "b")
	assert.NotEqual(t, pidA, mustPID(t, a.call(t, "a", "echo")), "changed server must respawn")

	// Reload: drop a entirely.
	mc.set(fakeServerJSON("b", fakeEnv(nil)))
	mc.publishChange(t)

	require.Eventually(t, func() bool {
		f := a.call(t, "a", "echo")
		return f.Err != nil && f.Err.Code == wire.ErrCodeNoGateway
	}, 5*time.Second, 50*time.Millisecond, "dropped server must stop serving")

	// b is unaffected throughout.
	assert.Equal(t, wire.FrameEnd, a.call(t, "b", "echo").Kind)
}

func TestGatewayFetchModeBootRetry(t *testing.T) {
	nc, url := fetchNATS(t)
	a := assemble(t, nc, url)

	src := &configsource.NATS{
		Conn:           nc,
		RequestSubject: "mcp.cfg.request",
		RequestTimeout: 200 * time.Millisecond,
		BootTimeout:    10 * time.Second,
		Logger:         testLogger(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = configsource.Run(ctx, testLogger(), src, func(cfg *config.Config) error {
			_, err := a.rec.Apply(cfg)
			return err
		})
	}()

	// Responder is down; the gateway should be retrying, serving nothing.
	f := a.call(t, "a", "echo")
	require.NotNil(t, f.Err)

	// Bring the responder up; the gateway converges without a restart.
	time.Sleep(400 * time.Millisecond)
	mc := &mutableConfig{nc: nc}
	mc.set(fakeServerJSON("a", fakeEnv(nil)))
	mc.serve(t)

	waitServing(t, a, "a")
}

func waitServing(t *testing.T, a *assembled, serverName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return a.call(t, serverName, "echo").Kind == wire.FrameEnd
	}, 8*time.Second, 100*time.Millisecond, "server %q never began serving", serverName)
}
