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

package shim

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend/legacy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/proxy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// newLegacyHarness is the M4 acceptance rig in miniature: a legacy client
// (us, speaking 2025-11-25 through the shim's stdio) talking to a legacy
// fake server behind the gateway — both compat wings live, modern-only wire
// in between.
func newLegacyHarness(t *testing.T) *harness {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, MaxPayload: 8 * 1024 * 1024}
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	pool := backend.NewPool(backend.PoolConfig{}, func(key backend.Key) (backend.Backend, error) {
		return &legacy.Backend{
			Inner: &backend.StdioBackend{
				Command: os.Args[0],
				Env: map[string]string{
					fakemcp.EnvFlag:    "1",
					"FAKEMCP_PROTOCOL": mcpspec.LegacyProtocolVersion,
				},
			},
		}, nil
	}, nil)
	t.Cleanup(pool.Shutdown)
	ws, err := wire.Serve(nc, wire.ServerConfig{
		Servers:   []string{"fake"},
		KeepAlive: 50 * time.Millisecond,
	}, proxy.New(pool, nil).Handler())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})

	wc, err := wire.NewClient(nc, wire.ClientConfig{Tenant: "acme", Inactivity: 5 * time.Second})
	require.NoError(t, err)

	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	s := New(wc, Config{Server: "fake"})

	h := &harness{stdin: stdinW, lines: make(chan string, 64), runErr: make(chan error, 1)}
	go func() { h.runErr <- s.Run(context.Background(), stdinR, stdoutW) }()
	go func() {
		sc := bufio.NewScanner(stdoutR)
		sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
		for sc.Scan() {
			h.lines <- sc.Text()
		}
		close(h.lines)
	}()
	t.Cleanup(func() { _ = stdinW.Close() })
	return h
}

func TestLegacyClientEndToEnd(t *testing.T) {
	h := newLegacyHarness(t)

	// 1. initialize — answered from server/discover, translated back.
	h.send(t, `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{"roots":{}},"clientInfo":{"name":"claude-code","version":"3.0"}}}`)
	line := h.next(t, 15*time.Second)
	m, err := jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	require.Nil(t, m.Error, "initialize failed: %s", line)
	assert.JSONEq(t, `0`, string(m.ID))
	var init struct {
		ProtocolVersion string          `json:"protocolVersion"`
		ServerInfo      json.RawMessage `json:"serverInfo"`
		Capabilities    json.RawMessage `json:"capabilities"`
	}
	require.NoError(t, json.Unmarshal(m.Result, &init))
	assert.Equal(t, mcpspec.LegacyProtocolVersion, init.ProtocolVersion,
		"the shim's face toward the client is legacy")
	assert.Contains(t, string(init.ServerInfo), "fakemcp")

	// 2. initialized — swallowed: no output, no error.
	h.send(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)

	// 3. ping — answered locally (would be method-not-found if forwarded).
	h.send(t, `{"jsonrpc":"2.0","id":1,"method":"ping"}`)
	line = h.next(t, 5*time.Second)
	m, err = jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	assert.JSONEq(t, `1`, string(m.ID))
	require.Nil(t, m.Error, "ping must be answered locally: %s", line)

	// 4. logging/setLevel — same treatment.
	h.send(t, `{"jsonrpc":"2.0","id":2,"method":"logging/setLevel","params":{"level":"debug"}}`)
	line = h.next(t, 5*time.Second)
	m, err = jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	require.Nil(t, m.Error)

	// 5. tools/list — crosses the modern wire with injected _meta, reaches
	// the legacy backend through the gateway's bridge.
	h.send(t, `{"jsonrpc":"2.0","id":3,"method":"tools/list"}`)
	line = h.next(t, 15*time.Second)
	m, err = jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	require.Nil(t, m.Error, "tools/list failed: %s", line)
	assert.Contains(t, string(m.Result), `"echo"`)

	// 6. tools/call with progress — the full streaming path, both wings.
	h.send(t, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"slow","_meta":{"progressToken":"lg-tok"}}}`)
	progress := 0
	for {
		line = h.next(t, 15*time.Second)
		m, err = jsonrpc.Decode([]byte(line))
		require.NoError(t, err)
		if m.Method == mcpspec.NotifProgress {
			assert.Contains(t, string(m.Params), "lg-tok")
			progress++
			continue
		}
		require.Nil(t, m.Error)
		assert.JSONEq(t, `4`, string(m.ID))
		assert.Equal(t, 3, progress, "progress must precede the response")
		break
	}
}

func TestLegacyInitializeWithNoGateway(t *testing.T) {
	h := newHarness(t, false) // modern harness, but no gateway at all
	h.send(t, `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","clientInfo":{"name":"x","version":"1"},"capabilities":{}}}`)
	line := h.next(t, 15*time.Second)
	m, err := jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	require.NotNil(t, m.Error, "initialize must fail legibly, not hang")
	assert.Equal(t, wire.ErrCodeNoGateway, m.Error.Code)
	assert.JSONEq(t, `0`, string(m.ID))
}
