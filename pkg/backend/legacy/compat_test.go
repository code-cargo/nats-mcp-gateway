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

package legacy

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakemcp.EnvFlag) == "1" {
		fakemcp.Main()
		return
	}
	os.Exit(m.Run())
}

// legacyMux builds a mux over a legacy fake server via the bridge.
func legacyMux(t *testing.T) *backend.Mux {
	t.Helper()
	b := &Backend{
		Inner: &backend.StdioBackend{
			Command: os.Args[0],
			Env: map[string]string{
				fakemcp.EnvFlag:    "1",
				"FAKEMCP_PROTOCOL": mcpspec.LegacyProtocolVersion,
			},
		},
	}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := backend.NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func toolCall(id, tool string) *jsonrpc.Message {
	params, _ := json.Marshal(map[string]any{"name": tool, "arguments": map[string]any{}})
	return jsonrpc.NewRequest(id, "tools/call", params)
}

func TestHandshakeThenToolCall(t *testing.T) {
	m := legacyMux(t)
	// The fake legacy server rejects any request before initialize, so a
	// working tools/call proves the bridge handshook on connect.
	resp, err := m.Call(context.Background(), toolCall("1", "echo"), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error, "legacy server must have been initialized: %v", resp.Error)
}

func TestDiscoverSynthesizedFromInitialize(t *testing.T) {
	m := legacyMux(t)
	// The legacy fake server answers server/discover with method-not-found,
	// so a real DiscoverResult proves it was synthesized, never forwarded.
	req := jsonrpc.NewRequest("2", mcpspec.MethodDiscover, json.RawMessage(`{}`))
	resp, err := m.Call(context.Background(), req, nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error, "discover must be synthesized, not forwarded")

	var d struct {
		SupportedVersions []string        `json:"supportedVersions"`
		ServerInfo        json.RawMessage `json:"serverInfo"`
		Capabilities      json.RawMessage `json:"capabilities"`
		TTLMs             int             `json:"ttlMs"`
		Instructions      string          `json:"instructions"`
	}
	require.NoError(t, json.Unmarshal(resp.Result, &d))
	assert.Equal(t, []string{mcpspec.ProtocolVersion}, d.SupportedVersions,
		"the gateway IS the modern server from the client's view")
	assert.Contains(t, string(d.ServerInfo), "fakemcp")
	assert.Contains(t, string(d.Capabilities), "tools")
	assert.Equal(t, 300000, d.TTLMs)
	assert.Equal(t, "fake server for tests", d.Instructions)
}

func TestModernMetaStrippedBeforeForwarding(t *testing.T) {
	m := legacyMux(t)
	// echo returns its arguments; the _meta strip happens on params. The
	// fake server would not fail on unknown _meta, so instead prove the call
	// still works when the modern keys are present (they must be removed
	// before a strict legacy server sees them).
	params, _ := json.Marshal(map[string]any{
		"name":      "echo",
		"arguments": map[string]any{},
		"_meta": map[string]any{
			mcpspec.MetaProtocolVersion:    mcpspec.ProtocolVersion,
			mcpspec.MetaClientInfo:         map[string]string{"name": "x"},
			mcpspec.MetaClientCapabilities: map[string]any{},
			"progressToken":                "keep-me",
		},
	})
	resp, err := m.Call(context.Background(), jsonrpc.NewRequest("3", "tools/call", params), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)
}

func TestListenSynthesisFromListChanged(t *testing.T) {
	m := legacyMux(t)

	notifications := make(chan *jsonrpc.Message, 8)
	listenParams, _ := json.Marshal(map[string]any{"toolsListChanged": true})
	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()

	listenDone := make(chan error, 1)
	go func() {
		_, err := m.Call(listenCtx, jsonrpc.NewRequest("sub-1", mcpspec.MethodListen, listenParams),
			func(n *jsonrpc.Message) { notifications <- n })
		listenDone <- err
	}()

	// Give the listen registration a moment, then trigger a list_changed.
	time.Sleep(200 * time.Millisecond)
	resp, err := m.Call(context.Background(), toolCall("4", "notify_changed"), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	select {
	case n := <-notifications:
		assert.Equal(t, "notifications/tools/list_changed", n.Method)
		// The subscriptionId must reference the CALLER's listen id.
		var p struct {
			Meta map[string]json.RawMessage `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(n.Params, &p))
		assert.JSONEq(t, `"sub-1"`, string(p.Meta[mcpspec.MetaSubscriptionID]))
	case <-time.After(10 * time.Second):
		t.Fatal("list_changed never reached the listen stream")
	}

	// Cancelling the listen must end its Call without killing the conn.
	cancelListen()
	select {
	case err := <-listenDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("listen call did not return after cancel")
	}
	resp, err = m.Call(context.Background(), toolCall("5", "echo"), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error, "connection must survive listen teardown")
}

func TestListenFilterRespected(t *testing.T) {
	m := legacyMux(t)
	notifications := make(chan *jsonrpc.Message, 8)
	// Subscribe to PROMPTS changes only; a tools/list_changed must not land.
	listenParams, _ := json.Marshal(map[string]any{"promptsListChanged": true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = m.Call(ctx, jsonrpc.NewRequest("sub-2", mcpspec.MethodListen, listenParams),
			func(n *jsonrpc.Message) { notifications <- n })
	}()
	time.Sleep(200 * time.Millisecond)

	_, err := m.Call(context.Background(), toolCall("6", "notify_changed"), nil)
	require.NoError(t, err)

	select {
	case n := <-notifications:
		t.Fatalf("filtered-out notification leaked: %s", n.Method)
	case <-time.After(1 * time.Second):
	}
}
