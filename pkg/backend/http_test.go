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

package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// modernHTTPServer is a minimal 2026-07-28 Streamable HTTP MCP server: it
// rejects requests missing the required headers, answers tools/list with
// JSON, and streams tools/call "slow" as SSE (progress then response).
func modernHTTPServer(t *testing.T, sawAuth *atomic.Bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer sekrit" {
			sawAuth.Store(true)
		}
		msg := &jsonrpc.Message{}
		if err := json.NewDecoder(r.Body).Decode(msg); err != nil {
			http.Error(w, "bad body", 400)
			return
		}
		if msg.Kind() != jsonrpc.KindRequest {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		// 2026-07-28: Mcp-Method is REQUIRED and must match.
		if r.Header.Get(mcpspec.HeaderMethod) != msg.Method {
			w.WriteHeader(400)
			resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(msg.ID, mcpspec.ErrHeaderMismatch, "Mcp-Method mismatch", nil))
			w.Write(resp)
			return
		}

		switch msg.Method {
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(`{"resultType":"complete","tools":[{"name":"slow"}]}`)))
			w.Write(resp)
		case mcpspec.MethodToolsCall:
			// SSE: two progress events then the response.
			var p struct {
				Meta struct {
					ProgressToken json.RawMessage `json:"progressToken"`
				} `json:"_meta"`
			}
			_ = json.Unmarshal(msg.Params, &p)
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			for i := 1; i <= 2; i++ {
				params, _ := json.Marshal(map[string]any{
					"progressToken": p.Meta.ProgressToken, "progress": i, "total": 2,
				})
				n, _ := jsonrpc.Encode(jsonrpc.NewNotification(mcpspec.NotifProgress, params))
				fmt.Fprintf(w, "data: %s\n\n", n)
				fl.Flush()
			}
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"done"}]}`)))
			fmt.Fprintf(w, "data: %s\n\n", resp)
			fl.Flush()
		default:
			w.Header().Set("Content-Type", "application/json")
			resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(msg.ID, jsonrpc.CodeMethodNotFound, "nope", nil))
			w.Write(resp)
		}
	}))
}

func TestHTTPModernJSONAndSSE(t *testing.T) {
	var sawAuth atomic.Bool
	srv := modernHTTPServer(t, &sawAuth)
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL, Headers: map[string]string{"Authorization": "Bearer sekrit"}}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	// JSON response path.
	resp, err := m.Call(context.Background(), jsonrpc.NewRequest("1", "tools/list", json.RawMessage(`{}`)), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	assert.Contains(t, string(resp.Result), "slow")
	assert.True(t, sawAuth.Load(), "gateway must inject the Authorization header")

	// SSE streaming path with progress attribution through the mux.
	var progress int32
	params, _ := json.Marshal(map[string]any{
		"name": "slow", "arguments": map[string]any{},
		"_meta": map[string]any{"progressToken": "http-tok"},
	})
	resp, err = m.Call(context.Background(), jsonrpc.NewRequest("2", mcpspec.MethodToolsCall, params),
		func(n *jsonrpc.Message) {
			assert.Contains(t, string(n.Params), "http-tok")
			atomic.AddInt32(&progress, 1)
		})
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	assert.Contains(t, string(resp.Result), "done")
	assert.EqualValues(t, 2, atomic.LoadInt32(&progress))
}

// legacyHTTPServer requires initialize-with-session before anything else.
func legacyHTTPServer(t *testing.T) (*httptest.Server, *atomic.Bool) {
	t.Helper()
	var deleted atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted.Store(true)
			w.WriteHeader(200)
			return
		}
		msg := &jsonrpc.Message{}
		if err := json.NewDecoder(r.Body).Decode(msg); err != nil {
			http.Error(w, "bad body", 400)
			return
		}
		if msg.Kind() == jsonrpc.KindNotification {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if msg.Method == mcpspec.MethodInitialize {
			w.Header().Set("Mcp-Session-Id", "sess-42")
			w.Header().Set("Content-Type", "application/json")
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(
				`{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"legacy-http","version":"1"}}`,
			)))
			w.Write(resp)
			return
		}
		if r.Header.Get("Mcp-Session-Id") != "sess-42" {
			w.WriteHeader(400)
			resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(msg.ID, jsonrpc.CodeInvalidRequest, "missing session", nil))
			w.Write(resp)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(`{"tools":[{"name":"t"}]}`)))
		w.Write(resp)
	}))
	return srv, &deleted
}

func TestHTTPLegacySessionLifecycle(t *testing.T) {
	srv, deleted := legacyHTTPServer(t)
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL, Legacy: true}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)

	// Drive the legacy handshake by hand (the legacy.Backend bridge does
	// this in production; here we prove the session mechanics).
	initParams, _ := json.Marshal(map[string]any{
		"protocolVersion": mcpspec.LegacyProtocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test", "version": "1"},
	})
	require.NoError(t, conn.Write(context.Background(), jsonrpc.NewRequest("i", mcpspec.MethodInitialize, initParams)))
	msg, err := conn.Read(context.Background())
	require.NoError(t, err)
	require.Nil(t, msg.Error)

	// Session id must now be echoed: tools/list succeeds.
	require.NoError(t, conn.Write(context.Background(), jsonrpc.NewRequest("2", "tools/list", json.RawMessage(`{}`))))
	msg, err = conn.Read(context.Background())
	require.NoError(t, err)
	require.Nil(t, msg.Error, "session header must have been echoed: %v", msg.Error)

	// Close must DELETE the session.
	require.NoError(t, conn.Close())
	assert.True(t, deleted.Load(), "Close must DELETE the legacy session")
}
