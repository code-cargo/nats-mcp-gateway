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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// xmcpHeaderServer is a 2026-07-28 server whose execute_sql tool annotates
// its `region` parameter with x-mcp-header, and which enforces the resulting
// Mcp-Param-Region header exactly as the spec requires: reject with -32020
// when the body carries the value but the header is missing or disagrees.
func xmcpHeaderServer(t *testing.T, listCalls, callAttempts *atomic.Int32) *httptest.Server {
	t.Helper()
	const toolsList = `{"resultType":"complete","ttlMs":300000,"cacheScope":"private","tools":[
		{"name":"execute_sql","inputSchema":{"type":"object","properties":{
			"region":{"type":"string","x-mcp-header":"Region"},
			"query":{"type":"string"}}}},
		{"name":"broken","inputSchema":{"type":"object","properties":{
			"a":{"type":"number","x-mcp-header":"A"}}}}
	]}`
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := &jsonrpc.Message{}
		if err := json.NewDecoder(r.Body).Decode(msg); err != nil {
			http.Error(w, "bad body", 400)
			return
		}
		reject := func(detail string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(
				msg.ID, mcpspec.ErrHeaderMismatch, detail, nil,
			))
			_, _ = w.Write(resp)
		}

		switch msg.Method {
		case mcpspec.MethodToolsList:
			listCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(toolsList)))
			_, _ = w.Write(resp)
		case mcpspec.MethodToolsCall:
			callAttempts.Add(1)
			var p struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			}
			_ = json.Unmarshal(msg.Params, &p)
			region, inBody := p.Arguments["region"].(string)
			got := r.Header.Get("Mcp-Param-Region")
			if inBody && got != region {
				reject("Mcp-Param-Region " + got + " does not match body " + region)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID,
				json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"ran"}]}`)))
			_, _ = w.Write(resp)
		default:
			w.Header().Set("Content-Type", "application/json")
			resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(msg.ID, jsonrpc.CodeMethodNotFound, "nope", nil))
			_, _ = w.Write(resp)
		}
	}))
}

func TestXMcpHeaderRecoveredReactively(t *testing.T) {
	var listCalls, callAttempts atomic.Int32
	srv := xmcpHeaderServer(t, &listCalls, &callAttempts)
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	// Straight to tools/call, having never seen the schema. The gateway is
	// schema-blind, so the first attempt omits Mcp-Param-Region and is
	// rejected; it must then learn the annotation and retry itself.
	params, _ := json.Marshal(map[string]any{
		"name":      "execute_sql",
		"arguments": map[string]any{"region": "us-west1", "query": "SELECT 1"},
	})
	resp, err := m.Call(context.Background(), jsonrpc.NewRequest("1", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error, "the retry must succeed, not surface the -32020")
	assert.Contains(t, string(resp.Result), "ran")

	assert.Equal(t, int32(2), callAttempts.Load(), "exactly one retry")
	assert.Equal(t, int32(1), listCalls.Load(), "the schema is fetched only when needed")

	// Now that the annotation is cached, a second call must carry the header
	// on its FIRST attempt — the recovery is a one-time cost, not per-call.
	callAttempts.Store(0)
	listCalls.Store(0)
	resp, err = m.Call(context.Background(), jsonrpc.NewRequest("2", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	assert.Equal(t, int32(1), callAttempts.Load(), "no second attempt needed")
	assert.Equal(t, int32(0), listCalls.Load(), "no second schema fetch")
}

func TestXMcpHeaderAppliedAfterToolsList(t *testing.T) {
	var listCalls, callAttempts atomic.Int32
	srv := xmcpHeaderServer(t, &listCalls, &callAttempts)
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	// The ordinary client flow: list, then call. The annotation is learned
	// from the listing passing through, so the call never round-trips twice.
	resp, err := m.Call(context.Background(),
		jsonrpc.NewRequest("1", mcpspec.MethodToolsList, json.RawMessage(`{}`)), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	// A tool whose annotation is invalid (x-mcp-header on a `number`) must be
	// excluded from the listing rather than poisoning the whole toolset.
	assert.Contains(t, string(resp.Result), "execute_sql")
	assert.NotContains(t, string(resp.Result), "broken",
		"a tool with an invalid annotation must be dropped from tools/list")

	params, _ := json.Marshal(map[string]any{
		"name":      "execute_sql",
		"arguments": map[string]any{"region": "us-west1", "query": "SELECT 1"},
	})
	resp, err = m.Call(context.Background(), jsonrpc.NewRequest("2", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	assert.Equal(t, int32(1), callAttempts.Load(), "the header was right the first time")
}

func TestHeaderMismatchSurfacesWhenItIsNotAboutParams(t *testing.T) {
	// A -32020 the gateway cannot fix must reach the caller as itself, not as
	// an opaque "http 400" wrapped in an internal error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := &jsonrpc.Message{}
		_ = json.NewDecoder(r.Body).Decode(msg)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(
			msg.ID, mcpspec.ErrHeaderMismatch, "MCP-Protocol-Version missing", nil,
		))
		_, _ = w.Write(resp)
	}))
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	resp, err := m.Call(context.Background(),
		jsonrpc.NewRequest("1", mcpspec.MethodResourcesRead, json.RawMessage(`{"uri":"file:///x"}`)), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Error)
	assert.Equal(t, mcpspec.ErrHeaderMismatch, resp.Error.Code)
	assert.Contains(t, resp.Error.Message, "MCP-Protocol-Version")
}

func TestNoWastedRetryWhenAnnotationsAreUnchanged(t *testing.T) {
	// A -32020 the gateway cannot fix by mirroring headers must surface after
	// ONE attempt. Re-reading the schema, finding it identical, and reissuing
	// a byte-identical request just triples the round trips per call.
	var listCalls, callAttempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := &jsonrpc.Message{}
		_ = json.NewDecoder(r.Body).Decode(msg)
		switch msg.Method {
		case mcpspec.MethodToolsList:
			listCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(
				`{"resultType":"complete","tools":[{"name":"plain","inputSchema":{"type":"object"}}]}`,
			)))
			_, _ = w.Write(resp)
		default:
			callAttempts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(
				msg.ID, mcpspec.ErrHeaderMismatch, "MCP-Protocol-Version missing", nil,
			))
			_, _ = w.Write(resp)
		}
	}))
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	params, _ := json.Marshal(map[string]any{"name": "plain", "arguments": map[string]any{}})
	resp, err := m.Call(context.Background(), jsonrpc.NewRequest("1", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Error)
	assert.Equal(t, mcpspec.ErrHeaderMismatch, resp.Error.Code)
	assert.Equal(t, int32(1), callAttempts.Load(), "no retry: the annotations did not change")

	// A backend rejecting for a reason mirroring cannot fix must not cost a
	// probe on EVERY call. The first refresh established that this tool has
	// no annotations; that answer is remembered, so later rejections probe
	// nothing at all.
	listCalls.Store(0)
	for i := range 3 {
		_, err = m.Call(context.Background(),
			jsonrpc.NewRequest(strconv.Itoa(i+2), mcpspec.MethodToolsCall, params), nil)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(0), listCalls.Load(),
		"a known-unannotated tool must never be re-probed")
}

func TestLegacyHTTPBackendIsNotJudgedAgainstXMcpHeader(t *testing.T) {
	// x-mcp-header does not exist in 2025-11-25. Excluding a legacy server's
	// tools for violating a rule its author never agreed to would silently
	// shrink its toolset.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := &jsonrpc.Message{}
		_ = json.NewDecoder(r.Body).Decode(msg)
		w.Header().Set("Content-Type", "application/json")
		resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(
			`{"tools":[{"name":"legacy_tool","inputSchema":{"type":"object","properties":{
				"n":{"type":"number","x-mcp-header":"N"}}}}]}`,
		)))
		_, _ = w.Write(resp)
	}))
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL, Legacy: true}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	resp, err := m.Call(context.Background(),
		jsonrpc.NewRequest("1", mcpspec.MethodToolsList, json.RawMessage(`{}`)), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	assert.Contains(t, string(resp.Result), "legacy_tool",
		"a legacy server's tools must survive a rule that postdates its protocol")
}

func TestRejectedNotificationDoesNotBecomeAPhantomResponse(t *testing.T) {
	// A 4xx JSON-RPC error answering a NOTIFICATION has no id to reply on.
	// Delivering one would create a response the mux cannot route and drops
	// silently, so the rejection must stay on the logged-failure path.
	served := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(served)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(
			nil, mcpspec.ErrHeaderMismatch, "no headers on notifications", nil,
		))
		_, _ = w.Write(resp)
	}))
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	require.NoError(t, conn.Write(context.Background(),
		jsonrpc.NewNotification(mcpspec.NotifCancelled, json.RawMessage(`{"requestId":"x"}`))))

	// Wait for the exchange to have actually happened. Without this the read
	// deadline below can expire before the branch under test ever runs, and
	// the test would pass just as happily with the guard removed.
	select {
	case <-served:
	case <-time.After(10 * time.Second):
		t.Fatal("the backend was never called")
	}

	// Nothing may reach the inbox: an id-less response there is unroutable.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err = conn.Read(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded,
		"a rejected notification must not surface as a message")
}

func TestFailedRefreshDoesNotSuppressALaterOne(t *testing.T) {
	// A caller queued behind a FAILED probe must still run its own. Crediting
	// it with work that did not happen makes it skip the retry and fail a
	// call whose fix was available all along.
	var listCalls atomic.Int32
	var listFails atomic.Bool
	listFails.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := &jsonrpc.Message{}
		_ = json.NewDecoder(r.Body).Decode(msg)
		if msg.Method == mcpspec.MethodToolsList {
			listCalls.Add(1)
			if listFails.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(
				`{"resultType":"complete","tools":[{"name":"t","inputSchema":{"type":"object",
					"properties":{"region":{"type":"string","x-mcp-header":"Region"}}}}]}`,
			)))
			_, _ = w.Write(resp)
			return
		}
		var p struct {
			Arguments map[string]any `json:"arguments"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		region, _ := p.Arguments["region"].(string)
		if r.Header.Get("Mcp-Param-Region") != region {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(
				msg.ID, mcpspec.ErrHeaderMismatch, "missing Mcp-Param-Region", nil,
			))
			_, _ = w.Write(resp)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID,
			json.RawMessage(`{"resultType":"complete","content":[]}`)))
		_, _ = w.Write(resp)
	}))
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	params, _ := json.Marshal(map[string]any{
		"name": "t", "arguments": map[string]any{"region": "us-west1"},
	})

	// First call: the probe fails, so the -32020 legitimately reaches us.
	resp, err := m.Call(context.Background(), jsonrpc.NewRequest("1", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Error)
	assert.Equal(t, int32(1), listCalls.Load())

	// The backend recovers. The next call must probe again and succeed —
	// which it cannot do if the failed refresh was recorded as progress.
	listFails.Store(false)
	resp, err = m.Call(context.Background(), jsonrpc.NewRequest("2", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error, "the recovery must run after an earlier probe failed")
	assert.Equal(t, int32(2), listCalls.Load())
}

func TestScanSSEJoinsDataLinesWithNewline(t *testing.T) {
	// The SSE spec joins an event's data lines with a newline. Welding them
	// together loses a separator that may be part of the payload.
	var got [][]byte
	err := scanSSE(strings.NewReader("data: line1\ndata: line2\n\ndata: solo\n\n"),
		func(b []byte) bool { got = append(got, b); return true })
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "line1\nline2", string(got[0]))
	assert.Equal(t, "solo", string(got[1]))
}

func TestAnnotationProbeSurvivesAnExpiredToken(t *testing.T) {
	// The probe used to POST directly, skipping the 401/Invalidate retry every
	// other request gets — so an expired token defeated the whole recovery.
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := &jsonrpc.Message{}
		_ = json.NewDecoder(r.Body).Decode(msg)
		if r.Header.Get("Authorization") != "Bearer fresh" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if msg.Method == mcpspec.MethodToolsList {
			calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(
				`{"resultType":"complete","tools":[{"name":"t","inputSchema":{"type":"object",
					"properties":{"region":{"type":"string","x-mcp-header":"Region"}}}}]}`,
			)))
			_, _ = w.Write(resp)
			return
		}
		var p struct {
			Arguments map[string]any `json:"arguments"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		region, _ := p.Arguments["region"].(string)
		if r.Header.Get("Mcp-Param-Region") != region {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(
				msg.ID, mcpspec.ErrHeaderMismatch, "missing Mcp-Param-Region", nil,
			))
			_, _ = w.Write(resp)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID,
			json.RawMessage(`{"resultType":"complete","content":[]}`)))
		_, _ = w.Write(resp)
	}))
	t.Cleanup(srv.Close)

	ts := &staleTokenSource{}
	b := &HTTPBackend{URL: srv.URL, TokenSource: ts}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	params, _ := json.Marshal(map[string]any{
		"name": "t", "arguments": map[string]any{"region": "us-west1"},
	})
	resp, err := m.Call(context.Background(), jsonrpc.NewRequest("1", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error, "the probe must refresh the token like any other request")
	assert.Positive(t, calls.Load(), "the schema was actually fetched")
}

// staleTokenSource hands out an expired token until Invalidate is called.
type staleTokenSource struct {
	mu    sync.Mutex
	fresh bool
}

func (s *staleTokenSource) Headers(context.Context) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fresh {
		return map[string]string{"Authorization": "Bearer fresh"}, nil
	}
	return map[string]string{"Authorization": "Bearer stale"}, nil
}

func (s *staleTokenSource) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fresh = true
}

// TestUnreadableParamsSendNoName pins the fallback in roundTripOnce.
//
// A body whose params cannot be read unambiguously is unreachable from the
// wire — pkg/proxy.Check decoded the same bytes before authorizing them — but
// this Conn is also driven directly by the legacy bridge and by tests, so the
// behavior has to be defined rather than incidental. The gateway must not
// invent a name: it sends the request with no Mcp-Name and lets the server
// answer, which a conformant one does with its own -32020. Guessing either
// reading would put a name on the wire that nothing verified.
func TestUnreadableParamsSendNoName(t *testing.T) {
	var sawName string
	var sawNameHeader bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawName, sawNameHeader = r.Header.Get(mcpspec.HeaderName), r.Header.Values(mcpspec.HeaderName) != nil
		msg := &jsonrpc.Message{}
		_ = json.NewDecoder(r.Body).Decode(msg)
		w.Header().Set("Content-Type", "application/json")
		resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(
			msg.ID, mcpspec.ErrHeaderMismatch, "Mcp-Name missing", nil))
		_, _ = w.Write(resp)
	}))
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	// Two readings, "a" and "b", and the gateway is entitled to neither.
	resp, err := m.Call(context.Background(), jsonrpc.NewRequest(
		"1", mcpspec.MethodToolsCall, json.RawMessage(`{"name":"a","name":"b"}`)), nil)
	require.NoError(t, err)

	assert.False(t, sawNameHeader, "an unreadable name must not be guessed at, got %q", sawName)
	require.NotNil(t, resp.Error)
	assert.Equal(t, mcpspec.ErrHeaderMismatch, resp.Error.Code,
		"the server's own rejection must reach the caller unchanged")
}

// TestNonStringNameSendsNoName is the same rule for a name that is present
// but is not a string: "" is what an ABSENT name decodes to, so reading one
// as the other would send a request claiming to name nothing.
func TestNonStringNameSendsNoName(t *testing.T) {
	var sawNameHeader bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawNameHeader = r.Header.Values(mcpspec.HeaderName) != nil
		msg := &jsonrpc.Message{}
		_ = json.NewDecoder(r.Body).Decode(msg)
		w.Header().Set("Content-Type", "application/json")
		resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(`{}`)))
		_, _ = w.Write(resp)
	}))
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	_, err = m.Call(context.Background(), jsonrpc.NewRequest(
		"1", mcpspec.MethodToolsCall, json.RawMessage(`{"name":{"toString":"echo"}}`)), nil)
	require.NoError(t, err)
	assert.False(t, sawNameHeader, "a non-string name must not become an empty one")
}
