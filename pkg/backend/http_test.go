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
	"io"
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

func TestOnePageOfAListingCannotSettleTheWholeToolset(t *testing.T) {
	// tools/list is paginated, and one conn is shared by every caller of a
	// (server, tenant, credential set) — so one client fetching page 1 and
	// another calling a tool that lives on page 2 is ordinary traffic, not a
	// corner case. Page 1 carrying no x-mcp-header says nothing about page 2,
	// and concluding otherwise disables the recovery permanently: the call is
	// rejected for a missing Mcp-Param-*, the gateway decides it already knows
	// this server has no annotations, and the -32020 is unrecoverable for as
	// long as the conn lives.
	var listCalls, callAttempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := &jsonrpc.Message{}
		_ = json.NewDecoder(r.Body).Decode(msg)
		w.Header().Set("Content-Type", "application/json")
		switch msg.Method {
		case mcpspec.MethodToolsList:
			listCalls.Add(1)
			var p struct {
				Cursor string `json:"cursor"`
			}
			_ = json.Unmarshal(msg.Params, &p)
			page := `{"resultType":"complete","nextCursor":"p2","tools":[
				{"name":"ping","inputSchema":{"type":"object"}}]}`
			if p.Cursor == "p2" {
				page = `{"resultType":"complete","tools":[
					{"name":"execute_sql","inputSchema":{"type":"object","properties":{
						"region":{"type":"string","x-mcp-header":"Region"}}}}]}`
			}
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(page)))
			_, _ = w.Write(resp)
		default:
			callAttempts.Add(1)
			var p struct {
				Arguments map[string]any `json:"arguments"`
			}
			_ = json.Unmarshal(msg.Params, &p)
			region, _ := p.Arguments["region"].(string)
			if r.Header.Get("Mcp-Param-Region") != region {
				w.WriteHeader(http.StatusBadRequest)
				resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(
					msg.ID, mcpspec.ErrHeaderMismatch, "missing Mcp-Param-Region", nil,
				))
				_, _ = w.Write(resp)
				return
			}
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID,
				json.RawMessage(`{"resultType":"complete","content":[]}`)))
			_, _ = w.Write(resp)
		}
	}))
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	// An ordinary first page passes through, carrying no annotations.
	resp, err := m.Call(context.Background(),
		jsonrpc.NewRequest("1", mcpspec.MethodToolsList, json.RawMessage(`{}`)), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	require.Contains(t, string(resp.Result), "ping")

	params, _ := json.Marshal(map[string]any{
		"name": "execute_sql", "arguments": map[string]any{"region": "us-west1"},
	})
	resp, err = m.Call(context.Background(), jsonrpc.NewRequest("2", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error, "the recovery must still run for a tool whose page was never seen")
	assert.Equal(t, int32(2), callAttempts.Load(), "one rejection, one retry carrying the header")
	assert.Equal(t, int32(3), listCalls.Load(),
		"the client's page plus both pages of the probe that had to read past it")
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

func TestCloseEndsInFlightExchanges(t *testing.T) {
	// A backend that accepts the POST and answers nothing is the shape that
	// costs the gateway a goroutine and a socket per call: the client carries
	// no Timeout (SSE streams are long-lived by design) and the exchange runs
	// detached from the caller, so nothing on this side ever ends it. Close,
	// EvictServer and Shutdown all returned while it ran on, which is what
	// makes the leak unbounded rather than merely slow.
	var arrived atomic.Int32
	var abandoned atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// net/http only starts watching for a client disconnect once the
		// request body has been consumed, so a handler that ignores it never
		// learns its caller is gone.
		_, _ = io.Copy(io.Discard, r.Body)
		arrived.Add(1)
		select {
		case <-r.Context().Done():
			abandoned.Add(1)
		case <-release:
		}
	}))
	// LIFO: the wedged handlers are released before the server is closed, so a
	// regression fails this test instead of hanging its cleanup.
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	c := conn.(*httpConn)

	// One of each: a request, whose exchange is tied to the caller waiting for
	// it, and a notification, which has no caller to be tied to.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, conn.Write(ctx, jsonrpc.NewRequest("1", mcpspec.MethodToolsList, json.RawMessage(`{}`))))
	require.NoError(t, conn.Write(ctx, jsonrpc.NewNotification(mcpspec.NotifCancelled, json.RawMessage(`{"requestId":"x"}`))))
	require.Eventually(t, func() bool { return arrived.Load() == 2 }, 10*time.Second, 10*time.Millisecond,
		"the backend never received both exchanges")

	require.NoError(t, conn.Close())

	// Both exchanges end, rather than sitting in a round trip nothing bounds.
	// This says nothing about WHEN Close returned relative to them — cancelling
	// alone satisfies it, because a cancelled round trip returns on its own.
	// TestCloseWaitsForInflightExchange is what pins the drain.
	drained := make(chan struct{})
	go func() { c.inflight.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled exchanges never finished")
	}

	assert.Eventually(t, func() bool { return abandoned.Load() == 2 }, 10*time.Second, 10*time.Millisecond,
		"closing the conn must reach the backend as a disconnect, not leave the requests parked on it")
}

func TestCloseWaitsForInflightExchange(t *testing.T) {
	// Close is what the pool calls to reclaim a backend, so "closed" has to
	// mean the exchanges are off the wire and not merely cancelled: an exchange
	// still inside io.ReadAll holds its socket until that read returns.
	//
	// The response body below stalls, and ignores cancellation while it does.
	// That is the whole point: connCtx cancellation is not the same event as
	// the exchange finishing, and a body that ends the moment it is cancelled
	// cannot tell a Close that drains from one that only cancels and returns.
	body := newStalledBody(`{"jsonrpc":"2.0","id":"1","result":{"resultType":"complete","tools":[]}}`)
	t.Cleanup(body.release)

	// Never dialed: the transport answers every request itself.
	b := &HTTPBackend{
		URL:    "http://stalled.invalid",
		Client: &http.Client{Transport: &stalledTransport{body: body}},
	}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, conn.Write(ctx, jsonrpc.NewRequest("1", mcpspec.MethodToolsList, json.RawMessage(`{}`))))
	select {
	case <-body.reading:
	case <-time.After(10 * time.Second):
		t.Fatal("the exchange never reached the response body")
	}

	closed := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(closed)
		assert.NoError(t, conn.Close())
	}()

	// Well inside terminateGrace, so a Close that blocks correctly is still
	// blocked here and only a Close that skipped the drain has returned.
	select {
	case <-closed:
		t.Fatal("Close returned while an exchange was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	body.release()
	select {
	case <-closed:
		// Drained or timed out are the two ways to arrive here, and they are
		// told apart by the clock rather than by a bound on the wait: the
		// exchange finishes in microseconds once the body is released, while
		// the timeout path cannot return before terminateGrace is up. Waiting
		// generously and then asserting on elapsed keeps a loaded runner from
		// reading as the failure it is supposed to detect.
		assert.Less(t, time.Since(start), terminateGrace,
			"Close returned no sooner than the grace deadline, so it timed out rather than drained")
	case <-time.After(30 * time.Second):
		t.Fatal("Close did not return once its exchange finished")
	}
}

func TestAwaitInflightGivesUpAtGrace(t *testing.T) {
	// The other half of the contract: a backend that never lets go must cost
	// the pool a bounded wait, not a permanent one. terminateGrace is a package
	// constant, so Close's own bound cannot be shortened for a test without a
	// multi-second sleep — but the bound is a parameter here, which is the
	// level the behavior actually lives at.
	b := &HTTPBackend{URL: "http://unused.invalid"}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	c := conn.(*httpConn)

	c.inflight.Add(1)
	assert.False(t, c.awaitInflight(50*time.Millisecond),
		"a wedged exchange must be abandoned when the grace expires")

	c.inflight.Done()
	assert.True(t, c.awaitInflight(10*time.Second),
		"a drained conn must not be reported as still in flight")
}

// stalledTransport answers every request with a stalled body, so an exchange
// can be held past teardown without a server that has to be torn down too.
type stalledTransport struct{ body *stalledBody }

func (t *stalledTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       t.body,
		Request:    req,
	}, nil
}

// stalledBody blocks in Read until released, then yields its payload. Reads
// are what the exchange goroutine is doing when Close arrives, and this one
// does not end on cancellation — see TestCloseWaitsForInflightExchange.
type stalledBody struct {
	reading   chan struct{}
	released  chan struct{}
	readOnce  sync.Once
	closeOnce sync.Once
	rest      io.Reader
}

func newStalledBody(payload string) *stalledBody {
	return &stalledBody{
		reading:  make(chan struct{}),
		released: make(chan struct{}),
		rest:     strings.NewReader(payload),
	}
}

// release is idempotent so a failing test's cleanup cannot double-close it.
func (b *stalledBody) release() { b.closeOnce.Do(func() { close(b.released) }) }

func (b *stalledBody) Read(p []byte) (int, error) {
	b.readOnce.Do(func() { close(b.reading) })
	<-b.released
	return b.rest.Read(p)
}

func (b *stalledBody) Close() error { return nil }

func TestCallerCancellationEndsItsExchange(t *testing.T) {
	// The other end of the same leak: the conn stays open and healthy, so
	// nothing closes it, and the caller that provoked the exchange gives up.
	// Its POST has no deadline of its own, so without the caller's context it
	// runs until the pool eventually recycles the whole conn — while later
	// requests keep arriving and keep the conn from ever being idle enough to
	// recycle.
	var abandoned atomic.Bool
	arrived := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(arrived)
		select {
		case <-r.Context().Done():
			abandoned.Store(true)
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, conn.Write(ctx, jsonrpc.NewRequest("1", mcpspec.MethodToolsList, json.RawMessage(`{}`))))
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("the backend was never called")
	}

	cancel()
	assert.Eventually(t, abandoned.Load, 10*time.Second, 10*time.Millisecond,
		"a request nobody is waiting for must not stay on the wire")
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
			msg.ID, mcpspec.ErrHeaderMismatch, "Mcp-Name missing", nil,
		))
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
		"1", mcpspec.MethodToolsCall, json.RawMessage(`{"name":"a","name":"b"}`),
	), nil)
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
		"1", mcpspec.MethodToolsCall, json.RawMessage(`{"name":{"toString":"echo"}}`),
	), nil)
	require.NoError(t, err)
	assert.False(t, sawNameHeader, "a non-string name must not become an empty one")
}

// TestBackendErrorDoesNotEchoTheURLsSecrets covers what a transport error
// carries to the caller.
//
// net/http quotes the request URL into every transport error, the proxy
// forwards a backend's JSON-RPC error verbatim, and a backend URL is a
// documented place to expand a secret into. So "connection refused" — which
// any caller can provoke against an unreachable backend — was enough to read
// an api_key out of the query string. Go redacts userinfo passwords and
// nothing else.
func TestBackendErrorDoesNotEchoTheURLsSecrets(t *testing.T) {
	for _, raw := range []string{
		"http://127.0.0.1:1/mcp?api_key=SUPERSECRET",
		"http://127.0.0.1:1/mcp?a=1&token=SUPERSECRET#frag",
		"http://user:SUPERSECRET@127.0.0.1:1/mcp",
		"http://127.0.0.1:1/mcp?api_key=SUPERSECRET&x=2",
	} {
		b := &HTTPBackend{URL: raw}
		conn, err := b.Connect(context.Background())
		require.NoError(t, err)
		m := NewMux(conn, nil)
		resp, err := m.Call(context.Background(),
			jsonrpc.NewRequest("1", mcpspec.MethodToolsList, nil), nil)
		require.NoError(t, err)
		require.NotNil(t, resp.Error)
		assert.NotContains(t, resp.Error.Message, "SUPERSECRET", "url %q leaked to the caller", raw)
		assert.Contains(t, resp.Error.Message, "127.0.0.1:1",
			"the host must survive: which backend failed is the whole content of the error")
		_ = m.Close()
	}
}

func TestScrubURLsKeepsErrorsDiagnosable(t *testing.T) {
	cases := map[string]string{
		`Post "http://h/mcp?k=s": refused`: `Post "http://h/mcp?[redacted]": refused`,
		`Post "http://u:p@h/mcp": refused`: `Post "http://h/mcp": refused`,
		`Post "http://h/mcp": refused`:     `Post "http://h/mcp": refused`,
		`dial tcp 1.2.3.4:80: no route`:    `dial tcp 1.2.3.4:80: no route`,
		`Get "https://h/a/b#frag" failed`:  `Get "https://h/a/b" failed`,
	}
	for in, want := range cases {
		assert.Equal(t, want, scrubURLs(in), "input %q", in)
	}
}

// endlessCursorServer answers tools/list with a page that always names a next
// cursor while endless is set, and rejects every tools/call whose
// Mcp-Param-Region disagrees with its body — the two halves that together make
// each rejection provoke a full paginated sweep. Clearing endless switches it
// to a single terminating page carrying the annotation the calls need.
func endlessCursorServer(listPages, callAttempts *atomic.Int32, endless *atomic.Bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := &jsonrpc.Message{}
		_ = json.NewDecoder(r.Body).Decode(msg)
		w.Header().Set("Content-Type", "application/json")
		if msg.Method == mcpspec.MethodToolsList {
			listPages.Add(1)
			page := `{"tools":[],"nextCursor":"more"}`
			if !endless.Load() {
				page = `{"resultType":"complete","tools":[{"name":"t","inputSchema":
					{"type":"object","properties":{
						"region":{"type":"string","x-mcp-header":"Region"}}}}]}`
			}
			resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, json.RawMessage(page)))
			_, _ = w.Write(resp)
			return
		}
		callAttempts.Add(1)
		var p struct {
			Arguments map[string]any `json:"arguments"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		region, _ := p.Arguments["region"].(string)
		if r.Header.Get("Mcp-Param-Region") != region {
			w.WriteHeader(http.StatusBadRequest)
			resp, _ := jsonrpc.Encode(jsonrpc.NewErrorResponse(
				msg.ID, mcpspec.ErrHeaderMismatch, "missing Mcp-Param-Region", nil,
			))
			_, _ = w.Write(resp)
			return
		}
		resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID,
			json.RawMessage(`{"resultType":"complete","content":[]}`)))
		_, _ = w.Write(resp)
	}))
}

// regionCallParams is a tools/call the endless-cursor server always rejects
// until the gateway has learned to mirror `region` into a header.
func regionCallParams(t *testing.T) json.RawMessage {
	t.Helper()
	params, err := json.Marshal(map[string]any{
		"name": "t", "arguments": map[string]any{"region": "us-west1"},
	})
	require.NoError(t, err)
	return params
}

// TestTruncatedListingDampsReProbing bounds what a backend can make the
// gateway do by never ending its cursor.
//
// Every rejected call is entitled to ask whether the schema explains the
// rejection, and answering costs a full paginated sweep of the toolset. A
// backend whose cursor never terminates makes that sweep cost the page cap
// every time, so without damping the gateway spends annotationProbeMaxPages
// round trips per rejected call, forever, learning the same nothing.
func TestTruncatedListingDampsReProbing(t *testing.T) {
	var listPages, callAttempts atomic.Int32
	var endless atomic.Bool
	endless.Store(true)
	srv := endlessCursorServer(&listPages, &callAttempts, &endless)
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	c := conn.(*httpConn)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	const calls = 4
	params := regionCallParams(t)
	for i := range calls {
		resp, err := m.Call(context.Background(),
			jsonrpc.NewRequest(strconv.Itoa(i+1), mcpspec.MethodToolsCall, params), nil)
		require.NoError(t, err)
		require.NotNil(t, resp.Error, "the backend rejects every call while its listing is unreadable")
		require.Equal(t, mcpspec.ErrHeaderMismatch, resp.Error.Code)
	}

	assert.Equal(t, int32(calls), callAttempts.Load(),
		"nothing was learned, so no call may be reissued")
	assert.Equal(t, int32(annotationProbeMaxPages), listPages.Load(),
		"one sweep covers all %d rejections; a sweep apiece costs %d pages",
		calls, calls*annotationProbeMaxPages)

	c.mu.Lock()
	known := c.annotationsKnown
	c.mu.Unlock()
	assert.False(t, known,
		"a listing that never ended is not evidence that the server publishes no annotations")
}

// TestTruncationDoesNotPermanentlyDisableRecovery is the other half: the
// damping above must not become a latch.
//
// The listing belongs to the backend, and one that overran the page cap during
// a deploy or a bad rollout can come back under it a minute later. A conn that
// recorded the truncation once and refused ever to probe again would answer
// -32020 to every tools/call for the rest of its life — with the fix sitting
// one readable listing away.
func TestTruncationDoesNotPermanentlyDisableRecovery(t *testing.T) {
	var listPages, callAttempts atomic.Int32
	var endless atomic.Bool
	endless.Store(true)
	srv := endlessCursorServer(&listPages, &callAttempts, &endless)
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	c := conn.(*httpConn)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	params := regionCallParams(t)
	resp, err := m.Call(context.Background(),
		jsonrpc.NewRequest("1", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Error, "the probe could not reach the tool, so the call legitimately fails")
	require.Equal(t, int32(annotationProbeMaxPages), listPages.Load())

	// The backend's listing comes back under the cap, and the cooldown the
	// truncation started runs out. Rewound rather than waited out: what is
	// under test is that the window ends, not how long it is.
	endless.Store(false)
	c.mu.Lock()
	c.annotationsTruncatedAt = time.Now().Add(-annotationTruncationCooldown - time.Second)
	c.mu.Unlock()

	resp, err = m.Call(context.Background(),
		jsonrpc.NewRequest("2", mcpspec.MethodToolsCall, params), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error,
		"the recovery must run again once the listing the truncation described is gone")
	assert.Equal(t, int32(annotationProbeMaxPages+1), listPages.Load(),
		"the second probe reads the one page the recovered listing has")

	// And the recovered state is the ordinary one: the truncation is retired,
	// not merely expired, so the next rejection is judged on what is known.
	c.mu.Lock()
	known, truncatedAt := c.annotationsKnown, c.annotationsTruncatedAt
	c.mu.Unlock()
	assert.True(t, known, "a listing read to its end settles the toolset")
	assert.True(t, truncatedAt.IsZero(), "a terminating listing retires the truncation")
}

// TestRedirectCannotCarryTheCredentialAway covers the four things Go's default
// redirect policy will do with an injected credential.
//
// requireHTTPS constrains the URL an operator wrote; the URL actually dialled
// is chosen by the backend. Go follows ten hops, keeps Authorization across a
// scheme downgrade and across a subdomain hop (isDomainOrSubdomain never looks
// at the scheme), never strips the BODY, and hands the final response back to
// the caller — so a redirect is at once credential theft, cleartext downgrade,
// and a read primitive into the pod's network.
func TestRedirectCannotCarryTheCredentialAway(t *testing.T) {
	var stolen atomic.Int32
	thief := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			stolen.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"g1","result":{"secret":"internal"}}`))
	}))
	t.Cleanup(thief.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, thief.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	b := &HTTPBackend{URL: redirector.URL, Headers: map[string]string{"Authorization": "Bearer SUPER-SECRET"}}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	resp, err := m.Call(context.Background(),
		jsonrpc.NewRequest("1", mcpspec.MethodToolsList, nil), nil)
	// However it surfaces — a transport error or a synthesized JSON-RPC error —
	// what must not happen is the hop being followed.
	assert.Equal(t, int32(0), stolen.Load(), "the credential reached the redirect target")
	if err == nil && resp != nil {
		assert.NotContains(t, string(resp.Result), "internal",
			"the redirect target's body reached the caller")
	}
}

// A client is injected for the TRANSPORT — a proxy, a custom TLS config, a
// test hook — and nothing about that says the caller meant to opt out of
// redirect policy. Guarding only the nil case left the protection on a
// default nobody had to keep: the first caller to pass a client for an
// unrelated reason would silently get Go's browser rules back, carrying this
// backend's injected Authorization wherever a hop pointed.
func TestInjectedClientStillRefusesUnsafeRedirects(t *testing.T) {
	var stolen atomic.Int32
	thief := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			stolen.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"g1","result":{"secret":"internal"}}`))
	}))
	t.Cleanup(thief.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, thief.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	caller := &http.Client{} // no CheckRedirect: Go's default would follow
	b := &HTTPBackend{
		URL:     redirector.URL,
		Headers: map[string]string{"Authorization": "Bearer SUPER-SECRET"},
		Client:  caller,
	}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	_, _ = m.Call(context.Background(), jsonrpc.NewRequest("1", mcpspec.MethodToolsList, nil), nil)
	assert.Equal(t, int32(0), stolen.Load(), "the credential reached the redirect target")
	assert.Nil(t, caller.CheckRedirect, "the caller's own client was mutated instead of copied")
}

// TestRedirectCannotRewriteTheMethod covers the hop that changes what the
// exchange IS rather than where it goes.
//
// Go rewrites POST to GET on 301/302/303 and drops the body. Every authority
// check still passes on a same-origin hop, so nothing else here stops it: the
// injected credential rides along and the reply reaches the caller as an MCP
// result, making an MCP route that can be made to redirect a read of every
// GET-able path beside it. 307/308 preserve the method and stay allowed.
func TestRedirectCannotRewriteTheMethod(t *testing.T) {
	mk := func(method, u string) *http.Request {
		r, err := http.NewRequest(method, u, nil)
		require.NoError(t, err)
		return r
	}
	assert.Error(t, RefuseUnsafeRedirect(
		mk(http.MethodGet, "https://h/admin"), []*http.Request{mk(http.MethodPost, "https://h/mcp")}),
		"a 301/302/303 rewrite of the POST must not be followed, same origin or not")
	assert.NoError(t, RefuseUnsafeRedirect(
		mk(http.MethodPost, "https://h/mcp/v2"), []*http.Request{mk(http.MethodPost, "https://h/mcp")}),
		"307/308 keep the method, and a same-origin path hop is the one thing allowed")
	assert.NoError(t, RefuseUnsafeRedirect(
		mk(http.MethodGet, "https://h/mcp/"), []*http.Request{mk(http.MethodGet, "https://h/mcp")}),
		"the SSE stream is a GET to begin with; a GET->GET path hop is unchanged")
}

// TestSameOriginRedirectCannotReadAnotherPath is the end-to-end form: the hop
// never leaves the backend's own host and port, so the authority checks are
// satisfied and only the method rule stands between a 302 and another path's
// body being returned as this call's result.
func TestSameOriginRedirectCannotReadAnotherPath(t *testing.T) {
	var readSecret atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/secret", func(w http.ResponseWriter, r *http.Request) {
		readSecret.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"g1","result":{"secret":"internal"}}`))
	})
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/secret", http.StatusFound) // 302: Go rewrites POST to GET
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	b := &HTTPBackend{URL: srv.URL + "/mcp", Headers: map[string]string{"Authorization": "Bearer SUPER-SECRET"}}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })

	resp, err := m.Call(context.Background(),
		jsonrpc.NewRequest("1", mcpspec.MethodToolsList, nil), nil)
	assert.Equal(t, int32(0), readSecret.Load(),
		"a same-origin 302 turned the POST into a GET of another path")
	if err == nil && resp != nil {
		assert.NotContains(t, string(resp.Result), "internal",
			"the other path's body reached the caller as an MCP result")
	}
}

func TestRefuseUnsafeRedirect(t *testing.T) {
	req := func(u string) *http.Request {
		r, err := http.NewRequest(http.MethodPost, u, nil)
		require.NoError(t, err)
		return r
	}
	tests := []struct {
		name, from, to string
		ok             bool
	}{
		{"same host, path only", "https://h/mcp", "https://h/mcp/", true},
		{"plain stays plain", "http://h/mcp", "http://h/other", true},
		{"scheme downgrade", "https://h/mcp", "http://h/mcp", false},
		{"different host", "https://h/mcp", "https://other/mcp", false},
		{"subdomain", "https://mcp.example.com/x", "https://evil.mcp.example.com/x", false},
		{"host case only", "https://H/mcp", "https://h/mcp", true},
		{"explicit default port", "https://h/mcp", "https://h:443/mcp", true},
		{"different port, same host", "http://h:8080/mcp", "http://h:9090/mcp", false},
		{"upgrade is fine", "http://h/mcp", "https://h/mcp", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RefuseUnsafeRedirect(req(tt.to), []*http.Request{req(tt.from)})
			if tt.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// TestRedirectHostFoldingIsASCIIOnly guards the comparison that decides
// whether a hop is "the same host".
//
// strings.EqualFold applies Unicode folding, under which U+212A KELVIN SIGN
// folds to k and U+017F LONG S folds to s — so it would read
// "ſlacK.example.com" as "slack.example.com". The name that goes on the wire
// disagrees: Request.write emits it through Punycode with no UTS46 mapping,
// producing a different DNS label. The TCP target is unchanged, so this is not
// SSRF; what changes is the Host header, and the injected credential rides
// along to what any Host-routed ingress treats as a different vhost.
func TestRedirectHostFoldingIsASCIIOnly(t *testing.T) {
	req := func(u string) *http.Request {
		r, err := http.NewRequest(http.MethodPost, u, nil)
		require.NoError(t, err)
		return r
	}
	for _, to := range []string{
		"http://ſlacK.example.com/steal", // ſlacK
		"http://slacK.example.com/steal", // slacK
		"http://ſlack.example.com/steal", // ſlack
	} {
		assert.Error(t, RefuseUnsafeRedirect(req(to), []*http.Request{req("http://slack.example.com/mcp")}),
			"a fold-equal spelling is a different DNS label on the wire: %q", to)
	}
	// ASCII case still folds, which is what the comparison is for.
	assert.NoError(t, RefuseUnsafeRedirect(
		req("http://SLACK.example.com/x"), []*http.Request{req("http://slack.example.com/mcp")},
	))
}

// TestRedirectHopCapIsEnforced pins the only bound on a same-host loop.
//
// Setting CheckRedirect REPLACES Go's built-in 10-hop cap, the backend client
// is built with Timeout 0 because response streams are long-lived, and the
// wire handler's context carries no deadline of its own — so a backend that
// redirects to itself would spin until something else gave out.
func TestRedirectHopCapIsEnforced(t *testing.T) {
	req := func(u string) *http.Request {
		r, err := http.NewRequest(http.MethodPost, u, nil)
		require.NoError(t, err)
		return r
	}
	via := func(n int) []*http.Request {
		out := make([]*http.Request, n)
		for i := range out {
			out[i] = req("https://h/mcp")
		}
		return out
	}
	assert.NoError(t, RefuseUnsafeRedirect(req("https://h/a"), via(5)),
		"a legitimate same-host chain must still be followed")
	assert.Error(t, RefuseUnsafeRedirect(req("https://h/a"), via(6)),
		"nothing else bounds this loop")
}
