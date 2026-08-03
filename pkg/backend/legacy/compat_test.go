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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
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
		SupportedVersions []string                   `json:"supportedVersions"`
		ServerInfo        json.RawMessage            `json:"serverInfo"`
		Capabilities      json.RawMessage            `json:"capabilities"`
		TTLMs             int                        `json:"ttlMs"`
		CacheScope        string                     `json:"cacheScope"`
		Instructions      string                     `json:"instructions"`
		Meta              map[string]json.RawMessage `json:"_meta"`
	}
	require.NoError(t, json.Unmarshal(resp.Result, &d))
	assert.Equal(t, []string{mcpspec.ProtocolVersion}, d.SupportedVersions,
		"the gateway IS the modern server from the client's view")
	// serverInfo left DiscoverResult's top level for result _meta on
	// 2026-07-16; emitting it in both places would be the easy way to keep
	// this test green while shipping a field no conformant reader looks at.
	assert.Empty(t, d.ServerInfo, "serverInfo must no longer be a top-level field")
	assert.Contains(t, string(d.Meta[mcpspec.MetaServerInfo]), "fakemcp")
	assert.Contains(t, string(d.Capabilities), "tools")
	assert.Equal(t, 300000, d.TTLMs)
	assert.Equal(t, mcpspec.CacheScopePrivate, d.CacheScope,
		"a per-tenant backend's listing defaults to private")
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
	listenParams := listenFor(t, map[string]any{"toolsListChanged": true})
	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()

	listenDone := make(chan error, 1)
	go func() {
		_, err := m.Call(listenCtx, jsonrpc.NewRequest("sub-1", mcpspec.MethodListen, listenParams),
			func(n *jsonrpc.Message) { notifications <- n })
		listenDone <- err
	}()

	// The acknowledgment MUST arrive before any notification on the stream.
	ack := requireNotification(t, notifications, "acknowledgment")
	require.Equal(t, mcpspec.NotifSubscriptionsAcknowledged, ack.Method,
		"the ack must be the FIRST message on a listen stream")

	// Give the listen registration a moment, then trigger a list_changed.
	time.Sleep(200 * time.Millisecond)
	resp, err := m.Call(context.Background(), toolCall("4", "notify_changed"), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	select {
	case n := <-notifications:
		assert.Equal(t, mcpspec.NotifToolsListChanged, n.Method)
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
	listenParams := listenFor(t, map[string]any{"promptsListChanged": true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = m.Call(ctx, jsonrpc.NewRequest("sub-2", mcpspec.MethodListen, listenParams),
			func(n *jsonrpc.Message) { notifications <- n })
	}()
	ack := requireNotification(t, notifications, "acknowledgment")
	require.Equal(t, mcpspec.NotifSubscriptionsAcknowledged, ack.Method)
	time.Sleep(200 * time.Millisecond)

	_, err := m.Call(context.Background(), toolCall("6", "notify_changed"), nil)
	require.NoError(t, err)

	select {
	case n := <-notifications:
		t.Fatalf("filtered-out notification leaked: %s", n.Method)
	case <-time.After(1 * time.Second):
	}
}

// listenFor builds subscriptions/listen params. The SubscriptionFilter lives
// under params.notifications — NOT at the params root, which is what this
// bridge used to read and why no filter ever matched.
func listenFor(t *testing.T, filter map[string]any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"notifications": filter})
	require.NoError(t, err)
	return raw
}

func requireNotification(t *testing.T, ch <-chan *jsonrpc.Message, what string) *jsonrpc.Message {
	t.Helper()
	select {
	case n := <-ch:
		return n
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never arrived", what)
		return nil
	}
}

func TestListenAckReportsOnlyWhatTheBridgeHonors(t *testing.T) {
	m := legacyMux(t)
	notifications := make(chan *jsonrpc.Message, 8)
	// resourceSubscriptions cannot be served off a legacy server: there is no
	// resources/subscribe to bridge it to. The ack must say so by omission
	// rather than leaving the client waiting for updates that never come.
	listenParams := listenFor(t, map[string]any{
		"toolsListChanged":      true,
		"resourceSubscriptions": []string{"file:///watched"},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = m.Call(ctx, jsonrpc.NewRequest("sub-3", mcpspec.MethodListen, listenParams),
			func(n *jsonrpc.Message) { notifications <- n })
	}()

	ack := requireNotification(t, notifications, "acknowledgment")
	require.Equal(t, mcpspec.NotifSubscriptionsAcknowledged, ack.Method)

	var p struct {
		Meta          map[string]json.RawMessage `json:"_meta"`
		Notifications map[string]any             `json:"notifications"`
	}
	require.NoError(t, json.Unmarshal(ack.Params, &p))
	assert.JSONEq(t, `"sub-3"`, string(p.Meta[mcpspec.MetaSubscriptionID]),
		"the ack carries the CALLER's listen id")
	assert.Equal(t, true, p.Notifications["toolsListChanged"])
	assert.NotContains(t, p.Notifications, "resourceSubscriptions",
		"an unhonored type must be omitted, not echoed back as agreed")
}

func TestBridgedListResultsCarryCachingHints(t *testing.T) {
	m := legacyMux(t)
	// The legacy fake server returns a bare tools/list result. A modern
	// client is talking to what it believes is a 2026-07-28 server, so the
	// bridge owes it resultType and the caching hints.
	resp, err := m.Call(context.Background(),
		jsonrpc.NewRequest("7", mcpspec.MethodToolsList, json.RawMessage(`{}`)), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	var r struct {
		ResultType string          `json:"resultType"`
		TTLMs      *int            `json:"ttlMs"`
		CacheScope string          `json:"cacheScope"`
		Tools      json.RawMessage `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(resp.Result, &r))
	assert.Equal(t, mcpspec.ResultTypeComplete, r.ResultType)
	require.NotNil(t, r.TTLMs, "ttlMs is required on a cacheable result")
	assert.Equal(t, 300000, *r.TTLMs)
	assert.Equal(t, mcpspec.CacheScopePrivate, r.CacheScope)
	assert.Contains(t, string(r.Tools), "echo", "the backend's payload survives")
}

func TestToolCallResultIsNotStampedCacheable(t *testing.T) {
	m := legacyMux(t)
	resp, err := m.Call(context.Background(), toolCall("8", "echo"), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	var r map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(resp.Result, &r))
	assert.Equal(t, `"`+mcpspec.ResultTypeComplete+`"`, string(r["resultType"]),
		"every result carries resultType")
	assert.NotContains(t, r, "ttlMs", "tools/call results are not cacheable")
	assert.NotContains(t, r, "cacheScope")
}

func TestCloseEndsSubscriptionsGracefully(t *testing.T) {
	m := legacyMux(t)
	notifications := make(chan *jsonrpc.Message, 8)
	listenParams := listenFor(t, map[string]any{"toolsListChanged": true})

	type result struct {
		msg *jsonrpc.Message
		err error
	}
	done := make(chan result, 1)
	go func() {
		msg, err := m.Call(context.Background(),
			jsonrpc.NewRequest("sub-4", mcpspec.MethodListen, listenParams),
			func(n *jsonrpc.Message) { notifications <- n })
		done <- result{msg, err}
	}()
	ack := requireNotification(t, notifications, "acknowledgment")
	require.Equal(t, mcpspec.NotifSubscriptionsAcknowledged, ack.Method)

	// A drain or pool eviction is a graceful teardown. The subscription must
	// receive its (empty) response and end cleanly — a stream that just stops
	// is how an unexpected disconnect is signalled, and conflating the two
	// tells the client to reconnect when the gateway is going away.
	require.NoError(t, m.Close())

	select {
	case got := <-done:
		require.NoError(t, got.err, "graceful closure must not surface as a dead connection")
		require.NotNil(t, got.msg)
		require.Nil(t, got.msg.Error)
		var r struct {
			ResultType string                     `json:"resultType"`
			Meta       map[string]json.RawMessage `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(got.msg.Result, &r))
		assert.Equal(t, mcpspec.ResultTypeComplete, r.ResultType)
		assert.JSONEq(t, `"sub-4"`, string(r.Meta[mcpspec.MetaSubscriptionID]),
			"the closure names the subscription it ends")
	case <-time.After(10 * time.Second):
		t.Fatal("listen call never returned after Close")
	}
}

// newStampConn builds a bare conn for exercising stampResult directly; the
// map bookkeeping it does has no seam through the Mux.
func newStampConn() *conn {
	return &conn{
		ttlMs:      300000,
		cacheScope: mcpspec.CacheScopePrivate,
		pending:    map[string]string{},
		log:        slog.Default(),
	}
}

func TestStampResultRetiresPendingOnEveryOutcome(t *testing.T) {
	// The pending entry is this request's only bookkeeping. A response ends
	// the request whether it succeeded or not, so failing to retire it leaks
	// one entry per failed call for the life of a pooled connection — and a
	// backend erroring steadily is exactly when that matters.
	outcomes := map[string]*jsonrpc.Message{
		"error response":  jsonrpc.NewErrorResponse(json.RawMessage(`"1"`), -32000, "nope", nil),
		"empty result":    jsonrpc.NewResponse(json.RawMessage(`"1"`), nil),
		"non-object body": jsonrpc.NewResponse(json.RawMessage(`"1"`), json.RawMessage(`"a string"`)),
		"ordinary result": jsonrpc.NewResponse(json.RawMessage(`"1"`), json.RawMessage(`{"tools":[]}`)),
	}
	for name, msg := range outcomes {
		t.Run(name, func(t *testing.T) {
			c := newStampConn()
			c.pending[msg.IDKey()] = mcpspec.MethodToolsList
			c.stampResult(msg)
			assert.Empty(t, c.pending, "the pending entry must not outlive its response")
		})
	}
}

func TestStampResultTTLDependsOnWhatIsBeingCached(t *testing.T) {
	tests := []struct {
		method  string
		wantTTL string
	}{
		// Server shape: changes rarely, and this bridge forwards the legacy
		// server's */list_changed as an invalidation signal.
		{mcpspec.MethodToolsList, "300000"},
		{mcpspec.MethodPromptsList, "300000"},
		// Resource CONTENT: a 2025-11-25 server gives us no invalidation
		// signal at all, so anything above 0 would licence a client to serve
		// a stale file for five minutes.
		{mcpspec.MethodResourcesRead, "0"},
	}
	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			c := newStampConn()
			msg := jsonrpc.NewResponse(json.RawMessage(`"1"`), json.RawMessage(`{"x":1}`))
			c.pending[msg.IDKey()] = tc.method
			c.stampResult(msg)

			var got map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(msg.Result, &got))
			assert.Equal(t, tc.wantTTL, string(got["ttlMs"]))
			assert.Equal(t, `"`+mcpspec.CacheScopePrivate+`"`, string(got["cacheScope"]))
		})
	}
}

func TestStampResultPreservesTheBackendsBytes(t *testing.T) {
	// The payload must arrive as the backend sent it. Re-encoding through
	// encoding/json would HTML-escape <, > and & inside strings and reorder
	// keys, silently rewriting tool output that happens to contain markup or
	// source code.
	c := newStampConn()
	const payload = `{"content":[{"type":"text","text":"if (a < b && c > d) { return \"x\"; }"}],"zeta":1,"alpha":2}`
	msg := jsonrpc.NewResponse(json.RawMessage(`"1"`), json.RawMessage(payload))
	c.stampResult(msg)

	got := string(msg.Result)

	// The hazard is not hypothetical: the same payload through encoding/json
	// comes back rewritten, which is exactly why stampResult splices instead.
	var obj map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(payload), &obj))
	remarshalled, err := json.Marshal(obj)
	require.NoError(t, err)
	require.NotEqual(t, payload, string(remarshalled),
		"if re-encoding were lossless this test would be guarding nothing")

	// Only an insertion at the front: everything the backend sent follows it
	// byte for byte, key order and escaping included.
	assert.Equal(t, `{"resultType":"complete",`+payload[1:], got)
	assert.Contains(t, got, `if (a < b && c > d) { return \"x\"; }`)
}

func TestSpliceFields(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		fields []string
		empty  bool
		want   string
		wantOK bool
	}{
		{
			name: "into a populated object", raw: `{"a":1}`,
			fields: []string{`"resultType":"complete"`}, empty: false,
			want: `{"resultType":"complete","a":1}`, wantOK: true,
		},
		{
			// No trailing comma when nothing follows.
			name: "into an empty object", raw: `{}`,
			fields: []string{`"resultType":"complete"`}, empty: true,
			want: `{"resultType":"complete"}`, wantOK: true,
		},
		{
			name: "several fields", raw: `{"a":1}`,
			fields: []string{`"resultType":"complete"`, `"ttlMs":0`}, empty: false,
			want: `{"resultType":"complete","ttlMs":0,"a":1}`, wantOK: true,
		},
		{
			name: "leading whitespace is preserved", raw: "  {\"a\":1}",
			fields: []string{`"x":1`}, empty: false,
			want: "  {\"x\":1,\"a\":1}", wantOK: true,
		},
		{name: "not an object", raw: `[1,2]`, fields: []string{`"x":1`}, wantOK: false},
		{name: "empty input", raw: ``, fields: []string{`"x":1`}, wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := spliceFields([]byte(tc.raw), tc.fields, tc.empty)
			require.Equal(t, tc.wantOK, ok)
			if !ok {
				return
			}
			assert.Equal(t, tc.want, string(got))
			assert.True(t, json.Valid(got), "the result must still be valid JSON")
		})
	}
}

func TestWriteFailureDoesNotLeakPending(t *testing.T) {
	// The request never left, so no response will retire the entry and the
	// mux sends no cancel for a failed write. Without cleanup here it would
	// outlive the request for the life of a pooled connection.
	c := newStampConn()
	c.inner = failingConn{}
	c.listeners = map[string]listenFilter{}
	c.synth = make(chan *jsonrpc.Message, 4)

	req := jsonrpc.NewRequest("1", mcpspec.MethodToolsList, json.RawMessage(`{}`))
	err := c.Write(context.Background(), req)
	require.Error(t, err, "the write must surface its failure")
	assert.Empty(t, c.pending, "a request that never left must leave no bookkeeping")
}

// stubConn is an inner legacy connection whose reads the test feeds by hand.
type stubConn struct {
	reads chan *jsonrpc.Message
}

func (s *stubConn) Write(context.Context, *jsonrpc.Message) error { return nil }

func (s *stubConn) Read(ctx context.Context) (*jsonrpc.Message, error) {
	select {
	case m := <-s.reads:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *stubConn) Close() error { return nil }

// A */list_changed must reach EVERY subscription that asked for it. It is the
// only invalidation a 2025-11-25 server offers, so a subscriber that misses
// one goes on serving its cached listing for the whole discovery TTL — five
// minutes by default — with nothing to tell it or the client that the listing
// is wrong.
//
// The fan-out queue holds 16 and Read is what drains it, but the fan-out runs
// INSIDE Read: one list_changed with more than sixteen subscribers overflows
// the queue in a single pass, and everything past the sixteenth was logged and
// thrown away. Sixteen subscribers on one connection is an ordinary number —
// backends are pooled per (server, tenant), so every client of a tenant using
// a static-credential server shares one.
func TestListChangedReachesEverySubscriber(t *testing.T) {
	const subscribers = 40
	stub := &stubConn{reads: make(chan *jsonrpc.Message, 1)}
	c := &conn{
		inner:      stub,
		init:       &initResult{},
		ttlMs:      300000,
		cacheScope: mcpspec.CacheScopePrivate,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		synth:      make(chan *jsonrpc.Message, 16),
		listeners:  make(map[string]listenFilter),
		pending:    make(map[string]string),
	}
	want := map[string]bool{}
	for i := 0; i < subscribers; i++ {
		key := strconv.Quote(fmt.Sprintf("sub-%d", i))
		c.listeners[key] = listenFilter{Tools: true}
		want[key] = true
	}

	stub.reads <- jsonrpc.NewNotification(mcpspec.NotifToolsListChanged, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := map[string]bool{}
	for len(got) < subscribers {
		msg, err := c.Read(ctx)
		require.NoError(t, err,
			"only %d of %d subscribers received the invalidation", len(got), subscribers)
		require.Equal(t, mcpspec.NotifToolsListChanged, msg.Method)
		var p struct {
			Meta map[string]json.RawMessage `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(msg.Params, &p))
		got[string(p.Meta[mcpspec.MetaSubscriptionID])] = true
	}
	assert.Equal(t, want, got)
}

// handshakePinger is a 2025-11-25 server that pings inside the handshake
// window and blocks on the answer — it sends its InitializeResult only once
// the ping has been responded to. Sending a ping there is legal: 2025-11-25
// says a server SHOULD NOT send requests before the initialize response, and
// names ping and logging as the exceptions.
type handshakePinger struct {
	initSeen chan struct{}
	answered chan struct{}
	once     sync.Once
	step     int

	mu        sync.Mutex
	pingReply *jsonrpc.Message
}

func (c *handshakePinger) Write(_ context.Context, msg *jsonrpc.Message) error {
	switch {
	case msg.Kind() == jsonrpc.KindRequest && msg.Method == mcpspec.MethodInitialize:
		close(c.initSeen)
	case msg.Kind() == jsonrpc.KindResponse:
		c.mu.Lock()
		c.pingReply = msg
		c.mu.Unlock()
		c.once.Do(func() { close(c.answered) })
	}
	return nil
}

// Read is only ever called from the handshake's own goroutine, so step needs
// no guarding.
func (c *handshakePinger) Read(ctx context.Context) (*jsonrpc.Message, error) {
	c.step++
	switch c.step {
	case 1:
		select {
		case <-c.initSeen:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return jsonrpc.NewRequest("srv-ping", mcpspec.MethodPing, nil), nil
	case 2:
		select {
		case <-c.answered:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		result, _ := json.Marshal(map[string]any{
			"protocolVersion": mcpspec.LegacyProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "pinger"},
		})
		return jsonrpc.NewResponse(json.RawMessage(`"natsmcp-init"`), result), nil
	default:
		<-ctx.Done()
		return nil, ctx.Err()
	}
}

func (c *handshakePinger) Close() error { return nil }

// The mux answers a server-initiated request with -32601 so the subprocess is
// never left blocking on a response that cannot come — but the mux does not
// exist until the handshake has finished. Inside the handshake window the loop
// skipped anything that was not the InitializeResult, so a server waiting on
// its own request waited out the 30s handshake timeout and the whole
// connection failed. The README promises the -32601 without qualifying it.
func TestHandshakeAnswersServerInitiatedRequests(t *testing.T) {
	c := &handshakePinger{
		initSeen: make(chan struct{}),
		answered: make(chan struct{}),
	}
	// Well under handshakeTimeout: a swallowed request has to fail this test
	// promptly rather than sit out the real 30s.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	init, err := handshake(ctx, c)
	require.NoError(t, err, "the handshake never answered the server's ping")
	require.NotNil(t, init)
	assert.Contains(t, string(init.Capabilities), "tools")

	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotNil(t, c.pingReply, "the server-initiated request went unanswered")
	assert.JSONEq(t, `"srv-ping"`, string(c.pingReply.ID), "the answer must echo the server's id")
	require.NotNil(t, c.pingReply.Error)
	assert.Equal(t, jsonrpc.CodeMethodNotFound, c.pingReply.Error.Code)
}

// failingConn is a backend.Conn whose writes always fail.
type failingConn struct{}

func (failingConn) Write(context.Context, *jsonrpc.Message) error {
	return errors.New("pipe closed")
}

func (failingConn) Read(context.Context) (*jsonrpc.Message, error) { return nil, errors.New("closed") }

func (failingConn) Close() error { return nil }
