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

// Package legacy presents a Modern (2026-07-28) face over a Legacy
// (2025-11-25) MCP server — the only schema-aware code on the gateway side.
//
// On connect it performs the legacy initialize handshake exactly once,
// deliberately advertising NO sampling/elicitation/roots client capabilities:
// a conformant legacy server then may not initiate server->client requests,
// which is what keeps the gateway stateless. From the cached
// InitializeResult it synthesizes server/discover without touching the
// subprocess, and it fans legacy */list_changed notifications into open
// subscriptions/listen streams.
package legacy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// handshakeTimeout bounds the one-time initialize exchange on connect.
const handshakeTimeout = 30 * time.Second

// Backend wraps a legacy backend, handshaking on every new connection.
type Backend struct {
	// Inner produces raw connections (stdio subprocess, HTTP).
	Inner backend.Backend
	// DiscoverTTLMs is served in the synthesized DiscoverResult (default 300000).
	DiscoverTTLMs int
	// CacheScope is served on every cacheable result (default "private").
	CacheScope string
	Logger     *slog.Logger
}

// Connect spawns the inner connection and performs the 2025-11-25 handshake.
func (b *Backend) Connect(ctx context.Context) (backend.Conn, error) {
	log := b.Logger
	if log == nil {
		log = slog.Default()
	}
	inner, err := b.Inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	init, err := handshake(ctx, inner)
	if err != nil {
		_ = inner.Close()
		return nil, fmt.Errorf("legacy: handshake: %w", err)
	}
	ttl := b.DiscoverTTLMs
	if ttl <= 0 {
		ttl = 300000
	}
	scope := b.CacheScope
	if scope == "" {
		// Fail closed: a legacy backend runs with per-tenant (often per-user)
		// credentials, so its listings are not safe to share across
		// authorization contexts unless someone says so explicitly.
		scope = mcpspec.CacheScopePrivate
	}
	return &conn{
		inner:      inner,
		init:       init,
		ttlMs:      ttl,
		cacheScope: scope,
		log:        log,
		synth:      make(chan *jsonrpc.Message, 16),
		listeners:  make(map[string]listenFilter),
		pending:    make(map[string]string),
	}, nil
}

// initResult is the slice of InitializeResult the bridge needs, kept raw so
// nothing is lost in translation.
type initResult struct {
	Capabilities json.RawMessage `json:"capabilities"`
	ServerInfo   json.RawMessage `json:"serverInfo"`
	Instructions json.RawMessage `json:"instructions"`
}

// handshake performs initialize + notifications/initialized. The advertised
// clientCapabilities are EMPTY on purpose — see the package comment.
func handshake(ctx context.Context, c backend.Conn) (*initResult, error) {
	ctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	params, _ := json.Marshal(map[string]any{
		"protocolVersion": mcpspec.LegacyProtocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "natsmcp-gateway", "version": "1.0"},
	})
	req := jsonrpc.NewRequest("natsmcp-init", mcpspec.MethodInitialize, params)
	if err := c.Write(ctx, req); err != nil {
		return nil, err
	}

	for {
		msg, err := c.Read(ctx)
		if err != nil {
			return nil, err
		}
		if msg.Kind() != jsonrpc.KindResponse || msg.IDKey() != req.IDKey() {
			continue // stray notifications before the response are legal
		}
		if msg.Error != nil {
			return nil, fmt.Errorf("initialize rejected: %s", msg.Error)
		}
		var init initResult
		if err := json.Unmarshal(msg.Result, &init); err != nil {
			return nil, fmt.Errorf("bad InitializeResult: %w", err)
		}
		if err := c.Write(ctx, jsonrpc.NewNotification(mcpspec.NotifInitialized, nil)); err != nil {
			return nil, err
		}
		return &init, nil
	}
}

// listenFilter is the slice of SubscriptionFilter v1 honors.
type listenFilter struct {
	Tools     bool `json:"toolsListChanged"`
	Prompts   bool `json:"promptsListChanged"`
	Resources bool `json:"resourcesListChanged"`
}

// conn is the modern-facing connection over a handshaken legacy one.
type conn struct {
	inner      backend.Conn
	init       *initResult
	ttlMs      int
	cacheScope string
	log        *slog.Logger

	// synth carries locally-synthesized messages (discover responses,
	// listen acknowledgments and notifications, graceful closures) into Read.
	synth chan *jsonrpc.Message

	mu        sync.Mutex
	listeners map[string]listenFilter // listen request id key -> filter
	pending   map[string]string       // in-flight request id key -> method
	readCh    chan readResult         // in-flight inner read, if any
	closed    bool
	// closures holds the graceful-closure responses Close queued, drained by
	// Read ahead of the transport error.
	closures []*jsonrpc.Message
	deferErr error // transport error withheld until synth and closures drain
}

type readResult struct {
	msg *jsonrpc.Message
	err error
}

// Close ends every open subscription gracefully before dropping the
// connection. The spec distinguishes the two endings: a listen stream that
// receives its (empty) response closed cleanly, while one whose transport
// simply stops is an unexpected disconnect the client may retry. Without this
// a drain or pool eviction is indistinguishable from a backend crash, and the
// caller sees -32010 stream lost.
func (c *conn) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		for key := range c.listeners {
			result, err := json.Marshal(map[string]any{
				"resultType": mcpspec.ResultTypeComplete,
				"_meta":      map[string]json.RawMessage{mcpspec.MetaSubscriptionID: json.RawMessage(key)},
			})
			if err != nil {
				continue
			}
			// Queued on an unbounded slice, not into synth: synth holds 16 and
			// may already carry fan-out notifications, so a non-blocking send
			// would hand the abrupt disconnect this exists to prevent to every
			// subscription past the sixteenth.
			c.closures = append(c.closures, jsonrpc.NewResponse(json.RawMessage(key), result))
		}
		clear(c.listeners)
	}
	c.mu.Unlock()
	return c.inner.Close()
}

func (c *conn) Write(ctx context.Context, msg *jsonrpc.Message) error {
	switch {
	case msg.Kind() == jsonrpc.KindRequest && msg.Method == mcpspec.MethodDiscover:
		// Synthesized from the cached InitializeResult; the subprocess never
		// sees it. supportedVersions is the GATEWAY's supported set — from the
		// client's viewpoint the gateway is the modern server.
		result := map[string]any{
			"resultType":        mcpspec.ResultTypeComplete,
			"supportedVersions": mcpspec.SupportedProtocolVersions,
			"capabilities":      orEmpty(c.init.Capabilities),
			"ttlMs":             c.ttlMs,
			"cacheScope":        c.cacheScope,
			// serverInfo left DiscoverResult's top level for result _meta on
			// 2026-07-16; it is no longer a field of the result itself.
			"_meta": map[string]json.RawMessage{
				mcpspec.MetaServerInfo: orEmpty(c.init.ServerInfo),
			},
		}
		if len(c.init.Instructions) > 0 {
			result["instructions"] = c.init.Instructions
		}
		raw, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if !c.push(ctx, jsonrpc.NewResponse(msg.ID, raw)) {
			// Returning nil here would report success for a request that will
			// never be answered.
			return context.Cause(ctx)
		}
		return nil

	case msg.Kind() == jsonrpc.KindRequest && msg.Method == mcpspec.MethodListen:
		// Register the stream. No RESPONSE is synthesized — a listen stream
		// stays open until cancelled — but the acknowledgment MUST be the
		// first message on it, before any notification.
		var p struct {
			Notifications listenFilter `json:"notifications"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		// Queue the ack BEFORE registering. synth is FIFO, and fan-out only
		// targets registered listeners — so nothing can reach this stream
		// ahead of its acknowledgment. Registering first leaves a window in
		// which a concurrent list_changed is queued first, which is precisely
		// the ordering the spec makes a MUST.
		//
		// And if the ack cannot be queued at all, no listener is registered:
		// a stream that never got its acknowledgment is not a stream the
		// client can use, and registering one anyway would leave it live but
		// non-conformant.
		if !c.push(ctx, ackFor(msg.IDKey(), p.Notifications)) {
			return context.Cause(ctx)
		}
		c.mu.Lock()
		// Close may have run while the ack was queueing. Registering now
		// would resurrect the listener map after Close drained it, leaving a
		// subscription that never receives its graceful closure.
		if c.closed {
			c.mu.Unlock()
			return backend.ErrConnDead
		}
		c.listeners[msg.IDKey()] = p.Notifications
		c.mu.Unlock()
		return nil

	case msg.Kind() == jsonrpc.KindNotification && msg.Method == mcpspec.NotifCancelled:
		// A cancel for a registered listen stream is ours; everything else
		// forwards (in-flight tool calls).
		var p struct {
			RequestID json.RawMessage `json:"requestId"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		key := string(p.RequestID)
		c.mu.Lock()
		_, isListen := c.listeners[key]
		delete(c.listeners, key)
		delete(c.pending, key) // no response is coming; do not leak the entry
		c.mu.Unlock()
		if isListen {
			return nil
		}
		return c.inner.Write(ctx, msg)

	default:
		tracked := ""
		if msg.Kind() == jsonrpc.KindRequest {
			// Remember what was asked so the response can be recognized as
			// cacheable on the way back — a legacy server tells us nothing.
			// Recorded BEFORE the write, because the response may already be
			// in flight by the time Write returns.
			if mcpspec.IsCacheableMethod(msg.Method) {
				tracked = msg.IDKey()
				c.mu.Lock()
				c.pending[tracked] = msg.Method
				c.mu.Unlock()
			}
			msg = stripModernMeta(msg)
		}
		err := c.inner.Write(ctx, msg)
		if err != nil && tracked != "" {
			// The request never left. No response will retire this entry, and
			// the mux does not send a cancel for a failed write — so without
			// this it would outlive the request permanently.
			c.mu.Lock()
			delete(c.pending, tracked)
			c.mu.Unlock()
		}
		return err
	}
}

// ackFor builds the subscription acknowledgment. Its notifications field
// reports the subset this bridge actually honors, so a client asking for
// resourceSubscriptions — which v1 cannot serve off a legacy server — learns
// that immediately instead of waiting forever for an update that never comes.
func ackFor(idKey string, f listenFilter) *jsonrpc.Message {
	honored := map[string]bool{}
	if f.Tools {
		honored["toolsListChanged"] = true
	}
	if f.Prompts {
		honored["promptsListChanged"] = true
	}
	if f.Resources {
		honored["resourcesListChanged"] = true
	}
	params, _ := json.Marshal(map[string]any{
		"_meta":         map[string]json.RawMessage{mcpspec.MetaSubscriptionID: json.RawMessage(idKey)},
		"notifications": honored,
	})
	return jsonrpc.NewNotification(mcpspec.NotifSubscriptionsAcknowledged, params)
}

func (c *conn) Read(ctx context.Context) (*jsonrpc.Message, error) {
	for {
		select {
		case m := <-c.synth:
			return m, nil
		default:
		}
		c.mu.Lock()
		if len(c.closures) > 0 {
			m := c.closures[0]
			c.closures = c.closures[1:]
			c.mu.Unlock()
			return m, nil
		}
		// A transport error withheld so that queued closures could drain
		// surfaces here, once nothing synthesized is left.
		deferred := c.deferErr
		c.deferErr = nil
		c.mu.Unlock()
		if deferred != nil {
			return nil, deferred
		}
		// Poll-read the inner connection but stay responsive to synth: a
		// discover answered locally must not wait behind a quiet subprocess.
		msg, err := c.innerReadOrSynth(ctx)
		if err != nil {
			return nil, err
		}
		if msg == nil {
			continue
		}
		if msg.Kind() == jsonrpc.KindNotification {
			if out := c.translateNotification(msg); out != nil {
				return out, nil
			}
			// list_changed fan-out already queued on synth (or dropped).
			continue
		}
		if msg.Kind() == jsonrpc.KindResponse {
			c.stampResult(msg)
		}
		return msg, nil
	}
}

// stampResult fills in what a 2026-07-28 client requires and a 2025-11-25
// server cannot know to send. The gateway presents itself as a modern server,
// so "clients MUST tolerate an earlier-protocol server omitting these" is not
// a licence we get to use. Only absent fields are written: a backend that
// grows its own values keeps them.
func (c *conn) stampResult(msg *jsonrpc.Message) {
	// Retire the pending entry FIRST, whatever the outcome. An error response
	// still ends that request, and returning before the delete would leak one
	// entry per failed call for the life of a pooled connection.
	c.mu.Lock()
	method, cacheable := c.pending[msg.IDKey()]
	delete(c.pending, msg.IDKey())
	c.mu.Unlock()

	if msg.Error != nil || len(msg.Result) == 0 {
		return
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(msg.Result, &obj) != nil || obj == nil {
		return // not an object (or malformed); pass it through untouched
	}
	var additions []string
	add := func(key, rawJSON string) {
		if _, present := obj[key]; !present {
			additions = append(additions, strconv.Quote(key)+":"+rawJSON)
		}
	}
	add("resultType", strconv.Quote(mcpspec.ResultTypeComplete))
	if cacheable {
		// Interim input_required results are never cacheable, but this bridge
		// advertises no client capabilities, so a legacy server has nothing to
		// ask for and every result it returns is complete.
		add("ttlMs", strconv.Itoa(c.ttlMsFor(method)))
		add("cacheScope", strconv.Quote(c.cacheScope))
	}
	if len(additions) == 0 {
		return
	}
	if spliced, ok := spliceFields(msg.Result, additions, len(obj) == 0); ok {
		msg.Result = spliced
	}
}

// ttlMsFor picks the freshness hint for one method's result.
//
// Discovery and the list methods describe the server's SHAPE: it changes
// rarely, and when it does this bridge forwards the legacy server's
// */list_changed as an invalidation, so the configured TTL is safe.
//
// resources/read returns CONTENT, and a 2025-11-25 server gives us no
// invalidation signal for it whatsoever. Handing a client the discovery TTL
// there would licence it to serve a five-minute-stale file. 0 — "consider
// this immediately stale" — is the only honest hint we can give.
func (c *conn) ttlMsFor(method string) int {
	if method == mcpspec.MethodResourcesRead {
		return 0
	}
	return c.ttlMs
}

// spliceFields inserts fields directly after a JSON object's opening brace,
// leaving every other byte exactly as the backend sent it.
//
// Re-encoding the parsed object would be shorter, but encoding/json
// HTML-escapes <, > and & inside strings and reorders keys — so a tool result
// carrying source code or markup would reach the client rewritten, and a
// multi-megabyte payload would be re-marshalled on every single call purely
// to add fields that are always absent.
func spliceFields(raw []byte, fields []string, empty bool) ([]byte, bool) {
	i := 0
	for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t' || raw[i] == '\n' || raw[i] == '\r') {
		i++
	}
	if i >= len(raw) || raw[i] != '{' {
		return nil, false
	}
	body := strings.Join(fields, ",")
	if !empty {
		body += "," // only when a field follows it
	}
	out := make([]byte, 0, len(raw)+len(body))
	out = append(out, raw[:i+1]...)
	out = append(out, body...)
	return append(out, raw[i+1:]...), true
}

// innerReadOrSynth returns the next inner message, or nil if a synth message
// became available first (the caller loops and picks it up).
func (c *conn) innerReadOrSynth(ctx context.Context) (*jsonrpc.Message, error) {
	// The inner read must survive this call: successive Read calls reuse it.
	c.mu.Lock()
	if c.readCh == nil {
		ch := make(chan readResult, 1)
		c.readCh = ch
		go func() {
			m, err := c.inner.Read(context.Background())
			ch <- readResult{m, err}
		}()
	}
	ch := c.readCh
	c.mu.Unlock()

	select {
	case r := <-ch:
		c.mu.Lock()
		c.readCh = nil
		c.mu.Unlock()
		if r.err != nil {
			// Closing the inner connection is HOW graceful closure is
			// delivered, so the read error it causes races the closure
			// responses Close just queued. Hand those over first and hold the
			// error until synth is empty — otherwise the very messages that
			// mark a clean shutdown are the ones the shutdown discards.
			c.mu.Lock()
			pendingClosures := len(c.closures) > 0
			c.mu.Unlock()
			select {
			case m := <-c.synth:
				c.withholdErr(r.err)
				return m, nil
			default:
				if pendingClosures {
					// Let Read loop round and hand them over first.
					c.withholdErr(r.err)
					return nil, nil
				}
			}
		}
		return r.msg, r.err
	case m := <-c.synth:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// withholdErr remembers a transport error so it surfaces only after every
// synthesized message has been handed over.
func (c *conn) withholdErr(err error) {
	c.mu.Lock()
	if c.deferErr == nil {
		c.deferErr = err
	}
	c.mu.Unlock()
}

// translateNotification handles legacy server notifications. It returns the
// message to surface directly, or nil if handled (fanned out or dropped).
func (c *conn) translateNotification(msg *jsonrpc.Message) *jsonrpc.Message {
	var want func(listenFilter) bool
	switch msg.Method {
	case mcpspec.NotifToolsListChanged:
		want = func(f listenFilter) bool { return f.Tools }
	case mcpspec.NotifPromptsListChanged:
		want = func(f listenFilter) bool { return f.Prompts }
	case mcpspec.NotifResourcesListChanged:
		want = func(f listenFilter) bool { return f.Resources }
	default:
		// progress etc. pass through untouched; the mux attributes them.
		return msg
	}

	c.mu.Lock()
	ids := make([]json.RawMessage, 0, len(c.listeners))
	for key, f := range c.listeners {
		if want(f) {
			ids = append(ids, json.RawMessage(key))
		}
	}
	c.mu.Unlock()

	for _, id := range ids {
		params := map[string]json.RawMessage{}
		if len(msg.Params) > 0 {
			_ = json.Unmarshal(msg.Params, &params)
		}
		meta := map[string]json.RawMessage{}
		if raw, ok := params["_meta"]; ok {
			_ = json.Unmarshal(raw, &meta)
		}
		meta[mcpspec.MetaSubscriptionID] = id
		metaRaw, _ := json.Marshal(meta)
		params["_meta"] = metaRaw
		paramsRaw, _ := json.Marshal(params)
		select {
		case c.synth <- jsonrpc.NewNotification(msg.Method, paramsRaw):
		default:
			c.log.Warn("listen fan-out queue full, dropping notification", "method", msg.Method)
		}
	}
	return nil
}

// push queues a synthesized message for Read, reporting whether it was
// queued. A dropped message is not always harmless — a subscription
// acknowledgment MUST precede its stream — so the caller decides.
func (c *conn) push(ctx context.Context, m *jsonrpc.Message) bool {
	select {
	case c.synth <- m:
		return true
	case <-ctx.Done():
		return false
	}
}

// stripModernMeta removes io.modelcontextprotocol/* keys from params._meta
// before a request reaches a legacy server that never negotiated them.
func stripModernMeta(msg *jsonrpc.Message) *jsonrpc.Message {
	var params map[string]json.RawMessage
	if len(msg.Params) == 0 || json.Unmarshal(msg.Params, &params) != nil {
		return msg
	}
	rawMeta, ok := params["_meta"]
	if !ok {
		return msg
	}
	var meta map[string]json.RawMessage
	if json.Unmarshal(rawMeta, &meta) != nil {
		return msg
	}
	changed := false
	for k := range meta {
		if strings.HasPrefix(k, "io.modelcontextprotocol/") {
			delete(meta, k)
			changed = true
		}
	}
	if !changed {
		return msg
	}
	if len(meta) == 0 {
		delete(params, "_meta")
	} else {
		metaRaw, _ := json.Marshal(meta)
		params["_meta"] = metaRaw
	}
	paramsRaw, _ := json.Marshal(params)
	return &jsonrpc.Message{JSONRPC: "2.0", ID: msg.ID, Method: msg.Method, Params: paramsRaw}
}

// orEmpty substitutes an empty object for a value a legacy server did not
// supply. A literal `null` counts as not supplied: a 2025-11-25 server may
// answer initialize with "capabilities": null, and forwarding that into a
// DiscoverResult hands a modern client a null where an object is required.
func orEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return json.RawMessage(`{}`)
	}
	return raw
}
