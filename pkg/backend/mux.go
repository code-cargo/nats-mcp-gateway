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
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// Mux multiplexes many concurrent logical callers onto one Conn, correlating
// by rewritten JSON-RPC id. Within a pool entry all callers share a tenant
// and credential set, so multiplexing is safe; the pool key guarantees
// tenants never share a Mux.
//
// Notification attribution:
//   - notifications/progress routes by progressToken (rewritten on the way
//     in, restored on the way out);
//   - notifications carrying _meta subscriptionId route to the
//     subscriptions/listen request they reference;
//   - anything else has no correlator and is logged at the gateway, never
//     forwarded — the documented cost of multiplexing.
type Mux struct {
	conn Conn
	log  *slog.Logger

	seq atomic.Uint64

	mu     sync.Mutex
	calls  map[string]*call // muxed id (raw JSON bytes) -> call
	tokens map[string]*call // muxed progress token -> call

	dead    chan struct{}
	deadErr error
	once    sync.Once
}

type call struct {
	// method is the request's MCP method. Only a subscriptions/listen call
	// can produce a response whose body names a subscription, so this is what
	// gates the closure rewrite below.
	method    string
	origID    json.RawMessage
	origToken json.RawMessage // caller's progressToken, nil if none
	notify    func(*jsonrpc.Message)
	resp      chan *jsonrpc.Message
}

// NewMux wraps a connection and starts its read loop.
func NewMux(conn Conn, log *slog.Logger) *Mux {
	if log == nil {
		log = slog.Default()
	}
	m := &Mux{
		conn:   conn,
		log:    log,
		calls:  make(map[string]*call),
		tokens: make(map[string]*call),
		dead:   make(chan struct{}),
	}
	go m.readLoop()
	return m
}

// Dead reports whether the underlying connection has failed.
func (m *Mux) Dead() bool {
	select {
	case <-m.dead:
		return true
	default:
		return false
	}
}

// Close closes the underlying connection, failing all in-flight calls.
func (m *Mux) Close() error { return m.conn.Close() }

// Call sends one request and blocks until its response, ctx cancellation, or
// connection death. notify receives this request's notifications (progress,
// listen events) with the caller's own id/token restored; it must not block.
// On ctx cancellation a notifications/cancelled is sent to the backend
// best-effort and ctx.Err() is returned — the wire layer then emits the
// empty-body end frame.
func (m *Mux) Call(ctx context.Context, msg *jsonrpc.Message, notify func(*jsonrpc.Message)) (*jsonrpc.Message, error) {
	if msg.Kind() != jsonrpc.KindRequest {
		return nil, fmt.Errorf("backend: Call requires a request, got %v", msg.Kind())
	}
	if notify == nil {
		notify = func(*jsonrpc.Message) {}
	}

	n := m.seq.Add(1)
	muxID := json.RawMessage(strconv.Quote("g" + strconv.FormatUint(n, 10)))
	muxToken := "gt" + strconv.FormatUint(n, 10)

	c := &call{
		method: msg.Method,
		origID: msg.ID,
		notify: notify,
		resp:   make(chan *jsonrpc.Message, 1),
	}

	out := &jsonrpc.Message{
		JSONRPC: "2.0",
		ID:      muxID,
		Method:  msg.Method,
		Params:  msg.Params,
	}
	// Rewrite the caller's progressToken (if any) to a mux-unique one: two
	// concurrent callers may have picked the same token.
	if tok, params, ok := rewriteProgressToken(msg.Params, muxToken); ok {
		c.origToken = tok
		out.Params = params
	}

	m.mu.Lock()
	if m.Dead() {
		m.mu.Unlock()
		return nil, m.deadError()
	}
	m.calls[string(muxID)] = c
	if c.origToken != nil {
		m.tokens[muxToken] = c
	}
	m.mu.Unlock()

	unregister := func() {
		m.mu.Lock()
		delete(m.calls, string(muxID))
		delete(m.tokens, muxToken)
		m.mu.Unlock()
	}

	if err := m.conn.Write(ctx, out); err != nil {
		unregister()
		return nil, fmt.Errorf("backend: write request: %w", err)
	}

	select {
	case resp := <-c.resp:
		unregister()
		resp.ID = c.origID
		return resp, nil
	case <-ctx.Done():
		unregister()
		// A response that already landed wins here too: both channels can be
		// ready at once, and reporting a deadline for work the backend finished
		// would also send it a cancel for a request it already answered.
		select {
		case resp := <-c.resp:
			resp.ID = c.origID
			return resp, nil
		default:
		}
		// Best-effort cancellation toward the backend; fresh context because
		// ctx is already done.
		cancelParams, _ := json.Marshal(map[string]json.RawMessage{"requestId": muxID})
		_ = m.conn.Write(context.Background(),
			jsonrpc.NewNotification(mcpspec.NotifCancelled, cancelParams))
		return nil, ctx.Err()
	case <-m.dead:
		unregister()
		// A response that landed before the connection died wins. Closing the
		// conn is HOW a graceful subscription closure is delivered, so both
		// channels are ready at once and a bare select would pick randomly —
		// reporting "connection dead" for a stream that ended cleanly, which
		// is the exact distinction the closure exists to draw.
		select {
		case resp := <-c.resp:
			resp.ID = c.origID
			return resp, nil
		default:
		}
		return nil, m.deadError()
	}
}

func (m *Mux) deadError() error {
	if m.deadErr != nil {
		return fmt.Errorf("backend: connection dead: %w", m.deadErr)
	}
	return ErrConnDead
}

func (m *Mux) readLoop() {
	for {
		msg, err := m.conn.Read(context.Background())
		if err != nil {
			m.once.Do(func() {
				m.deadErr = err
				close(m.dead) // every blocked Call sees this and fails
			})
			return
		}
		switch msg.Kind() {
		case jsonrpc.KindResponse:
			m.routeResponse(msg)
		case jsonrpc.KindNotification:
			m.routeNotification(msg)
		case jsonrpc.KindRequest:
			// Server-initiated requests have no message direction on the
			// 2026-07-28 wire. Answer immediately so the server does not
			// block forever on a response that can never come.
			m.log.Warn("rejecting server-initiated request", "method", msg.Method)
			_ = m.conn.Write(context.Background(), jsonrpc.NewErrorResponse(
				msg.ID, jsonrpc.CodeMethodNotFound, MsgServerInitiatedUnsupported, nil,
			))
		default:
		}
	}
}

func (m *Mux) routeResponse(msg *jsonrpc.Message) {
	m.mu.Lock()
	c := m.calls[msg.IDKey()]
	m.mu.Unlock()
	if c == nil {
		m.log.Debug("response for unknown id dropped", "id", string(msg.ID))
		return
	}
	// A subscriptions/listen closure is the one response whose BODY names a
	// request: it carries the subscription id in result _meta. That id is this
	// mux's rewritten one, so restore the caller's — otherwise the message
	// that ends the stream refers to an id the caller never issued, and leaks
	// a gateway-internal identifier while doing it.
	//
	// Gated on the METHOD, not on the payload. This is the funnel every
	// response passes through, including multi-megabyte tool results, and
	// metaField has to walk the whole document; a content probe would also
	// fire on any result that merely mentions the key, such as a resource
	// containing this specification. Only a listen call can close a stream.
	if c.method == mcpspec.MethodListen {
		if _, ok := metaField(msg.Result, mcpspec.MetaSubscriptionID); ok {
			if result, ok := setMetaField(msg.Result, mcpspec.MetaSubscriptionID, c.origID); ok {
				msg.Result = result
			}
		}
	}
	select {
	case c.resp <- msg:
	default: // duplicate response; drop
	}
}

func (m *Mux) routeNotification(msg *jsonrpc.Message) {
	// notifications/progress: correlate by top-level params.progressToken.
	if msg.Method == mcpspec.NotifProgress {
		var p struct {
			ProgressToken json.RawMessage `json:"progressToken"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		var tok string
		_ = json.Unmarshal(p.ProgressToken, &tok)
		m.mu.Lock()
		c := m.tokens[tok]
		m.mu.Unlock()
		if c == nil {
			m.log.Debug("progress for unknown token dropped", "token", tok)
			return
		}
		if params, ok := setObjectField(msg.Params, "progressToken", c.origToken); ok {
			msg.Params = params
		}
		c.notify(msg)
		return
	}

	// Listen events: correlate by _meta subscriptionId, which references the
	// muxed id of the subscriptions/listen request.
	if subID, ok := metaField(msg.Params, mcpspec.MetaSubscriptionID); ok {
		m.mu.Lock()
		c := m.calls[string(subID)]
		m.mu.Unlock()
		if c == nil {
			m.log.Debug("notification for unknown subscription dropped", "method", msg.Method)
			return
		}
		if params, ok := setMetaField(msg.Params, mcpspec.MetaSubscriptionID, c.origID); ok {
			msg.Params = params
		}
		c.notify(msg)
		return
	}

	// No correlator (e.g. notifications/message): log at the gateway, never
	// forward — in a multiplexed process it is fundamentally unattributable.
	m.log.Info("unattributable backend notification", "method", msg.Method,
		"params", truncate(msg.Params, 300))
}

// --- shallow JSON editing helpers -----------------------------------------

var errNotObject = errors.New("not a JSON object")

func decodeObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(raw) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, errNotObject
	}
	if obj == nil {
		obj = map[string]json.RawMessage{}
	}
	return obj, nil
}

// rewriteProgressToken swaps params._meta.progressToken for muxToken.
// Returns the original token, the rewritten params, and whether a token was
// present.
func rewriteProgressToken(params json.RawMessage, muxToken string) (json.RawMessage, json.RawMessage, bool) {
	obj, err := decodeObject(params)
	if err != nil {
		return nil, nil, false
	}
	meta, err := decodeObject(obj["_meta"])
	if err != nil {
		return nil, nil, false
	}
	orig, present := meta[mcpspec.MetaProgressToken]
	if !present {
		return nil, nil, false
	}
	meta[mcpspec.MetaProgressToken] = json.RawMessage(strconv.Quote(muxToken))
	metaRaw, err := marshalNoEscape(meta)
	if err != nil {
		return nil, nil, false
	}
	obj["_meta"] = metaRaw
	out, err := marshalNoEscape(obj)
	if err != nil {
		return nil, nil, false
	}
	return orig, out, true
}

// setObjectField sets a top-level field of a JSON object.
func setObjectField(params json.RawMessage, key string, val json.RawMessage) (json.RawMessage, bool) {
	obj, err := decodeObject(params)
	if err != nil {
		return nil, false
	}
	obj[key] = val
	out, err := marshalNoEscape(obj)
	if err != nil {
		return nil, false
	}
	return out, true
}

// metaField reads params._meta[key].
func metaField(params json.RawMessage, key string) (json.RawMessage, bool) {
	obj, err := decodeObject(params)
	if err != nil {
		return nil, false
	}
	meta, err := decodeObject(obj["_meta"])
	if err != nil {
		return nil, false
	}
	v, ok := meta[key]
	return v, ok
}

// setMetaField sets params._meta[key].
func setMetaField(params json.RawMessage, key string, val json.RawMessage) (json.RawMessage, bool) {
	obj, err := decodeObject(params)
	if err != nil {
		return nil, false
	}
	meta, err := decodeObject(obj["_meta"])
	if err != nil {
		return nil, false
	}
	meta[key] = val
	metaRaw, err := marshalNoEscape(meta)
	if err != nil {
		return nil, false
	}
	obj["_meta"] = metaRaw
	// Escape-free: this rewrites a whole result or params document to change
	// one nested field, and the rest of it belongs to the backend.
	out, err := marshalNoEscape(obj)
	if err != nil {
		return nil, false
	}
	return out, true
}
