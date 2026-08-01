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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// HTTPBackend speaks Streamable HTTP to an MCP server. Hand-rolled on
// purpose: under 2026-07-28 the client is small (POST + required Mcp-Method/
// Mcp-Name headers; response is application/json or an SSE stream), and for
// 2025-11-25 servers only the Mcp-Session-Id dance is added — the legacy
// bridge (pkg/backend/legacy) runs above this Conn exactly as it does for
// stdio. The optional legacy GET stream is deliberately not opened: it
// carries only unsolicited server messages, which the gateway drops anyway.
type HTTPBackend struct {
	URL string
	// Headers are injected on every request (Authorization etc.) — the
	// static credential-injection point for HTTP backends.
	Headers map[string]string
	// TokenSource, when set, supplies additional per-request headers
	// resolved at call time (per-user bearer tokens), overriding Headers on
	// collision. Because it is consulted per request, a refreshed token
	// applies without rebuilding the conn.
	TokenSource TokenSource
	// Legacy enables Mcp-Session-Id capture/echo/DELETE.
	Legacy bool
	Client *http.Client
	Logger *slog.Logger
}

// TokenSource resolves credential headers per request. Invalidate is called
// after an authorization failure (HTTP 401) before the single retry, so
// implementations drop any cached token first.
type TokenSource interface {
	Headers(ctx context.Context) (map[string]string, error)
	Invalidate()
}

// Connect validates the config; HTTP needs no persistent socket.
func (b *HTTPBackend) Connect(ctx context.Context) (Conn, error) {
	if b.URL == "" {
		return nil, fmt.Errorf("backend: http url required")
	}
	log := b.Logger
	if log == nil {
		log = slog.Default()
	}
	client := b.Client
	if client == nil {
		// Timeout 0: streams are long-lived; per-request ctx bounds them.
		client = &http.Client{Timeout: 0, CheckRedirect: RefuseUnsafeRedirect}
	}
	c := &httpConn{
		backend: b,
		client:  client,
		log:     log,
		inbox:   make(chan *jsonrpc.Message, 64),
	}
	// The conn's life as a context, because every exchange has to be scoped
	// inside it and a channel cannot be a context's parent.
	c.connCtx, c.connCancel = context.WithCancel(context.Background())
	return c, nil
}

type httpConn struct {
	backend *HTTPBackend
	client  *http.Client
	log     *slog.Logger

	inbox chan *jsonrpc.Message

	mu        sync.Mutex
	sessionID string
	inflight  sync.WaitGroup
	// closing latches Close's arrival, so admitting an exchange (the Add on
	// inflight) and draining them (the Wait) cannot interleave. connCtx alone
	// cannot do this: a Write may pass a context check and reach its Add after
	// Wait has already started on a zero counter, which that Wait will not see.
	closing bool

	// annotations maps a tool name to its honored x-mcp-header parameters,
	// learned from tools/list responses passing through. A tool with no
	// annotations maps to nil, which is indistinguishable from "not learned
	// yet" — deliberately: both mean "send no Mcp-Param-* headers", and the
	// refresh path decides whether to retry by comparing what it knew before
	// against what it knows after, not by consulting a third state.
	annotations map[string][]headerParam
	// annotationsKnown records that a tools/list has been read to its LAST
	// page at least once. Without it a backend rejecting for a reason
	// mirroring cannot fix re-probes its whole tool list on every call,
	// forever. Only the out-of-band probe sets it, because only the probe
	// follows the cursor to the end: a page passing through on its way to a
	// client is one page, and what the pages after it hold is exactly the
	// question being answered.
	annotationsKnown bool
	// annotationsTruncated records that a probe hit the page cap with the
	// listing unfinished. Distinct from annotationsKnown: it says "we looked
	// and could not see it all", which is not grounds to believe there are no
	// annotations, but is grounds to stop re-reading the same pages — the
	// backend chooses when its cursor ends.
	annotationsTruncated bool
	// refreshGen counts completed out-of-band schema fetches, so concurrent
	// callers can tell whether one they waited on covered their need.
	refreshGen uint64

	// refreshMu serializes those fetches. Separate from mu, which is only
	// ever held briefly: this one is held across a network round trip.
	refreshMu sync.Mutex

	connCtx    context.Context
	connCancel context.CancelFunc
	closeOnce  sync.Once
}

func (c *httpConn) Read(ctx context.Context) (*jsonrpc.Message, error) {
	select {
	case m := <-c.inbox:
		return m, nil
	case <-c.connCtx.Done():
		// Drain anything already queued before reporting closed.
		select {
		case m := <-c.inbox:
			return m, nil
		default:
			return nil, ErrConnDead
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *httpConn) Write(ctx context.Context, msg *jsonrpc.Message) error {
	if c.connCtx.Err() != nil {
		return ErrConnDead
	}
	body, err := jsonrpc.Encode(msg)
	if err != nil {
		return err
	}

	// The response may be a long SSE stream: run the exchange in the
	// background so Write keeps the Conn contract (non-blocking beyond the
	// POST itself). The request is (re)built inside so the single 401 retry
	// gets a fresh body reader and freshly-resolved headers.
	//
	// Admission is latched against Close under c.mu, not against connCtx: a
	// WaitGroup counter raised after Wait has begun on a zero counter is not
	// seen by that Wait, so a Write that had already passed a bare context
	// check could start an exchange behind the drain Close just reported.
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return ErrConnDead
	}
	c.inflight.Add(1)
	c.mu.Unlock()
	exchange, done := c.exchangeContext(ctx, msg)
	go func() {
		defer c.inflight.Done()
		defer done()
		c.roundTripOnce(exchange, body, msg, false)
	}()
	return nil
}

// notificationExchangeTimeout bounds a notification's POST. Nothing is
// waiting for one — the mux sends notifications/cancelled after its caller is
// already gone, so inheriting that caller's context would abort the message
// on its way out — and a notification cannot stream, so a fixed bound costs it
// nothing.
const notificationExchangeTimeout = 30 * time.Second

// exchangeContext scopes one exchange, which outlives Write by design.
//
// A request's exchange lives exactly as long as the caller waiting for it. An
// SSE stream answering subscriptions/listen may legitimately run for hours,
// and the only thing that says whether it still should is whether anyone is
// still reading it; a wall-clock cap could not tell the two apart. A
// notification has no such caller and takes the fixed bound above instead.
//
// Either way the conn's own life is the ceiling. Without it a backend that
// accepts the POST and answers nothing holds a goroutine and a socket past
// every teardown the gateway has — the client has no Timeout, so eviction,
// recycling and Shutdown all returned while the exchange ran on.
func (c *httpConn) exchangeContext(ctx context.Context, msg *jsonrpc.Message) (context.Context, func()) {
	var out context.Context
	var cancel context.CancelFunc
	if msg.Kind() == jsonrpc.KindRequest {
		out, cancel = context.WithCancel(ctx)
	} else {
		out, cancel = context.WithTimeout(context.WithoutCancel(ctx), notificationExchangeTimeout)
	}
	stop := context.AfterFunc(c.connCtx, cancel)
	return out, func() { stop(); cancel() }
}

// newRequest builds one POST with the static headers, the token source's
// current headers, and the MCP framing headers.
func (c *httpConn) newRequest(ctx context.Context, body []byte, msg *jsonrpc.Message, name string, paramHeaders map[string]string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.backend.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	// Mirrored parameter headers go on BEFORE credentials below. They are
	// derived from caller-supplied arguments, and the credential headers are
	// the gateway's own; setting them first means an Mcp-Param-* can never
	// displace an injected secret even if the two ever collide by name.
	for k, v := range paramHeaders {
		req.Header.Set(k, v)
	}
	for k, v := range c.backend.Headers {
		req.Header.Set(k, v)
	}
	if ts := c.backend.TokenSource; ts != nil {
		hdrs, err := ts.Headers(ctx)
		if err != nil {
			return nil, fmt.Errorf("resolving credentials: %w", err)
		}
		for k, v := range hdrs {
			req.Header.Set(k, v)
		}
	}
	if msg.Kind() == jsonrpc.KindRequest {
		// Required by 2026-07-28; harmless extras for legacy servers.
		req.Header.Set(mcpspec.HeaderMethod, msg.Method)
		if name != "" {
			// Resource URIs are never header-safe by grammar, and tool names
			// are only SHOULD-constrained, so this may be sentinel-encoded.
			req.Header.Set(mcpspec.HeaderName, mcpspec.EncodeHeaderValue(name))
		}
		if c.backend.Legacy {
			req.Header.Set(mcpspec.HeaderProtocolVersion, mcpspec.LegacyProtocolVersion)
		} else {
			req.Header.Set(mcpspec.HeaderProtocolVersion, mcpspec.ProtocolVersion)
		}
	}
	c.mu.Lock()
	if c.sessionID != "" {
		req.Header.Set("Mcp-Session-Id", c.sessionID)
	}
	c.mu.Unlock()
	return req, nil
}

// doWithAuthRetry performs one POST, retrying once with freshly-resolved
// credentials on 401. Every request on this conn goes through here, the
// out-of-band annotation probe included — otherwise an expired token that any
// normal request would refresh would silently defeat the probe.
func (c *httpConn) doWithAuthRetry(ctx context.Context, body []byte, msg *jsonrpc.Message, name string, paramHeaders map[string]string) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		req, err := c.newRequest(ctx, body, msg, name, paramHeaders)
		if err != nil {
			return nil, err
		}
		resp, err := c.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("http request failed: %w", err)
		}
		// Safe for any method, idempotent or not: a 401 rejects the request
		// before the server executes it.
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 && c.backend.TokenSource != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			c.backend.TokenSource.Invalidate()
			continue
		}
		return resp, nil
	}
}

// roundTripOnce performs one HTTP exchange. retried marks the reissue after a
// header mismatch, so the recovery cannot recurse.
func (c *httpConn) roundTripOnce(ctx context.Context, body []byte, msg *jsonrpc.Message, retried bool) {
	// Parsed once and threaded through: params can be megabytes, and the name
	// is wanted by the Mcp-Name header, the mirrored parameter headers and the
	// recovery below. DecodeParams also walks the whole document to reject
	// ambiguous keys, so re-decoding per consumer would multiply the one cost
	// this comment exists to avoid.
	//
	// Both failures below send the request onward with no Mcp-Name, which a
	// conformant server answers with its own -32020. That is the right
	// direction to fail, but it is opaque from the far side, so say what
	// happened here — the same reason paramHeadersFor logs its skips.
	params, err := mcpspec.DecodeParams(msg.Params)
	if err != nil {
		// Unreachable from the wire — pkg/proxy.Check decoded the same bytes
		// before authorizing them — but this Conn is also driven directly by
		// tests and by the legacy bridge, so it must not invent a name.
		c.log.Warn("params could not be read; sending without Mcp-Name",
			"method", msg.Method, "err", err)
		params = mcpspec.Params{}
	}
	name, _, err := params.Name(msg.Method)
	if err != nil {
		c.log.Warn("params name could not be read; sending without Mcp-Name",
			"method", msg.Method, "err", err)
		name = ""
	}
	// Captured before the request goes out, so the recovery can compare what
	// this request actually carried against what it would carry now, and can
	// tell a refresh that postdates it from one that merely finished later.
	sent := c.paramHeadersFor(msg, params, name)
	c.mu.Lock()
	gen := c.refreshGen
	c.mu.Unlock()
	resp, err := c.doWithAuthRetry(ctx, body, msg, name, sent)
	if err != nil {
		// A failure the exchange's own context caused needs no answer: either
		// the caller stopped waiting, or the conn is closing and the mux is
		// already failing every call on it. Synthesizing one would log a
		// backend error for a teardown and deliver a response nobody can route.
		if ctx.Err() == nil {
			c.fail(msg, err.Error())
		}
		return
	}
	defer resp.Body.Close()

	if c.backend.Legacy && msg.Method == mcpspec.MethodInitialize {
		if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
			c.mu.Lock()
			c.sessionID = sid
			c.mu.Unlock()
		}
	}

	switch {
	case resp.StatusCode == http.StatusAccepted:
		return // notification/response accepted, no body
	case resp.StatusCode >= 300:
		// Generous enough for a JSON-RPC error whose data enumerates the
		// expected Mcp-Param-* headers — a 4KiB cap truncated those and lost
		// both the structured error and the retry that recognizes it — but far
		// below max_payload, because this body can end up inside an error
		// message that travels back over NATS.
		data, _ := io.ReadAll(io.LimitReader(resp.Body, errorBodyLimit))
		// A modern server answers version, capability and header-validation
		// failures with 4xx AND a JSON-RPC error body. Surfacing that body is
		// what lets a caller tell "you addressed me wrong" from "the backend
		// is broken" — and it is how a missing Mcp-Param-* is reported.
		//
		// Only for a REQUEST: a rejected notification has no id to answer on,
		// so delivering one would produce a response the mux cannot route and
		// drops at debug level. fail() logs it instead, which is the whole
		// visibility a backend rejecting our cancellations will ever get.
		if rpcErr, ok := decodeRPCError(data); ok && msg.Kind() == jsonrpc.KindRequest {
			if c.retryWithParamHeaders(ctx, rpcErr, body, msg, params, name, sent, gen, retried) {
				return
			}
			// Logged as well as delivered. The caller gets the RPC error, but a
			// backend that starts 503-ing every call must leave a trace on the
			// gateway too, and the HTTP status reaches nobody otherwise.
			c.log.Warn("backend rejected request", "method", msg.Method,
				"status", resp.StatusCode, "code", rpcErr.Error.Code,
				"detail", rpcErr.Error.Message)
			rpcErr.ID = msg.ID
			c.deliver(rpcErr)
			return
		}
		c.fail(msg, fmt.Sprintf("http %d: %s", resp.StatusCode, truncate(bytes.TrimSpace(data), 512)))
		return
	}

	ct := resp.Header.Get("Content-Type")
	switch {
	case strings.HasPrefix(ct, "application/json"):
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			if ctx.Err() == nil {
				c.fail(msg, fmt.Sprintf("reading response: %v", err))
			}
			return
		}
		c.deliverBytes(data, msg)
	case strings.HasPrefix(ct, "text/event-stream"):
		c.pumpSSE(ctx, resp.Body, msg)
	default:
		c.fail(msg, fmt.Sprintf("unexpected content-type %q", ct))
	}
}

// decodeRPCError reports whether body is a JSON-RPC error response.
func decodeRPCError(body []byte) (*jsonrpc.Message, bool) {
	m, err := jsonrpc.Decode(bytes.TrimSpace(body))
	if err != nil || m.Error == nil {
		return nil, false
	}
	return m, true
}

// retryWithParamHeaders implements the spec's recovery for a tools/call
// rejected because its Mcp-Param-* headers were missing or stale: re-read the
// tool's inputSchema from tools/list, then reissue the call once with the
// headers the annotations call for. Reporting whether it took over.
//
// This is why the gateway can stay schema-blind on the fast path — it learns
// a schema only when a backend tells it that it needed one.
func (c *httpConn) retryWithParamHeaders(ctx context.Context, rpcErr *jsonrpc.Message, body []byte, msg *jsonrpc.Message, params mcpspec.Params, name string, sent map[string]string, gen uint64, retried bool) bool {
	if retried || c.backend.Legacy || msg.Method != mcpspec.MethodToolsCall ||
		rpcErr.Error == nil || rpcErr.Error.Code != mcpspec.ErrHeaderMismatch {
		return false
	}
	// If this tool's schema was already read, a missing Mcp-Param-* cannot be
	// the cause — we would have sent them. Re-reading it would cost a full
	// paginated tools/list on every rejection for as long as the backend keeps
	// rejecting, and could not change the outcome.
	c.mu.Lock()
	_, toolKnown := c.annotations[name]
	known := c.annotationsKnown && (toolKnown || len(c.annotations) == 0)
	// A truncated probe counts as done for this purpose. It read every page
	// the cap allows, so another one reads the same ones and learns nothing
	// new — and the backend chooses when the cursor ends, so without this a
	// listing that never terminates makes each rejected call re-sweep the
	// whole toolset.
	truncated := c.annotationsTruncated
	c.mu.Unlock()
	if known || truncated {
		return false
	}
	if err := c.refreshAnnotations(ctx, gen); err != nil {
		c.log.Warn("could not re-read tool annotations after a header mismatch", "err", err)
		return false
	}
	// Compared against what THIS request actually sent, not against a snapshot
	// taken after it failed. On a cold conn a concurrent caller's refresh may
	// already have landed by then, and a before/after snapshot would show no
	// change even though this request went out with no headers at all.
	now := c.paramHeadersFor(msg, params, name)
	if maps.Equal(sent, now) {
		// The reissue would be byte-identical and earn the same rejection: the
		// mismatch was about something mirroring cannot fix.
		return false
	}
	c.log.Debug("retrying tools/call with mirrored parameter headers",
		"tool", name, "headers", len(now))
	c.roundTripOnce(ctx, body, msg, true)
	return true
}

// pumpSSE delivers each SSE data payload as a message.
func (c *httpConn) pumpSSE(ctx context.Context, body io.Reader, req *jsonrpc.Message) {
	err := scanSSE(body, func(data []byte) bool {
		c.deliverBytes(data, req)
		return true
	})
	if err != nil && ctx.Err() == nil {
		// An oversize or unreadable event truncates the payload, which then
		// fails to decode and is dropped. Without this the request simply
		// never gets an answer and the caller waits out its whole context —
		// a payload-too-large reported as a timeout.
		c.fail(req, fmt.Sprintf("reading response stream: %v", err))
	}
}

// scanSSE calls fn with each SSE data payload until fn returns false, and
// reports any error that ended the scan early.
func scanSSE(body io.Reader, fn func([]byte) bool) error {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 64*1024), maxLineBytes)
	var data bytes.Buffer
	flush := func() bool {
		if data.Len() == 0 {
			return true
		}
		payload := append([]byte(nil), data.Bytes()...)
		data.Reset()
		return fn(payload)
	}
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if !flush() {
				return nil
			}
		case strings.HasPrefix(line, "data:"):
			// The SSE spec joins an event's data lines with a newline. Dropping
			// the separator silently welds two lines together, which JSON usually
			// tolerates and a payload with a meaningful newline does not.
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		default:
			// id:/event:/retry:/comments — 2026-07-28 removed resumability,
			// so ids carry nothing we need.
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	flush()
	return nil
}

// deliverBytes decodes one message and hands it on. req is the request this
// response belongs to, or nil; it is what lets a tools/list result be
// recognized without correlating ids.
func (c *httpConn) deliverBytes(data []byte, req *jsonrpc.Message) {
	m, err := jsonrpc.Decode(data)
	if err != nil {
		c.log.Warn("dropping undecodable http message", "err", err)
		return
	}
	// Legacy backends are excluded: x-mcp-header does not exist in 2025-11-25,
	// so judging a legacy server's schemas against SEP-2243 — and deleting the
	// tools that fail — would enforce a rule its author never agreed to.
	if req != nil && !c.backend.Legacy &&
		req.Method == mcpspec.MethodToolsList && m.Kind() == jsonrpc.KindResponse {
		c.absorbToolsList(m)
	}
	c.deliver(m)
}

func (c *httpConn) deliver(m *jsonrpc.Message) {
	select {
	case c.inbox <- m:
	case <-c.connCtx.Done():
	}
}

// fail synthesizes an error response for a request whose HTTP exchange
// failed, so the mux's caller gets an answer instead of a timeout.
func (c *httpConn) fail(msg *jsonrpc.Message, detail string) {
	// The operator gets the detail intact; the caller gets it with any URL
	// stripped of what a URL can carry. net/http quotes the request URL into
	// every transport error, the proxy forwards a backend's JSON-RPC error
	// verbatim, and a backend URL is a documented place to expand a secret
	// into — "http://host/mcp?api_key=${KEY}" reaches the caller in full on
	// nothing more than a connection refused. Go redacts userinfo passwords
	// and nothing else.
	c.log.Warn("http backend error", "method", msg.Method, "detail", detail)
	if msg.Kind() != jsonrpc.KindRequest {
		return
	}
	c.deliver(jsonrpc.NewErrorResponse(msg.ID, jsonrpc.CodeInternalError, scrubURLs(detail), nil))
}

// urlInText matches a URL embedded in free-form error text.
var urlInText = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^\s"'` + "`" + `]+`)

// scrubURLs removes the credential-bearing parts of every URL in s, keeping
// enough for the failure to stay diagnosable: the scheme, host and path say
// which backend could not be reached, which is the whole content of a
// transport error. Userinfo and query string are what a secret is expanded
// into, and neither tells the caller anything it is owed.
//
// The path is kept, which bounds what this can promise: a deployment that puts
// its secret in the path rather than the query (a webhook-shaped URL), or an
// error quoting a file:// URI, still passes that through. Keeping the path is
// the deliberate half of the trade — "which backend" is unreadable without it —
// so a secret belongs in a header or the query string, not in the path.
func scrubURLs(s string) string {
	return urlInText.ReplaceAllStringFunc(s, func(raw string) string {
		// Trailing punctuation belongs to the sentence, not the URL.
		trailer := ""
		for len(raw) > 0 && strings.ContainsRune(`.,;:)]}"'`, rune(raw[len(raw)-1])) {
			trailer, raw = raw[len(raw)-1:]+trailer, raw[:len(raw)-1]
		}
		u, err := url.Parse(raw)
		if err != nil {
			// Unparseable is exactly when a secret is most likely to be in
			// there, so keep only up to the authority.
			if i := strings.Index(raw, "://"); i >= 0 {
				if j := strings.IndexAny(raw[i+3:], "/?#"); j >= 0 {
					return raw[:i+3+j] + "/[redacted]" + trailer
				}
			}
			return "[redacted url]" + trailer
		}
		u.User = nil
		if u.RawQuery != "" {
			u.RawQuery = "[redacted]"
		}
		u.Fragment = ""
		return u.String() + trailer
	})
}

func (c *httpConn) Close() error {
	c.closeOnce.Do(func() {
		// Shuts the door before anything else, so no exchange can be admitted
		// behind the drain below, and ends the ones already through it: the
		// DELETE that follows is a courtesy call, and a request wedged against
		// this backend must not be what decides whether it goes out.
		c.mu.Lock()
		c.closing = true
		sid := c.sessionID
		c.mu.Unlock()
		c.connCancel()
		if c.backend.Legacy && sid != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.backend.URL, nil)
			if err == nil {
				req.Header.Set("Mcp-Session-Id", sid)
				for k, v := range c.backend.Headers {
					req.Header.Set(k, v)
				}
				if ts := c.backend.TokenSource; ts != nil {
					// Best-effort: the DELETE is a courtesy; an unresolvable
					// token must not block teardown.
					if hdrs, err := ts.Headers(ctx); err == nil {
						for k, v := range hdrs {
							req.Header.Set(k, v)
						}
					}
				}
				if resp, err := c.client.Do(req); err == nil {
					resp.Body.Close() // 405 is a legal "we don't support DELETE"
				}
			}
		}

		// Cancelled is not the same as finished: an exchange still inside
		// io.ReadAll holds its socket until it returns. Close is what the pool
		// calls to reclaim a backend, so it must not report one reclaimed
		// while its requests are still on the wire — the same grace the stdio
		// backend gives a subprocess to die.
		if !c.awaitInflight(terminateGrace) {
			c.log.Warn("http backend closed with exchanges still in flight",
				"grace", terminateGrace)
		}
	})
	return nil
}

// awaitInflight reports whether every exchange finished within d. On timeout
// the waiter outlives this call, which is bounded rather than leaked: the
// exchanges it waits on all run under connCtx, already cancelled by Close, and
// cancelling a request aborts the body read holding its socket.
func (c *httpConn) awaitInflight(d time.Duration) bool {
	drained := make(chan struct{})
	go func() {
		c.inflight.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return true
	case <-time.After(d):
		return false
	}
}

// annotationProbeID is the JSON-RPC id of the out-of-band tools/list the
// backend issues to (re)learn x-mcp-header annotations. It never reaches the
// inbox, so it cannot collide with a muxed caller id.
const annotationProbeID = "natsmcp-annotations"

// errorBodyLimit bounds a 4xx/5xx body read. Big enough for a JSON-RPC error
// whose data enumerates expected headers — the old 4KiB cap truncated those
// and lost the retry that recognizes them — and small enough that a backend
// outage does not have every failing call pull a megabyte of HTML off the
// wire only to discard it.
const errorBodyLimit = 64 << 10

// schemaAnnotationBytes is the cheap presence test that keeps the schema walk
// off the delivery path of servers that publish no annotations.
var schemaAnnotationBytes = []byte(mcpspec.SchemaHeaderAnnotation)

const (
	// annotationProbeTimeout bounds one out-of-band schema fetch.
	annotationProbeTimeout = 30 * time.Second
	// annotationProbeMaxPages bounds cursor following on a paginated
	// tools/list, so a server with a runaway cursor cannot loop us forever.
	annotationProbeMaxPages = 50
)

// absorbToolsList learns each tool's x-mcp-header annotations and removes any
// tool whose annotations are invalid. Exclusion is what the spec asks for:
// one malformed definition must not cost a server its whole toolset, and
// silently keeping it would leave the client unable to call it successfully.
func (c *httpConn) absorbToolsList(m *jsonrpc.Message) {
	if m.Error != nil || len(m.Result) == 0 {
		return
	}
	// A listing with the annotation nowhere in it needs no parsing at all —
	// which is every server that does not use the feature, i.e. all of them
	// today.
	if !bytes.Contains(m.Result, schemaAnnotationBytes) {
		return
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(m.Result, &result) != nil {
		return
	}
	var tools []json.RawMessage
	if raw, ok := result["tools"]; !ok || json.Unmarshal(raw, &tools) != nil {
		return
	}

	learned := map[string][]headerParam{}
	var invalid []string
	kept := make([]json.RawMessage, 0, len(tools))
	for _, t := range tools {
		var def struct {
			Name        string          `json:"name"`
			InputSchema json.RawMessage `json:"inputSchema"`
		}
		if json.Unmarshal(t, &def) != nil || def.Name == "" {
			kept = append(kept, t) // not ours to judge
			continue
		}
		params, err := parseToolAnnotations(def.InputSchema)
		if err != nil {
			c.log.Warn("excluding tool with an invalid x-mcp-header annotation",
				"tool", def.Name, "reason", err.Error())
			invalid = append(invalid, def.Name)
			continue
		}
		learned[def.Name] = params
		kept = append(kept, t)
	}

	c.mu.Lock()
	if c.annotations == nil {
		c.annotations = make(map[string][]headerParam, len(learned))
	}
	// Merged, not replaced: tools/list is paginated, so one response is not
	// necessarily the whole toolset.
	maps.Copy(c.annotations, learned)
	for _, name := range invalid {
		delete(c.annotations, name)
	}
	c.mu.Unlock()

	if len(invalid) == 0 {
		return
	}
	// Re-encoded WITHOUT HTML escaping. Dropping a tool means rebuilding the
	// array, but json.Marshal would rewrite every < > and & inside every
	// remaining description and schema — the same payload rewriting the legacy
	// bridge splices to avoid (pkg/backend/legacy: spliceFields).
	if encoded, err := marshalNoEscape(kept); err == nil {
		result["tools"] = encoded
		if out, err := marshalNoEscape(result); err == nil {
			m.Result = out
		}
	}
}

// marshalNoEscape encodes v with HTML escaping off, so <, > and & inside
// strings survive as themselves.
func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// refreshAnnotations re-reads tool schemas out of band. The response is
// consumed here and never delivered — no caller asked for it.
//
// One conn is shared by many concurrent callers, and a cold conn can have
// dozens of tools/call rejected at once. Without collapsing them, each would
// POST its own tools/list. Callers that arrive while a refresh is running
// wait for it and then use its result: the generation counter tells them
// whether the work they were waiting on is the work they needed.
func (c *httpConn) refreshAnnotations(ctx context.Context, gen uint64) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	c.mu.Lock()
	current := c.refreshGen
	c.mu.Unlock()
	if current != gen {
		// A refresh that began after our request went out has since completed,
		// so it saw at least as much as one starting now would. gen is sampled
		// before the request for exactly this reason: a refresh that STARTED
		// earlier and merely finished later proves nothing about what we need.
		return nil
	}

	// The probe needs its own deadline on top of the exchange's. The conn's
	// http.Client has no timeout (response streams are long-lived by design),
	// and the caller whose context this inherits may have none either, so a
	// backend that accepts this POST and never answers would hold refreshMu
	// for as long as that caller waits — and every later recovery attempt
	// behind it.
	ctx, cancel := context.WithTimeout(ctx, annotationProbeTimeout)
	defer cancel()

	cursor := ""
	for page := range annotationProbeMaxPages {
		next, err := c.probeToolsPage(ctx, cursor)
		if err != nil {
			return err
		}
		cursor = next
		if cursor == "" {
			break
		}
		if page == annotationProbeMaxPages-1 {
			// Never truncate silently: an unread page is a tool whose
			// annotations we will keep failing to find.
			c.log.Warn("stopped paging tools/list while learning annotations",
				"pages", annotationProbeMaxPages)
		}
	}

	// Bumped only on SUCCESS. A caller queued behind a failed refresh must go
	// on to run its own — crediting it with work that did not happen would
	// have it skip the retry and fail a call whose fix was available.
	c.mu.Lock()
	// "This server publishes no annotations" follows from "no annotation was
	// seen" only for a run that reached the end of the cursor. One stopped by
	// the page cap has tools it never looked at, and must stay willing to look
	// again.
	if cursor == "" {
		c.annotationsKnown = true
	} else {
		// Read as far as the cap allows and the listing still had more. We
		// cannot conclude "this server publishes no annotations" from that —
		// the tools we never saw may carry some — but we also must not keep
		// paying for the attempt: the next probe reads the same pages and
		// stops in the same place, so a backend that never terminates its
		// cursor would turn every -32020 into another full sweep. Recording
		// the truncation is what makes the recovery give up on a retry that
		// cannot succeed, without claiming knowledge it does not have.
		c.annotationsTruncated = true
	}
	c.refreshGen++
	c.mu.Unlock()
	return nil
}

// probeToolsPage fetches one page of tools/list out of band and absorbs its
// annotations, returning the next page's cursor ("" when done). The response
// is consumed here and never delivered — no caller asked for it.
func (c *httpConn) probeToolsPage(ctx context.Context, cursor string) (string, error) {
	params := json.RawMessage(`{}`)
	if cursor != "" {
		encoded, err := json.Marshal(map[string]string{"cursor": cursor})
		if err != nil {
			return "", err
		}
		params = encoded
	}
	probe := jsonrpc.NewRequest(annotationProbeID, mcpspec.MethodToolsList, params)
	body, err := jsonrpc.Encode(probe)
	if err != nil {
		return "", err
	}
	resp, err := c.doWithAuthRetry(ctx, body, probe, "", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("http %d", resp.StatusCode)
	}

	var data []byte
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		if err := scanSSE(resp.Body, func(payload []byte) bool {
			if m, err := jsonrpc.Decode(payload); err == nil && m.Kind() == jsonrpc.KindResponse {
				data = payload
				return false // the response ends the stream; stop reading
			}
			return true
		}); err != nil {
			return "", err
		}
	} else {
		data, err = io.ReadAll(io.LimitReader(resp.Body, maxLineBytes))
		if err != nil {
			return "", err
		}
	}
	m, err := jsonrpc.Decode(data)
	if err != nil {
		return "", err
	}
	if m.Error != nil {
		return "", fmt.Errorf("tools/list rejected: %s", m.Error)
	}
	c.absorbToolsList(m)

	var page struct {
		NextCursor string `json:"nextCursor"`
	}
	_ = json.Unmarshal(m.Result, &page)
	return page.NextCursor, nil
}

// paramHeadersFor renders the Mcp-Param-* headers for one request, if it is a
// tools/call on a tool whose annotations we have already learned.
func (c *httpConn) paramHeadersFor(msg *jsonrpc.Message, params mcpspec.Params, name string) map[string]string {
	if msg.Kind() != jsonrpc.KindRequest || msg.Method != mcpspec.MethodToolsCall {
		return nil
	}
	// Nothing learned yet means nothing to mirror, and on a backend that
	// publishes no annotations that is every call.
	c.mu.Lock()
	known := len(c.annotations)
	c.mu.Unlock()
	if known == 0 {
		return nil
	}
	annots := c.annotationsFor(name)
	if len(annots) == 0 {
		return nil
	}
	// Raw, not a plain map index: reading "arguments" is what obliges the
	// gateway to refuse an "ARGUMENTS" sibling that a case-folding backend
	// would bind instead. In practice DecodeParams already refused it — every
	// top-level key is checked there — so this is the rule stated where it is
	// relied on rather than a second line of defense.
	args, ok, err := params.Raw("arguments")
	if err != nil {
		c.log.Warn("tool arguments cannot be read; mirroring no headers",
			"tool", name, "err", err)
		return nil
	}
	if !ok {
		return nil
	}
	headers, skipped := paramHeaders(annots, args)
	if len(skipped) > 0 {
		// The backend will reject the call for the missing header and the
		// recovery cannot help — the schema is already known. Say so, or the
		// only trace is a -32020 with no explanation on this side.
		c.log.Warn("tool argument cannot be mirrored into a header",
			"tool", name, "params", skipped)
	}
	return headers
}

func (c *httpConn) annotationsFor(tool string) []headerParam {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.annotations[tool]
}
