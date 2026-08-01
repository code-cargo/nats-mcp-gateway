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

package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

const (
	// DefaultKeepAlive is the ka-frame interval on idle streams. Must be
	// well under the client's inactivity deadline.
	DefaultKeepAlive = 15 * time.Second

	// refusalLogInterval is the floor between two reply-subject refusal logs.
	refusalLogInterval = time.Second
)

var (
	errClientCancelled = errors.New("wire: cancelled by client")
	errDraining        = errors.New("wire: gateway draining")
)

// Inbound is one request as seen by the gateway handler: the NATS-enforced
// subject facts, the raw headers, and the decoded JSON-RPC envelope (params
// still opaque bytes).
type Inbound struct {
	Subject ParsedSubject
	Header  nats.Header
	Body    []byte
	Msg     *jsonrpc.Message
}

// StreamWriter emits reply frames for one request. Msg may be called many
// times; exactly one of End or Err terminates the stream — later terminal
// calls are ignored, so cleanup paths can call End/Err unconditionally.
type StreamWriter interface {
	// Msg publishes one JSON-RPC notification frame. Oversize bodies are
	// dropped (notifications are best-effort) and reported as an error.
	Msg(body []byte) error
	// End publishes the JSON-RPC response, or an empty-body end frame when
	// body is nil (cancelled: no response exists). An oversize body is
	// converted into an ErrCodePayloadTooLarge error frame.
	End(body []byte) error
	// Err publishes a gateway-synthesized JSON-RPC error response echoing
	// the request id.
	Err(code int, message string, data any) error
}

// Handler processes one request. ctx is cancelled on client cancellation or
// gateway drain (distinguish via context.Cause). If the handler returns
// without writing a terminal frame, the server writes one.
type Handler func(ctx context.Context, in *Inbound, w StreamWriter) error

// ServerConfig configures the gateway side of the wire.
type ServerConfig struct {
	// Prefix is the subject prefix (DefaultPrefix if empty).
	Prefix string
	// QueueGroup for load-balancing gateway replicas (default "mcpgw").
	QueueGroup string
	// Tenant/User scope every endpoint to a slice of the subject space:
	// Tenant alone binds {prefix}.req.{tenant}.*.{server}.> (an org
	// deployment serving all of one tenant's users); Tenant+User binds
	// {prefix}.req.{tenant}.{user}.{server}.> (a per-user pod). Both empty is
	// the all-callers central form; User without Tenant is invalid. A scoped
	// instance must not share a queue group with an unscoped gateway serving
	// the same server names, or the two would compete for the scoped traffic.
	Tenant string
	User   string
	// Servers is the list of MCP server names to register endpoints for.
	Servers []string
	// Name/Version identify the micro service ($SRV.INFO).
	Name    string
	Version string
	// KeepAlive overrides DefaultKeepAlive when > 0.
	KeepAlive time.Duration
	// Logger receives the wire's own operational messages: a refused reply
	// subject, and a control subscription the gateway could not establish —
	// conditions that degrade a request without failing it, which is exactly
	// why they need somewhere to go. slog.Default() when nil.
	Logger *slog.Logger
	// Claims, when set, parks oversize responses in a claim store instead of
	// failing them with ErrCodePayloadTooLarge — but only for callers that
	// sent HeaderAcceptClaim. Nil disables claim-check (today's behavior).
	Claims ClaimStore
}

// Server is a running wire endpoint fleet member. It runs one micro service
// per fronted MCP server, so the set can be reconciled at runtime
// (SetServers) — nats.go exposes Service.Stop but not per-endpoint removal,
// so per-server services are how a server is dropped without a restart.
type Server struct {
	nc         *nats.Conn
	handler    Handler
	prefix     string
	queueGroup string
	tenant     string
	user       string
	name       string
	version    string
	keepAlive  time.Duration
	claims     ClaimStore
	log        *slog.Logger

	// A refused reply subject is caller-triggered, so the log it produces is
	// caller-controlled too: one publisher can emit them as fast as it can
	// publish. Warn at most once per refusalLogInterval and carry the
	// suppressed count into the next line, so the refusal is never invisible
	// and never the thing that fills the disk.
	refuseMu          sync.Mutex
	refusedAt         time.Time
	refusedSuppressed int

	svcMu sync.Mutex
	svcs  map[string]micro.Service // server name -> its micro service

	base   context.Context
	cancel context.CancelCauseFunc

	// Requests keep arriving after stopAllServices returns: micro stops an
	// endpoint with Subscription.Drain, which only buffers the UNSUB. drainMu
	// fuses "has the drain given up on new work?" and "reserve a slot in wg"
	// into one decision, so once Shutdown starts waiting the counter can only
	// fall. Without it an Add can land on a zero counter concurrently with
	// Wait — the misuse sync.WaitGroup documents, and the exact window in
	// which Shutdown reports a clean drain while a handler is still running.
	drainMu  sync.Mutex
	draining bool
	wg       sync.WaitGroup
}

// DefaultQueueGroup is the group a gateway joins when none is configured.
//
// A scoped instance defaults to its OWN group: NATS dedupes queue subscribers
// by group name across different subject patterns, so sharing "mcpgw" with an
// unscoped fleet serving the same server names would make the two compete for
// the scoped traffic. The group mirrors the scope — mcpgw.{tenant} for an org
// deployment, mcpgw.{tenant}.{user} for a per-user pod.
//
// This default lives here rather than in the CLI so every config source gets
// it, which also means a caller wanting to know what a gateway WILL join has
// to ask here rather than reading a flag.
func DefaultQueueGroup(tenant, user string) string {
	switch {
	case tenant != "" && user != "":
		return "mcpgw." + tenant + "." + user
	case tenant != "":
		return "mcpgw." + tenant
	}
	return "mcpgw"
}

// Serve starts the wire server with the given initial server set. Each
// request runs in its own goroutine; per-backend concurrency limits belong to
// the backend pool, not the wire.
func Serve(nc *nats.Conn, cfg ServerConfig, handler Handler) (*Server, error) {
	if cfg.QueueGroup == "" {
		cfg.QueueGroup = DefaultQueueGroup(cfg.Tenant, cfg.User)
	}
	if cfg.Name == "" {
		cfg.Name = "natsmcp-gateway"
	}
	if cfg.Version == "" {
		cfg.Version = "0.0.1"
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = DefaultKeepAlive
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = DefaultPrefix
	} else if err := ValidateSubjectPrefix(prefix); err != nil {
		// The same check NewClient makes, and for a sharper reason on this
		// side: EndpointSubject pastes the prefix in unchecked, so an empty
		// token or a wildcard builds a SUBSCRIBE that is either invalid — the
		// server's -ERR 'Invalid Subject' is unrecognised by nats.go, which
		// closes the connection for good — or wider than the grant the prefix
		// exists to narrow. Every config source reaches Serve, so this is the
		// one place none of them can forget.
		return nil, fmt.Errorf("wire: %w", err)
	}

	if cfg.Tenant == "" && cfg.User != "" {
		return nil, fmt.Errorf("wire: endpoint scoping with a User requires a Tenant (got tenant=%q, user=%q)", cfg.Tenant, cfg.User)
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	base, cancel := context.WithCancelCause(context.Background())
	s := &Server{
		nc:         nc,
		handler:    handler,
		prefix:     prefix,
		log:        log,
		queueGroup: cfg.QueueGroup,
		tenant:     cfg.Tenant,
		user:       cfg.User,
		name:       cfg.Name,
		version:    cfg.Version,
		keepAlive:  cfg.KeepAlive,
		claims:     cfg.Claims,
		svcs:       make(map[string]micro.Service),
		base:       base,
		cancel:     cancel,
	}

	if err := s.SetServers(cfg.Servers); err != nil {
		_ = s.stopAllServices()
		cancel(nil)
		return nil, err
	}
	return s, nil
}

// SetServers reconciles the running micro services to exactly names: it stops
// services for servers no longer present and starts one for each new server.
// Existing services (unchanged servers) are left untouched, so their in-flight
// requests are undisturbed. It is safe to call repeatedly while serving.
//
// A failure rolls back to the set it found, so the caller's "keep the previous
// config live" (reconcile.Apply) is true of the wire too. That is why the adds
// come FIRST — the servers this call would drop are still owned by the config
// that stays live if an add fails, and a caller that had stopped them would be
// advertising a config it no longer answers for. The rollback is best-effort in
// the same way removal is: a service that will not stop is kept and reported,
// never forgotten (see stopLocked).
//
// Both sets are briefly bound while the adds run. That is safe because a
// service's subject carries its own {server} token, so no two of them overlap;
// and across a fleet the queue group makes each request reach exactly one
// gateway, however many of them are mid-reconcile.
//
// A stopped service stops answering its subject, so a subsequent request to a
// removed server hits NATS no-responders and the client sees ErrCodeNoGateway.
// Removal does NOT cancel in-flight handlers — only Shutdown does.
func (s *Server) SetServers(names []string) error {
	desired := make(map[string]struct{}, len(names))
	for _, n := range names {
		desired[n] = struct{}{}
	}

	s.svcMu.Lock()
	defer s.svcMu.Unlock()

	added := make([]string, 0, len(names))
	for _, name := range names {
		if _, ok := s.svcs[name]; ok {
			continue
		}
		subject, err := EndpointSubject(s.prefix, s.tenant, s.user, name)
		if err != nil {
			return errors.Join(err, s.stopLocked(added))
		}
		svc, err := micro.AddService(s.nc, micro.Config{
			Name:    s.name + "-" + name,
			Version: s.version,
			Endpoint: &micro.EndpointConfig{
				Subject:    subject,
				Handler:    micro.HandlerFunc(func(req micro.Request) { s.dispatch(s.prefix, req, s.handler) }),
				QueueGroup: s.queueGroup,
			},
		})
		if err != nil {
			return errors.Join(fmt.Errorf("wire: add service %q: %w", name, err), s.stopLocked(added))
		}
		s.svcs[name] = svc
		added = append(added, name)
	}

	var stale []string
	for name := range s.svcs {
		if _, ok := desired[name]; !ok {
			stale = append(stale, name)
		}
	}
	// Logged, not returned: the desired set is bound by here, so failing on a
	// removal would make the caller roll back an apply whose new servers are
	// already serving. stopLocked keeps what it could not stop, so the entry
	// stays in s.svcs and a later SetServers tries it again.
	//
	// "Later" is doing real work in that sentence: reconcile.Apply short-
	// circuits an empty diff without calling here at all, so the next attempt
	// comes with the next actual config change, which may be never. Until then
	// the service keeps answering for a server the live config does not list —
	// which is what the operator needs to be told, since nothing else will.
	if err := s.stopLocked(stale); err != nil {
		s.log.Warn("a removed server's service could not be stopped and is still bound; "+
			"it will be retried on the next config change",
			"err", err)
	}
	return nil
}

// stopLocked stops the named services and forgets the ones that stopped.
// svcMu must be held.
//
// A service whose Stop fails is KEPT. Stop is what unsubscribes it, so on
// failure it may still be answering, and dropping the handle would leave a
// subscription bound to a name nothing tracks — unreachable by any later
// reconcile, which is a worse end state than an entry that is stopped again
// next time round. The joined error is what tells the caller its rollback was
// partial; nothing here can make it whole.
func (s *Server) stopLocked(names []string) error {
	var errs []error
	for _, name := range names {
		svc, ok := s.svcs[name]
		if !ok {
			continue
		}
		if err := svc.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("stop service %q: %w", name, err))
			continue
		}
		delete(s.svcs, name)
	}
	return errors.Join(errs...)
}

// stopAllServices stops every running micro service.
func (s *Server) stopAllServices() error {
	s.svcMu.Lock()
	defer s.svcMu.Unlock()
	var errs []error
	for name, svc := range s.svcs {
		if err := svc.Stop(); err != nil {
			errs = append(errs, err)
		}
		delete(s.svcs, name)
	}
	return errors.Join(errs...)
}

// Shutdown drains: stop accepting requests on every server, terminate every
// live stream with ErrCodeStreamLost so clients re-issue immediately, then
// wait for handlers up to ctx's deadline. A request micro hands over after
// that wait is armed is refused with the same ErrCodeStreamLost — silence
// would cost its caller the full inactivity window.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.stopAllServices()
	s.drainMu.Lock()
	s.draining = true
	s.drainMu.Unlock()
	s.cancel(errDraining)
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return err
	case <-ctx.Done():
		return errors.Join(err, fmt.Errorf("wire: drain timed out: %w", ctx.Err()))
	}
}

// dispatch runs one request. It owns envelope decoding, the control-subject
// subscription, the keepalive ticker, and the exactly-one-terminal guarantee.
func (s *Server) dispatch(prefix string, req micro.Request, handler Handler) {
	if !s.beginRequest() {
		s.refuseDrained(req)
		return
	}
	go func() {
		defer s.wg.Done()

		// Every frame below goes to the reply-to the CALLER chose, published
		// under the GATEWAY's identity. See usableReplySubject for what that
		// hands a caller and why this refuses some of it.
		if reply := req.Reply(); !usableReplySubject(reply, prefix) {
			s.warnUnusableReply(req.Subject(), reply)
			return
		}

		w := &streamWriter{nc: s.nc, req: req}

		parsed, err := ParseSubject(req.Subject(), prefix)
		if err != nil {
			_ = w.errWithID(nil, jsonrpc.CodeInvalidRequest, err.Error(), nil)
			return
		}
		w.claims = s.claims
		w.tenant = parsed.Tenant
		w.acceptClaim = req.Headers().Get(HeaderAcceptClaim) == "1"
		if v := req.Headers().Get(HeaderWire); v != WireVersion {
			_ = w.errWithID(nil, jsonrpc.CodeInvalidRequest,
				fmt.Sprintf("unsupported wire version %q (want %s)", v, WireVersion), nil)
			return
		}
		msg, err := jsonrpc.Decode(req.Data())
		if err != nil {
			_ = w.errWithID(nil, jsonrpc.CodeParseError, err.Error(), nil)
			return
		}
		w.id = msg.ID

		in := &Inbound{
			Subject: *parsed,
			Header:  nats.Header(req.Headers()),
			Body:    req.Data(),
			Msg:     msg,
		}

		ctx, cancel := context.WithCancelCause(s.base)
		defer cancel(nil)

		// Control subject: a notifications/cancelled naming THIS request
		// cancels it. The body is checked rather than assumed, because
		// {reply}.ctl is an ordinary subject under the caller's inbox — a
		// stray publish, a probe, or a cancellation meant for another request
		// must not end a live call.
		ctlSubject := req.Reply() + ctlSuffix
		ctlSub, err := s.nc.Subscribe(ctlSubject, func(m *nats.Msg) {
			if !isCancellation(m.Data, msg.ID) {
				return
			}
			cancel(errClientCancelled)
		})
		if err != nil {
			// The request still runs — it just cannot be cancelled any more,
			// and the keepalives would hide that for the whole stream, so the
			// operator has to hear about it. A SUBSCRIBE the NATS server
			// refuses does not land here (that arrives asynchronously on the
			// connection); this is the local failures — closed connection,
			// unusable subject.
			s.log.Error("control subscription failed, this request cannot be cancelled",
				"subject", ctlSubject, "err", err)
		} else {
			defer func() { _ = ctlSub.Unsubscribe() }()
		}

		// Keepalives until terminal.
		kaDone := make(chan struct{})
		defer close(kaDone)
		go s.keepAliveLoop(kaDone, w)

		herr := handler(ctx, in, w)

		// Guarantee a terminal frame whatever the handler did.
		switch {
		case w.terminated():
			return
		case errors.Is(context.Cause(ctx), errClientCancelled):
			_ = w.End(nil)
		case errors.Is(context.Cause(ctx), errDraining):
			_ = w.Err(ErrCodeStreamLost, "gateway draining, re-issue the request", nil)
		case herr != nil:
			_ = w.Err(jsonrpc.CodeInternalError, herr.Error(), nil)
		default:
			_ = w.Err(jsonrpc.CodeInternalError, "handler returned no response", nil)
		}
	}()
}

// isCancellation reports whether a control-subject message is a
// notifications/cancelled naming the request with the given raw JSON-RPC id.
//
// Ids compare by DECODED VALUE, which is deliberately NOT what jsonrpc.IDKey
// does. IDKey compares raw bytes, and everywhere else in this repo that is
// right, because both sides of the comparison came out of the same decode.
// Here they did not: a native client's request body is forwarded verbatim
// while the cancellation naming it is re-encoded, and encoding/json escapes
// <, > and & — so the same id arrives spelled two ways and IDKey calls them
// different requests.
//
//	jsonrpc.IDKey:  "a<b" vs "a\u003cb" -> different
//	isCancellation: "a<b" vs "a\u003cb" -> the same request
//
// A string "1" and a number 1 are still different requests, and so are 1 and
// 1.0: sameRequestID keeps a number's literal via UseNumber, because the
// client chose the spelling and it is the client's to keep.
func isCancellation(data, id []byte) bool {
	if len(id) == 0 {
		return false
	}
	// Cheap reject ahead of the parsing. This runs on the NATS delivery
	// goroutine over whatever a caller chose to publish to its own inbox, so
	// the common non-cancellation — a stray publish, a probe, a flood — is
	// turned away on a substring scan instead of two JSON decodes. A false
	// positive costs only the work the checks below would have done anyway.
	if !bytes.Contains(data, []byte(mcpspec.NotifCancelled)) {
		return false
	}
	m, err := jsonrpc.Decode(data)
	if err != nil || m.Kind() != jsonrpc.KindNotification || m.Method != mcpspec.NotifCancelled {
		return false
	}
	var p struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if json.Unmarshal(m.Params, &p) != nil || len(p.RequestID) == 0 {
		return false
	}
	return sameRequestID(p.RequestID, id)
}

// sameRequestID reports whether two JSON-RPC ids denote the same request.
//
// Byte equality is not enough, because the two ids reaching this point did not
// come out of the same encoder. A native client's request body is forwarded
// VERBATIM, so its id is whatever that client wrote; the cancellation naming
// it is re-encoded by Go somewhere along the way, and encoding/json escapes
// <, > and & to \u003c, \u003e and \u0026 by default. An id carrying any of
// them — a URL, an HTML fragment, anything a client derived from user text —
// therefore arrives spelled two ways, and comparing the bytes drops the
// cancellation. The request then runs to completion after the client asked it
// to stop, which is the failure the ctl guard exists to prevent, reached from
// the other side.
//
// Comparing the decoded values also settles 1 vs 1.0 vs 1e0 the way a reader
// expects, since json.Number keeps the literal and two spellings of one number
// are not the same id under JSON-RPC's "the client chose this string" rule.
func sameRequestID(a, b []byte) bool {
	// Decoded first, with no byte-equality shortcut ahead of it. A shortcut
	// would answer before decodeID could refuse a value that is not an id at
	// all, so `null` would name the request whose id is `null` — and
	// jsonrpc.HasID counts a literal null as present, which is how such a
	// request exists on the wire in the first place. That is the one id an
	// attacker never has to guess.
	da, err := decodeID(a)
	if err != nil {
		return false
	}
	db, err := decodeID(b)
	if err != nil {
		return false
	}
	return da == db
}

func decodeID(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // a number's literal is its identity; float64 would round
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	switch v.(type) {
	case string, json.Number:
		return v, nil
	}
	// JSON-RPC 2.0 allows only a string or a number as an id; anything else
	// cannot name a request and must not match one.
	return nil, errNotAnID
}

var errNotAnID = errors.New("wire: id is neither a string nor a number")

// beginRequest reserves the request's slot in the drain WaitGroup, reporting
// false once Shutdown has stopped waiting for new work.
func (s *Server) beginRequest() bool {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	if s.draining {
		return false
	}
	s.wg.Add(1)
	return true
}

// usableReplySubject reports whether a caller-supplied reply-to is something
// this gateway may publish and subscribe to on that caller's behalf, given the
// wire prefix this replica serves.
//
// Three hazards, all reachable from one narrow publish grant, because NATS
// permission-checks the subject a client publishes TO and never the reply-to —
// that is checked against whoever answers, which is us.
//
// A privileged subject makes the gateway a publish proxy for its own rights.
// With claim-check on, our identity carries JetStream rights,
// $JS.API.STREAM.DELETE.<stream> takes no request body, and the keepalive is
// an empty-bodied frame emitted unprompted: together, another tenant's claim
// bucket deleted by a caller who cannot publish to that subject themselves.
//
// A subject that is not a valid LITERAL takes the whole replica down. dispatch
// subscribes to reply+ctlSuffix, so a reply of ">" or "a.>" builds an invalid
// subscribe subject, the server answers -ERR 'Invalid Subject', and nats.go
// treats an unrecognised -ERR as fatal and closes the connection for good —
// MaxReconnects(-1) does not apply, and nothing here installs a ClosedHandler.
// One publish and the replica is silently off the air.
//
// A subject inside our own prefix turns the gateway into a reflector onto the
// wire: a reply of {prefix}.req.{victim}.… has us publish keepalive and end
// frames onto another tenant's endpoint subject. Those frames carry no
// Mcp-Wire header, so nothing runs on the far side, but a caller who cannot
// publish there is still spending our dispatch goroutines to reach it. No
// client inbox lives under the wire prefix, so refusing it costs nothing.
//
// So: a non-empty literal subject, no wildcards, no empty tokens, no
// whitespace, outside the server's own $ namespace and outside ours.
// Deliberately not a check that it looks like an inbox — a deployment may
// configure any inbox prefix, and guessing at that would refuse working
// clients. What is left unreachable by this is a subject an account MAPPING
// aliases onto something privileged under an ordinary-looking name, which no
// syntactic test can see and only the gateway's own NATS grant can bound.
func usableReplySubject(reply, prefix string) bool {
	if reply == "" || strings.HasPrefix(reply, "$") {
		return false
	}
	if reply == prefix || strings.HasPrefix(reply, prefix+".") {
		return false
	}
	for _, tok := range strings.Split(reply, ".") {
		// An empty token is its own case: ContainsAny cannot see it.
		if tok == "" || strings.ContainsAny(tok, "*> \t\r\n") {
			return false
		}
	}
	return true
}

// warnUnusableReply logs a refusal, at most one line per refusalLogInterval —
// see the throttle fields on Server for why a caller-triggered log needs one.
func (s *Server) warnUnusableReply(subject, reply string) {
	now := time.Now()
	s.refuseMu.Lock()
	if !s.refusedAt.IsZero() && now.Sub(s.refusedAt) < refusalLogInterval {
		s.refusedSuppressed++
		s.refuseMu.Unlock()
		return
	}
	suppressed := s.refusedSuppressed
	s.refusedSuppressed = 0
	s.refusedAt = now
	s.refuseMu.Unlock()

	// slog quotes a value that needs it, so the caller-controlled reply cannot
	// forge log lines of its own here.
	s.log.Warn("refusing a request whose reply subject is not a usable client inbox",
		"subject", subject, "reply", reply, "suppressed", suppressed)
}

// refuseDrained answers a request delivered after the drain gave up on it.
// ErrCodeStreamLost is the same "re-issue" signal a request caught in flight
// gets, and it reaches another replica in the time silence would have spent
// waiting out the caller's inactivity window.
func (s *Server) refuseDrained(req micro.Request) {
	// Same rule as dispatch: this publishes to the caller's chosen subject
	// too, and a drain is not a reason to stop checking where.
	if !usableReplySubject(req.Reply(), s.prefix) {
		return
	}
	w := &streamWriter{nc: s.nc, req: req}
	// Best effort on the id: a body that will not decode has none to echo,
	// and re-issuing is the answer either way.
	if msg, err := jsonrpc.Decode(req.Data()); err == nil {
		w.id = msg.ID
	}
	_ = w.Err(ErrCodeStreamLost, "gateway draining, re-issue the request", nil)
}

func (s *Server) keepAliveLoop(done <-chan struct{}, w *streamWriter) {
	ticker := time.NewTicker(s.keepAlive)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if !w.ka() {
				return
			}
		}
	}
}

// streamWriter implements StreamWriter with the exactly-one-terminal
// invariant. Terminal frames go through micro's Respond/Error so the free
// $SRV.STATS request and error counters stay truthful; msg/ka frames are
// plain publishes because micro's Respond is one-shot.
type streamWriter struct {
	nc  *nats.Conn
	req micro.Request
	id  []byte // raw JSON-RPC id of the request, for synthesized errors

	// Claim-check state, set once the subject has parsed: the store, the
	// tenant fencing the bucket, and whether THIS caller opted in.
	claims      ClaimStore
	tenant      string
	acceptClaim bool

	mu   sync.Mutex
	done bool
}

func (w *streamWriter) terminated() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.done
}

// claim marks the stream terminal; reports false if it already was.
//
// Two properties of this function are what order the frames on the reply
// subject, and neither is local to it — change either and Msg/ka silently
// start publishing behind the terminal frame:
//
//  1. done is set BEFORE the caller publishes its terminal frame. That is what
//     makes a later Msg refuse rather than publish.
//  2. the lock is released before that publish, and Msg/ka hold it ACROSS
//     theirs. That is what makes claim block behind a notification already in
//     flight instead of overtaking it.
//
// Together: every msg frame either completes before claim returns or never
// goes out at all. So do not "tidy" this by setting done after the terminal
// publish, and do not fold the publish into this function — holding the lock
// across it would stall the keepalive loop behind whatever End is doing.
func (w *streamWriter) claim() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return false
	}
	w.done = true
	return true
}

// Msg publishes under the same lock the terminal frames claim, rather than
// checking terminated() and publishing after releasing it. In that gap a
// concurrent End can claim the stream and respond, putting this notification
// on the reply subject BEHIND the terminal frame — where the consumer has
// already stopped reading and the request is already answered. The proxy
// reaches the gap for real: pkg/backend's mux hands a response to the blocked
// Call and reads on immediately, so a backend notification arriving before
// Call unregisters is dispatched here while the handler is inside End.
//
// The cost is deliberate: a publish that blocks on a slow connection now holds
// up the terminal frame and the keepalives too. That is the right way round —
// a terminal frame that overtook a stalled notification is the bug — and a
// connection too backed up to accept a publish was going to stall the terminal
// frame at the socket regardless.
func (w *streamWriter) Msg(body []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return fmt.Errorf("wire: stream already terminated")
	}
	if max := w.nc.MaxPayload(); int64(len(body)) > max {
		return &Error{
			Code:    ErrCodePayloadTooLarge,
			Message: fmt.Sprintf("notification %d bytes exceeds NATS max_payload %d, dropped", len(body), max),
		}
	}
	return w.nc.PublishMsg(&nats.Msg{
		Subject: w.req.Reply(),
		Data:    body,
		Header:  nats.Header{HeaderFrame: []string{string(FrameMsg)}},
	})
}

// ka publishes one keepalive, reporting whether the stream is still live. It
// holds the lock across the publish for the same reason Msg does: once the
// terminal frame is on the reply subject, nothing else may follow it.
//
// The bool says "not terminal yet", NOT "the keepalive was delivered" — the
// publish error is dropped on purpose. A keepalive is a hint that resets the
// caller's inactivity deadline; a connection that cannot carry one cannot
// carry the response either, and that failure belongs to whoever is publishing
// the response, not to a ticker.
func (w *streamWriter) ka() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return false
	}
	_ = w.nc.PublishMsg(&nats.Msg{
		Subject: w.req.Reply(),
		Header:  nats.Header{HeaderFrame: []string{string(FrameKA)}},
	})
	return true
}

func (w *streamWriter) End(body []byte) error {
	if max := w.nc.MaxPayload(); int64(len(body)) > max {
		// Claim-check: park the body and send only the reference — but only
		// for callers that opted in (HeaderAcceptClaim), because a claimed
		// end frame's empty body would read as "cancelled" to anyone else.
		if w.claims != nil && w.acceptClaim && len(body) <= claimMaxBody {
			ctx, cancel := context.WithTimeout(context.Background(), claimOpTimeout)
			id, err := w.claims.Put(ctx, w.tenant, body)
			cancel()
			if err == nil {
				if !w.claim() {
					// Parking the body and claiming the stream cannot be one
					// step, and something terminated the stream in between —
					// a cancellation, most likely. The id is never sent, so
					// nothing can ever fetch this object, and the fetch is what
					// would have deleted it. Best effort, like the fetch-side
					// delete: the bucket TTL stays the backstop, and this
					// stream is already answered, so there is no one to tell.
					dctx, dcancel := context.WithTimeout(context.Background(), claimOpTimeout)
					_ = w.claims.Delete(dctx, w.tenant, id)
					dcancel()
					return nil
				}
				return w.req.Respond(nil, micro.WithHeaders(micro.Headers{
					HeaderFrame: []string{string(FrameEnd)},
					HeaderClaim: []string{id},
				}))
			}
			// Fall through: degrade to the legible oversize error, never a hang.
		}
		return w.Err(ErrCodePayloadTooLarge,
			fmt.Sprintf("response %d bytes exceeds NATS max_payload %d", len(body), max),
			map[string]int64{"size": int64(len(body)), "limit": max})
	}
	if !w.claim() {
		return nil
	}
	return w.req.Respond(body,
		micro.WithHeaders(micro.Headers{HeaderFrame: []string{string(FrameEnd)}}))
}

func (w *streamWriter) Err(code int, message string, data any) error {
	return w.errWithID(w.id, code, message, data)
}

func (w *streamWriter) errWithID(id []byte, code int, message string, data any) error {
	if !w.claim() {
		return nil
	}
	body, err := jsonrpc.Encode(jsonrpc.NewErrorResponse(id, code, message, data))
	if err != nil {
		body = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"error encoding failed"}}`)
	}
	return w.req.Error(strconv.Itoa(code), message, body,
		micro.WithHeaders(micro.Headers{HeaderFrame: []string{string(FrameErr)}}))
}
