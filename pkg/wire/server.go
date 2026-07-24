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
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
)

const (
	// DefaultKeepAlive is the ka-frame interval on idle streams. Must be
	// well under the client's inactivity deadline.
	DefaultKeepAlive = 15 * time.Second
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

	svcMu sync.Mutex
	svcs  map[string]micro.Service // server name -> its micro service

	base   context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup
}

// Serve starts the wire server with the given initial server set. Each
// request runs in its own goroutine; per-backend concurrency limits belong to
// the backend pool, not the wire.
func Serve(nc *nats.Conn, cfg ServerConfig, handler Handler) (*Server, error) {
	if cfg.QueueGroup == "" {
		// A scoped instance defaults to its OWN queue group: NATS dedupes
		// queue subscribers by group name across different subject patterns,
		// so sharing "mcpgw" with an unscoped fleet serving the same server
		// names would make the two compete for the scoped traffic. The group
		// mirrors the scope — mcpgw.{tenant} for an org deployment,
		// mcpgw.{tenant}.{user} for a per-user pod. This default lives here —
		// not in the CLI — so every config source gets it.
		switch {
		case cfg.Tenant != "" && cfg.User != "":
			cfg.QueueGroup = "mcpgw." + cfg.Tenant + "." + cfg.User
		case cfg.Tenant != "":
			cfg.QueueGroup = "mcpgw." + cfg.Tenant
		default:
			cfg.QueueGroup = "mcpgw"
		}
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
	}

	if cfg.Tenant == "" && cfg.User != "" {
		return nil, fmt.Errorf("wire: endpoint scoping with a User requires a Tenant (got tenant=%q, user=%q)", cfg.Tenant, cfg.User)
	}

	base, cancel := context.WithCancelCause(context.Background())
	s := &Server{
		nc:         nc,
		handler:    handler,
		prefix:     prefix,
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

	for name, svc := range s.svcs {
		if _, ok := desired[name]; !ok {
			_ = svc.Stop()
			delete(s.svcs, name)
		}
	}

	for _, name := range names {
		if _, ok := s.svcs[name]; ok {
			continue
		}
		subject, err := EndpointSubject(s.prefix, s.tenant, s.user, name)
		if err != nil {
			return err
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
			return fmt.Errorf("wire: add service %q: %w", name, err)
		}
		s.svcs[name] = svc
	}
	return nil
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
// wait for handlers up to ctx's deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.stopAllServices()
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
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

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

		// Control subject: any message on {reply}.ctl cancels this request.
		ctlSub, err := s.nc.Subscribe(req.Reply()+ctlSuffix, func(*nats.Msg) {
			cancel(errClientCancelled)
		})
		if err == nil {
			defer func() { _ = ctlSub.Unsubscribe() }()
		}

		// Keepalives until terminal.
		kaDone := make(chan struct{})
		defer close(kaDone)
		go s.keepAliveLoop(req.Reply(), kaDone, w)

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

func (s *Server) keepAliveLoop(reply string, done <-chan struct{}, w *streamWriter) {
	ticker := time.NewTicker(s.keepAlive)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if w.terminated() {
				return
			}
			_ = s.nc.PublishMsg(&nats.Msg{
				Subject: reply,
				Header:  nats.Header{HeaderFrame: []string{string(FrameKA)}},
			})
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
func (w *streamWriter) claim() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return false
	}
	w.done = true
	return true
}

func (w *streamWriter) Msg(body []byte) error {
	if w.terminated() {
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
