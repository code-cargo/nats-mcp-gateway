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
	// Servers is the list of MCP server names to register endpoints for.
	Servers []string
	// Name/Version identify the micro service ($SRV.INFO).
	Name    string
	Version string
	// KeepAlive overrides DefaultKeepAlive when > 0.
	KeepAlive time.Duration
}

// Server is a running wire endpoint fleet member.
type Server struct {
	svc       micro.Service
	nc        *nats.Conn
	keepAlive time.Duration

	base   context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup
}

// Serve registers one micro endpoint per configured MCP server and starts
// handling requests. Each request runs in its own goroutine; per-backend
// concurrency limits belong to the backend pool, not the wire.
func Serve(nc *nats.Conn, cfg ServerConfig, handler Handler) (*Server, error) {
	if cfg.QueueGroup == "" {
		cfg.QueueGroup = "mcpgw"
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
	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("wire: no servers configured")
	}

	base, cancel := context.WithCancelCause(context.Background())
	s := &Server{nc: nc, keepAlive: cfg.KeepAlive, base: base, cancel: cancel}

	svc, err := micro.AddService(nc, micro.Config{
		Name:    cfg.Name,
		Version: cfg.Version,
	})
	if err != nil {
		cancel(nil)
		return nil, fmt.Errorf("wire: add micro service: %w", err)
	}
	s.svc = svc

	prefix := cfg.Prefix
	if prefix == "" {
		prefix = DefaultPrefix
	}
	for _, server := range cfg.Servers {
		subject, err := EndpointSubject(prefix, server)
		if err != nil {
			_ = svc.Stop()
			cancel(nil)
			return nil, err
		}
		err = svc.AddEndpoint(
			endpointName(server),
			micro.HandlerFunc(func(req micro.Request) { s.dispatch(prefix, req, handler) }),
			micro.WithEndpointSubject(subject),
			micro.WithEndpointQueueGroup(cfg.QueueGroup),
		)
		if err != nil {
			_ = svc.Stop()
			cancel(nil)
			return nil, fmt.Errorf("wire: add endpoint %q: %w", server, err)
		}
	}
	return s, nil
}

// endpointName maps a server name to a micro endpoint name (must satisfy
// micro's name rules; our server tokens already do).
func endpointName(server string) string { return "mcp-" + server }

// Shutdown drains: stop accepting requests, terminate every live stream with
// ErrCodeStreamLost so clients re-issue immediately, then wait for handlers
// up to ctx's deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.svc.Stop()
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
