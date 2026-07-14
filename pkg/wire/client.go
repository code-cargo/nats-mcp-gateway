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
	"strings"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
)

const (
	// DefaultInactivity is how long a stream may be silent (no frames, no
	// keepalives) before the client declares it lost. It must comfortably
	// exceed the server's keepalive interval.
	DefaultInactivity = 60 * time.Second

	// ctlSuffix is appended to the reply subject to form the per-request
	// control subject (cancellation). The client subscribes to the reply
	// subject exactly, so its own ctl publishes are never self-received.
	ctlSuffix = ".ctl"

	// pendingMsgsLimit / pendingBytesLimit bound the per-request
	// subscription so one runaway stream degrades into a legible
	// ErrCodeStreamLost instead of unbounded memory growth.
	pendingMsgsLimit  = 4096
	pendingBytesLimit = 64 * 1024 * 1024
)

// ClientConfig configures a wire client.
type ClientConfig struct {
	// Prefix is the subject prefix (DefaultPrefix if empty).
	Prefix string
	// Tenant is this caller's tenant token.
	Tenant string
	// Inactivity overrides DefaultInactivity when > 0.
	Inactivity time.Duration
}

// Client sends requests over the wire. Tenant-inbox isolation, when wanted,
// comes from connecting the *nats.Conn with nats.CustomInboxPrefix — the
// client always derives reply subjects via nc.NewRespInbox.
type Client struct {
	nc         *nats.Conn
	prefix     string
	tenant     string
	inactivity time.Duration

	// A denied publish surfaces as an async connection error, not on the
	// subscription — without this registry the stream would sit out the
	// full inactivity window before failing.
	violMu sync.Mutex
	viol   map[string]map[*violReg]struct{} // publish subject -> streams
}

// NewClient wraps an established NATS connection.
func NewClient(nc *nats.Conn, cfg ClientConfig) (*Client, error) {
	if cfg.Tenant == "" || !TokenSafe(cfg.Tenant) {
		return nil, fmt.Errorf("wire: invalid tenant %q", cfg.Tenant)
	}
	c := &Client{
		nc:         nc,
		prefix:     cfg.Prefix,
		tenant:     cfg.Tenant,
		inactivity: cfg.Inactivity,
		viol:       make(map[string]map[*violReg]struct{}),
	}
	if c.prefix == "" {
		c.prefix = DefaultPrefix
	}
	if c.inactivity <= 0 {
		c.inactivity = DefaultInactivity
	}

	// Chain onto any existing handler rather than clobbering it.
	prev := nc.Opts.AsyncErrorCB
	nc.SetErrorHandler(func(conn *nats.Conn, sub *nats.Subscription, err error) {
		if prev != nil {
			prev(conn, sub, err)
		}
		if errors.Is(err, nats.ErrPermissionViolation) {
			c.failViolated(err)
		}
	})
	return c, nil
}

// violReg identifies one stream's registration in the violation registry.
type violReg struct {
	cancel context.CancelCauseFunc
}

// failViolated cancels every stream whose publish subject the server just
// refused.
func (c *Client) failViolated(err error) {
	subject := quotedSubject(err.Error())
	if subject == "" {
		return
	}
	c.violMu.Lock()
	regs := c.viol[subject]
	delete(c.viol, subject)
	c.violMu.Unlock()
	for r := range regs {
		r.cancel(&Error{
			Code:    ErrCodePermissionDenied,
			Message: fmt.Sprintf("NATS denied publish to %q: this caller's permissions do not cover it", subject),
		})
	}
}

// quotedSubject extracts the subject from a NATS permission-violation error
// ('Permissions Violation for Publish to "mcp.v1..."').
func quotedSubject(s string) string {
	i := strings.IndexByte(s, '"')
	if i < 0 {
		return ""
	}
	j := strings.IndexByte(s[i+1:], '"')
	if j < 0 {
		return ""
	}
	return s[i+1 : i+1+j]
}

// Request is one outbound MCP request: opaque JSON-RPC bytes plus the
// envelope facts the subject and headers are built from. The caller (shim,
// debug CLI) extracted Method/Name/ProtocolVersion from the body it already
// owns; the wire never parses bodies.
type Request struct {
	Server          string
	Method          string // MCP form, e.g. "tools/call"
	Name            string // params.name / params.uri; "" if none
	ProtocolVersion string
	Body            []byte
}

// Stream is one in-flight request's reply stream. Frames arrive on C in
// order, ending with exactly one terminal frame (Kind end or err), after
// which C is closed. Keepalives are consumed internally.
type Stream struct {
	// C delivers msg frames and the single terminal frame.
	C <-chan Frame

	reply string
	nc    *nats.Conn
	stop  context.CancelFunc
}

// Cancel publishes the given JSON-RPC notification bytes (typically
// notifications/cancelled) to the request's control subject. Best-effort by
// design: on an at-most-once bus a cancel can race the gateway's control
// subscription and be lost; the request then simply completes or times out.
func (s *Stream) Cancel(notification []byte) error {
	return s.nc.Publish(s.reply+ctlSuffix, notification)
}

// Close abandons the stream locally without cancelling the remote work.
func (s *Stream) Close() { s.stop() }

// Do publishes the request and returns its reply stream. It fails fast —
// before publishing — on oversize bodies, and converts NATS-level failures
// (no responders, inactivity, slow consumer) into a terminal FrameErr so
// consumers have exactly one code path.
func (c *Client) Do(ctx context.Context, req *Request) (*Stream, error) {
	if max := c.nc.MaxPayload(); int64(len(req.Body)) > max {
		return nil, &Error{
			Code:    ErrCodePayloadTooLarge,
			Message: fmt.Sprintf("request body %d bytes exceeds NATS max_payload %d", len(req.Body), max),
		}
	}
	subject, err := BuildSubject(c.prefix, c.tenant, req.Server, req.Method, req.Name)
	if err != nil {
		return nil, err
	}

	reply := c.nc.NewRespInbox()
	sub, err := c.nc.SubscribeSync(reply)
	if err != nil {
		return nil, fmt.Errorf("wire: subscribe reply: %w", err)
	}
	if err := sub.SetPendingLimits(pendingMsgsLimit, pendingBytesLimit); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("wire: pending limits: %w", err)
	}

	msg := &nats.Msg{
		Subject: subject,
		Reply:   reply,
		Data:    req.Body,
		Header: nats.Header{
			HeaderWire:            []string{WireVersion},
			HeaderMethod:          []string{req.Method},
			HeaderProtocolVersion: []string{req.ProtocolVersion},
		},
	}
	if req.Name != "" {
		msg.Header.Set(HeaderName, req.Name)
	}
	streamCtx, stop := context.WithCancelCause(ctx)

	// Register for permission-violation failure BEFORE publishing.
	reg := &violReg{cancel: stop}
	c.violMu.Lock()
	if c.viol[subject] == nil {
		c.viol[subject] = make(map[*violReg]struct{})
	}
	c.viol[subject][reg] = struct{}{}
	c.violMu.Unlock()
	unregister := func() {
		c.violMu.Lock()
		defer c.violMu.Unlock()
		if regs := c.viol[subject]; regs != nil {
			delete(regs, reg)
			if len(regs) == 0 {
				delete(c.viol, subject)
			}
		}
	}

	if err := c.nc.PublishMsg(msg); err != nil {
		unregister()
		stop(nil)
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("wire: publish: %w", err)
	}

	frames := make(chan Frame)
	s := &Stream{C: frames, reply: reply, nc: c.nc, stop: func() { stop(nil) }}
	go func() {
		defer unregister()
		c.pump(streamCtx, sub, frames)
	}()
	return s, nil
}

// pump reads reply messages, enforces the inactivity deadline, and delivers
// frames until terminal. It owns closing the channel and the subscription.
func (c *Client) pump(ctx context.Context, sub *nats.Subscription, frames chan<- Frame) {
	defer close(frames)
	defer func() { _ = sub.Unsubscribe() }()

	deliver := func(f Frame) bool {
		select {
		case frames <- f:
			return true
		case <-ctx.Done():
			return false
		}
	}
	fail := func(code int, message string) {
		deliver(Frame{Kind: FrameErr, Err: &Error{Code: code, Message: message}})
	}
	// failCaused surfaces a cancellation cause (permission violation) even
	// though ctx is already done; the consumer is our own pump loop and
	// always drains, so a short timeout only guards a vanished consumer.
	failCaused := func(werr *Error) {
		select {
		case frames <- Frame{Kind: FrameErr, Err: werr}:
		case <-time.After(3 * time.Second):
		}
	}

	for {
		next, cancel := context.WithTimeout(ctx, c.inactivity)
		msg, err := sub.NextMsgWithContext(next)
		cancel()
		switch {
		case err == nil:
			// fall through to frame handling
		case ctx.Err() != nil:
			var werr *Error
			if errors.As(context.Cause(ctx), &werr) {
				failCaused(werr)
			}
			return // otherwise: caller abandoned the stream
		case errors.Is(err, context.DeadlineExceeded):
			fail(ErrCodeStreamLost, fmt.Sprintf("stream inactive for %s", c.inactivity))
			return
		case errors.Is(err, nats.ErrNoResponders):
			fail(ErrCodeNoGateway, "no gateway is serving this server (NATS no-responders)")
			return
		case errors.Is(err, nats.ErrSlowConsumer):
			fail(ErrCodeStreamLost, "reply stream overflowed local pending limits")
			return
		default:
			fail(ErrCodeStreamLost, fmt.Sprintf("reply subscription failed: %v", err))
			return
		}

		kind := FrameKind(msg.Header.Get(HeaderFrame))
		switch kind {
		case FrameKA:
			continue // consumed internally; its arrival already reset the deadline
		case FrameMsg:
			if !deliver(Frame{Kind: FrameMsg, Body: msg.Data}) {
				return
			}
		case FrameEnd, FrameErr:
			deliver(Frame{Kind: kind, Body: msg.Data})
			return
		default:
			fail(ErrCodeStreamLost, fmt.Sprintf("unknown frame kind %q", msg.Header.Get(HeaderFrame)))
			return
		}
	}
}
