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
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
)

// runNATS starts an embedded nats-server on a random port.
func runNATS(t *testing.T, opts *server.Options) *nats.Conn {
	t.Helper()
	nc, _ := natstest.Run(t, opts)
	return nc
}

func testRequest(id, method string) *Request {
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": method, "params": map[string]any{},
	})
	return &Request{Server: "test", Method: method, ProtocolVersion: "2026-07-28", Body: body}
}

// collect drains a stream until terminal, with a test-level timeout.
func collect(t *testing.T, s *Stream) []Frame {
	t.Helper()
	var frames []Frame
	timeout := time.After(10 * time.Second)
	for {
		select {
		case f, ok := <-s.C:
			if !ok {
				return frames
			}
			frames = append(frames, f)
			if f.Kind.Terminal() {
				// drain to close
				for range s.C {
				}
				return frames
			}
		case <-timeout:
			t.Fatal("stream did not terminate")
		}
	}
}

func serve(t *testing.T, nc *nats.Conn, cfg ServerConfig, h Handler) *Server {
	t.Helper()
	if len(cfg.Servers) == 0 {
		cfg.Servers = []string{"test"}
	}
	srv, err := Serve(nc, cfg, h)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

func client(t *testing.T, nc *nats.Conn, inactivity time.Duration) *Client {
	t.Helper()
	c, err := NewClient(nc, ClientConfig{Tenant: "acme", Inactivity: inactivity})
	require.NoError(t, err)
	return c
}

func TestStreamMsgsThenEnd(t *testing.T) {
	nc := runNATS(t, nil)
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		assert.Equal(t, "acme", in.Subject.Tenant)
		assert.Equal(t, "tools/call", in.Subject.Method)
		require.NoError(t, w.Msg([]byte(`{"jsonrpc":"2.0","method":"notifications/progress","params":{"n":1}}`)))
		require.NoError(t, w.Msg([]byte(`{"jsonrpc":"2.0","method":"notifications/progress","params":{"n":2}}`)))
		return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{"ok":true}}`))
	})

	s, err := client(t, nc, 0).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)

	require.Len(t, frames, 3)
	assert.Equal(t, FrameMsg, frames[0].Kind)
	assert.Contains(t, string(frames[0].Body), `"n":1`)
	assert.Equal(t, FrameMsg, frames[1].Kind)
	assert.Contains(t, string(frames[1].Body), `"n":2`)
	assert.Equal(t, FrameEnd, frames[2].Kind)
	assert.Contains(t, string(frames[2].Body), `"ok":true`)
}

func TestUserAttribution(t *testing.T) {
	nc := runNATS(t, nil)
	gotUser := make(chan string, 1)
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		gotUser <- in.Subject.User
		return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
	})

	// A client with a user token: the gateway parses it off the subject.
	c, err := NewClient(nc, ClientConfig{Tenant: "acme", User: "u_9f3a", Inactivity: time.Second})
	require.NoError(t, err)
	_ = collect(t, mustDo(t, c, testRequest("1", "tools/call")))
	assert.Equal(t, "u_9f3a", <-gotUser, "gateway sees the caller's user token")

	// A client with no user token defaults to the unattributed placeholder.
	c2, err := NewClient(nc, ClientConfig{Tenant: "acme", Inactivity: time.Second})
	require.NoError(t, err)
	_ = collect(t, mustDo(t, c2, testRequest("2", "tools/call")))
	assert.Equal(t, UserUnattributed, <-gotUser, "unset user is the '_' placeholder")
}

func mustDo(t *testing.T, c *Client, req *Request) *Stream {
	t.Helper()
	s, err := c.Do(context.Background(), req)
	require.NoError(t, err)
	return s
}

func TestErrTerminal(t *testing.T) {
	nc := runNATS(t, nil)
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return w.Err(-32020, "header mismatch", nil)
	})

	s, err := client(t, nc, 0).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)

	require.Len(t, frames, 1)
	assert.Equal(t, FrameErr, frames[0].Kind)
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, -32020, m.Error.Code)
	assert.JSONEq(t, `"1"`, string(m.ID), "error must echo the request id")
}

func TestCancelViaCtl(t *testing.T) {
	nc := runNATS(t, nil)
	started := make(chan struct{})
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		close(started)
		<-ctx.Done() // wait for client cancellation
		return nil   // wrapper writes the empty end frame
	})

	s, err := client(t, nc, 0).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	<-started
	require.NoError(t, s.Cancel([]byte(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"1"}}`)))

	frames := collect(t, s)
	require.Len(t, frames, 1)
	assert.Equal(t, FrameEnd, frames[0].Kind)
	assert.Empty(t, frames[0].Body, "cancelled request ends with an empty body: no response exists")
}

func TestKeepAliveHoldsIdleStream(t *testing.T) {
	nc := runNATS(t, nil)
	serve(t, nc, ServerConfig{KeepAlive: 30 * time.Millisecond}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		time.Sleep(400 * time.Millisecond) // several client inactivity windows
		return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
	})

	s, err := client(t, nc, 120*time.Millisecond).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1)
	assert.Equal(t, FrameEnd, frames[0].Kind, "keepalives must hold the stream open")
}

func TestInactivityFires(t *testing.T) {
	nc := runNATS(t, nil)
	serve(t, nc, ServerConfig{KeepAlive: 10 * time.Second}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		time.Sleep(400 * time.Millisecond) // silent: keepalive is far away
		return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
	})

	s, err := client(t, nc, 100*time.Millisecond).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1)
	assert.Equal(t, FrameErr, frames[0].Kind)
	require.NotNil(t, frames[0].Err)
	assert.Equal(t, ErrCodeStreamLost, frames[0].Err.Code)
}

func TestNoGateway(t *testing.T) {
	nc := runNATS(t, nil)
	// No server registered at all.
	s, err := client(t, nc, 0).Do(context.Background(), testRequest("1", "tools/list"))
	require.NoError(t, err)

	start := time.Now()
	frames := collect(t, s)
	require.Len(t, frames, 1)
	assert.Equal(t, FrameErr, frames[0].Kind)
	require.NotNil(t, frames[0].Err)
	assert.Equal(t, ErrCodeNoGateway, frames[0].Err.Code)
	assert.Less(t, time.Since(start), 2*time.Second, "no-responders must fail fast, not wait out inactivity")
}

// Every request hangs a cancellable child off its caller's context, and only
// Stream.Close, a permission violation or a failed publish ever released it —
// never the normal terminal frame. A caller that outlives its requests (the
// shim runs one context for the whole process and issues every request under
// it) therefore accumulated one cancelCtx per completed request, forever.
func TestCompletedRequestReleasesItsCallerContext(t *testing.T) {
	nc := runNATS(t, nil)
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
	})

	caller := &countingCtx{done: make(chan struct{})}
	c := client(t, nc, 5*time.Second)
	for i := 0; i < 20; i++ {
		s, err := c.Do(caller, testRequest("1", "tools/call"))
		require.NoError(t, err)
		frames := collect(t, s)
		require.Len(t, frames, 1)
		require.Equal(t, FrameEnd, frames[0].Kind)
	}
	require.Positive(t, caller.peak(), "the requests must have registered on the caller's context at all")

	// The release lands just behind the frame channel's close, so let the last
	// request's goroutine finish unwinding.
	assert.Eventually(t, caller.empty, 2*time.Second, 10*time.Millisecond,
		"completed requests are still registered on the caller's context")
}

// countingCtx is a cancellable context that counts what is registered against
// it. context keeps its own child registry unexported, but it hands
// registration to a parent that implements AfterFunc — so a parent that does
// gets to watch the wire register a request and, once fixed, release it. The
// context is never actually cancelled: outliving its requests is the point.
type countingCtx struct {
	done chan struct{}

	mu   sync.Mutex
	live int
	seen int
}

func (c *countingCtx) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *countingCtx) Done() <-chan struct{} { return c.done }

func (c *countingCtx) Err() error { return nil }

func (c *countingCtx) Value(any) any { return nil }

func (c *countingCtx) AfterFunc(func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.live++
	c.seen++
	released := false
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if released {
			return false
		}
		released = true
		c.live--
		return true
	}
}

func (c *countingCtx) empty() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.live == 0
}

func (c *countingCtx) peak() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen
}

func TestOversizeRequestFailsBeforePublish(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: 1024})
	req := testRequest("1", "tools/call")
	req.Body = append(req.Body, make([]byte, 4096)...)

	_, err := client(t, nc, 0).Do(context.Background(), req)
	require.Error(t, err)
	var werr *Error
	require.ErrorAs(t, err, &werr)
	assert.Equal(t, ErrCodePayloadTooLarge, werr.Code)
}

func TestOversizeResponseBecomesErrFrame(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: 1024})
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		big := fmt.Sprintf(`{"jsonrpc":"2.0","id":"1","result":{"blob":%q}}`, make([]byte, 4096))
		return w.End([]byte(big))
	})

	s, err := client(t, nc, 0).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1)
	assert.Equal(t, FrameErr, frames[0].Kind)
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, ErrCodePayloadTooLarge, m.Error.Code)
}

func TestDrainTerminatesLiveStreams(t *testing.T) {
	nc := runNATS(t, nil)
	started := make(chan struct{})
	srv := serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		close(started)
		<-ctx.Done()
		return nil
	})

	s, err := client(t, nc, 0).Do(context.Background(), testRequest("1", "subscriptions/listen"))
	require.NoError(t, err)
	<-started

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(shutdownCtx))

	frames := collect(t, s)
	require.Len(t, frames, 1)
	assert.Equal(t, FrameErr, frames[0].Kind)
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, ErrCodeStreamLost, m.Error.Code)
	assert.Contains(t, m.Error.Message, "draining")
}

// Shutdown must never report a completed drain while the gateway is still
// taking work on. micro stops an endpoint with Subscription.Drain, which
// returns as soon as the UNSUB is buffered — the request published a moment
// earlier is still delivered to the handler afterwards. A drain that returns
// in that window leaves the caller with no terminal frame at all once the
// process exits, costing it the whole inactivity window.
func TestShutdownDoesNotAbandonRequestsItAccepts(t *testing.T) {
	nc := runNATS(t, nil)

	var running atomic.Int32
	var drainReturned, lateStart atomic.Bool
	srv := serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		if drainReturned.Load() {
			lateStart.Store(true)
		}
		running.Add(1)
		defer running.Add(-1)
		// Long enough that a handler picked up before the drain finished is
		// unmistakably still running when Shutdown returns.
		time.Sleep(200 * time.Millisecond)
		return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
	})

	// Inactivity far beyond collect's own fatal timeout: a caller left without
	// a terminal frame fails the test rather than quietly waiting it out.
	s, err := client(t, nc, 30*time.Second).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)

	// No handshake with the handler first: the request is on the wire but not
	// yet dispatched, which is precisely the window Shutdown has to cover.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	drainReturned.Store(true)
	stillRunning := running.Load()
	require.NoError(t, err)
	assert.Zero(t, stillRunning, "Shutdown reported a completed drain with a handler still running")

	// A dispatch landing after the drain is the same failure seen from the
	// other side — nothing is left to wait for it.
	time.Sleep(500 * time.Millisecond)
	assert.False(t, lateStart.Load(), "a request was dispatched after Shutdown reported a completed drain")

	// Whichever side of the drain it fell on, the caller gets a terminal frame.
	frames := collect(t, s)
	require.Len(t, frames, 1)
	assert.True(t, frames[0].Kind.Terminal())
}

// Once the drain has stopped waiting for new work, a request micro still
// delivers is answered rather than dropped: the caller re-issues against
// another replica instead of spending its whole inactivity window on a reply
// that is never coming.
func TestDrainedGatewayRefusesRatherThanGoingSilent(t *testing.T) {
	nc := runNATS(t, nil)
	srv := serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		t.Error("a drained gateway must not take on a handler")
		return nil
	})
	// Marked without stopping the services, so the request still routes and
	// the refusal is unambiguously what answers it.
	srv.drainMu.Lock()
	srv.draining = true
	srv.drainMu.Unlock()

	s, err := client(t, nc, 30*time.Second).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, ErrCodeStreamLost, m.Error.Code)
	assert.JSONEq(t, `"1"`, string(m.ID), "the refusal must echo the id so the client can fail that request")
}

func TestBadWireVersionRejected(t *testing.T) {
	nc := runNATS(t, nil)
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		t.Error("handler must not run for a bad wire version")
		return nil
	})

	// Hand-build the request to force a wrong Mcp-Wire header.
	reply := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(reply)
	require.NoError(t, err)
	subj, err := BuildSubject("", "acme", "_", "test", "tools/list", "")
	require.NoError(t, err)
	require.NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: subj,
		Reply:   reply,
		Data:    []byte(`{"jsonrpc":"2.0","id":"1","method":"tools/list"}`),
		Header:  nats.Header{HeaderWire: []string{"99"}},
	}))
	msg, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, string(FrameErr), msg.Header.Get(HeaderFrame))
	m, err := jsonrpc.Decode(msg.Data)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, jsonrpc.CodeInvalidRequest, m.Error.Code)
}

// A scoped instance (per-user pod shape) binds only its own
// {tenant}.{user} slice of the subject space: its user's requests reach it,
// anyone else's hit no-responders. It must NOT share a queue group with an
// instance serving the same server names for everyone.
func TestScopedEndpointServesOnlyItsUser(t *testing.T) {
	nc := runNATS(t, nil)
	serve(t, nc, ServerConfig{Tenant: "acme", User: "u1", QueueGroup: "qg-acme-u1"},
		func(ctx context.Context, in *Inbound, w StreamWriter) error {
			assert.Equal(t, "acme", in.Subject.Tenant)
			assert.Equal(t, "u1", in.Subject.User)
			return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{"served_by":"scoped"}}`))
		})

	asUser := func(user string) []Frame {
		c, err := NewClient(nc, ClientConfig{Tenant: "acme", User: user, Inactivity: 2 * time.Second})
		require.NoError(t, err)
		s, err := c.Do(context.Background(), testRequest("1", "tools/call"))
		require.NoError(t, err)
		return collect(t, s)
	}

	frames := asUser("u1")
	require.Len(t, frames, 1)
	assert.Equal(t, FrameEnd, frames[0].Kind)
	assert.Contains(t, string(frames[0].Body), "scoped")

	frames = asUser("u2")
	require.Len(t, frames, 1)
	require.NotNil(t, frames[0].Err, "another user's request must not reach the scoped instance")
	assert.Equal(t, ErrCodeNoGateway, frames[0].Err.Code)
}

// A tenant-scoped instance (org-deployment shape) binds its whole tenant's
// slice with the user token wildcarded: every user of that tenant reaches it,
// other tenants hit no-responders. Like a per-user pod, it must not share a
// queue group with an unscoped instance serving the same server names.
func TestTenantScopedEndpointServesAllItsUsers(t *testing.T) {
	nc := runNATS(t, nil)
	serve(t, nc, ServerConfig{Tenant: "acme", QueueGroup: "qg-acme"},
		func(ctx context.Context, in *Inbound, w StreamWriter) error {
			assert.Equal(t, "acme", in.Subject.Tenant)
			return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{"served_by":"tenant-scoped"}}`))
		})

	inTenant := func(tenant, user string) []Frame {
		c, err := NewClient(nc, ClientConfig{Tenant: tenant, User: user, Inactivity: 2 * time.Second})
		require.NoError(t, err)
		s, err := c.Do(context.Background(), testRequest("1", "tools/call"))
		require.NoError(t, err)
		return collect(t, s)
	}

	// Any user of the scoped tenant is served — including the unattributed
	// placeholder — because the user token is a wildcard.
	for _, user := range []string{"u1", "u2", UserUnattributed} {
		frames := inTenant("acme", user)
		require.Len(t, frames, 1)
		assert.Equal(t, FrameEnd, frames[0].Kind, "user %q of the scoped tenant must be served", user)
		assert.Contains(t, string(frames[0].Body), "tenant-scoped")
	}

	// Another tenant's request must not reach it.
	frames := inTenant("other", "u1")
	require.Len(t, frames, 1)
	require.NotNil(t, frames[0].Err, "another tenant's request must not reach the tenant-scoped instance")
	assert.Equal(t, ErrCodeNoGateway, frames[0].Err.Code)
}

// A User without a Tenant would scope by the attribution token alone,
// spanning every tenant — Serve rejects it. (Tenant alone is the valid
// org-deployment scope; see TestTenantScopedEndpointServesAllItsUsers.)
func TestServeRejectsUserWithoutTenant(t *testing.T) {
	nc := runNATS(t, nil)
	_, err := Serve(nc, ServerConfig{User: "u1", Servers: []string{"test"}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a Tenant")
}

// A NATS publish denial must fail the stream immediately with -32013 — not
// sit out the inactivity window. The violation arrives as an async connection
// error; the client's registry maps it back to the exact denied subject.
func TestPermissionViolationFailsFast(t *testing.T) {
	opts := &server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		Users: []*server.User{{
			Username: "restricted", Password: "pw",
			Permissions: &server.Permissions{
				Publish:   &server.SubjectPermission{Allow: []string{"mcp.v1.req.acme._.allowed.>", "_INBOX.>"}},
				Subscribe: &server.SubjectPermission{Allow: []string{"_INBOX.>"}},
			},
		}},
	}
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL(), nats.UserInfo("restricted", "pw"))
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	// Inactivity far beyond collect's own 10s timeout: if the fast-fail path
	// is broken, the frame would only arrive at the inactivity deadline and
	// collect would fatal first.
	c, err := NewClient(nc, ClientConfig{Tenant: "acme", Inactivity: 30 * time.Second})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": "1", "method": "tools/call", "params": map[string]any{},
	})
	s, err := c.Do(context.Background(), &Request{
		Server: "denied", Method: "tools/call", ProtocolVersion: "2026-07-28", Body: body,
	})
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	require.NotNil(t, frames[0].Err)
	assert.Equal(t, ErrCodePermissionDenied, frames[0].Err.Code)
	assert.Contains(t, frames[0].Err.Message, "permissions do not cover")

	// An allowed subject on the same connection still works normally
	// (no-responders here, since nothing serves it — the point is the
	// publish itself is permitted and fails differently).
	s, err = c.Do(context.Background(), &Request{
		Server: "allowed", Method: "tools/call", ProtocolVersion: "2026-07-28", Body: body,
	})
	require.NoError(t, err)
	frames = collect(t, s)
	require.Len(t, frames, 1)
	require.NotNil(t, frames[0].Err)
	assert.Equal(t, ErrCodeNoGateway, frames[0].Err.Code)
}

// The scoped queue-group default lives in Serve so EVERY config source gets
// it — a scoped instance must never silently share the fleet's "mcpgw" group
// (the two would compete for the scoped user's traffic).
func TestQueueGroupDefaults(t *testing.T) {
	nc := runNATS(t, nil)
	s := serve(t, nc, ServerConfig{}, nil)
	assert.Equal(t, "mcpgw", s.queueGroup)

	s = serve(t, nc, ServerConfig{Tenant: "acme"}, nil)
	assert.Equal(t, "mcpgw.acme", s.queueGroup, "tenant-scoped instances default to their tenant group")

	s = serve(t, nc, ServerConfig{Tenant: "acme", User: "u1"}, nil)
	assert.Equal(t, "mcpgw.acme.u1", s.queueGroup, "scoped instances must default to their own group")

	s = serve(t, nc, ServerConfig{Tenant: "acme", User: "u1", QueueGroup: "custom"}, nil)
	assert.Equal(t, "custom", s.queueGroup, "an explicit group is always respected")
}

// TestNewClientRejectsAnUnusableSubjectPrefix closes the gap between the two
// prefixes a client carries.
//
// --inbox-prefix is validated by every caller; --subject-prefix was not, and
// BuildSubject checks every token EXCEPT the prefix it pastes in. A trailing
// dot or a wildcard therefore built a subject nothing serves, and the caller
// was told -32011 — "no gateway is serving this server" — about a fleet that
// is serving it fine. Validating in NewClient rather than in each subcommand
// means a future caller cannot forget it.
func TestNewClientRejectsAnUnusableSubjectPrefix(t *testing.T) {
	nc, _ := natstest.Run(t, nil)
	for _, prefix := range []string{"mcp.v1.", ".mcp.v1", "mcp..v1", "mcp.>", "mcp.*", "mcp v1"} {
		_, err := NewClient(nc, ClientConfig{Prefix: prefix, Tenant: "acme"})
		assert.Error(t, err, "prefix %q builds subjects nothing can serve", prefix)
	}
	// The ordinary cases still work, including the empty prefix that means
	// "use DefaultPrefix".
	for _, prefix := range []string{"", "mcp.v1", "acme.mcp", "a"} {
		c, err := NewClient(nc, ClientConfig{Prefix: prefix, Tenant: "acme"})
		require.NoError(t, err, "prefix %q is legitimate", prefix)
		require.NotNil(t, c)
	}
}
