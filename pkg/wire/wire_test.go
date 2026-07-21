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
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
)

// runNATS starts an embedded nats-server on a random port.
func runNATS(t *testing.T, opts *server.Options) *nats.Conn {
	t.Helper()
	if opts == nil {
		opts = &server.Options{}
	}
	opts.Host = "127.0.0.1"
	opts.Port = -1
	// Silence logging; keep JetStream off.
	opts.NoLog = true
	opts.NoSigs = true
	if opts.MaxPayload == 0 {
		opts.MaxPayload = 8 * 1024 * 1024 // production-recommended size (see README)
	}
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second), "embedded nats-server did not start")
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
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

func TestServeRejectsHalfScoping(t *testing.T) {
	nc := runNATS(t, nil)
	_, err := Serve(nc, ServerConfig{Tenant: "acme", Servers: []string{"test"}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both Tenant and User")
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
