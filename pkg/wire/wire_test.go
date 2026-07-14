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
	subj, err := BuildSubject("", "acme", "test", "tools/list", "")
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
