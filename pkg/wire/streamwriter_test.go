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
	"sync"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRequest is a micro.Request that publishes exactly where the real one
// does — the same connection, the same reply subject — so a raw subscriber
// observes the true order the frames reached the wire.
type fakeRequest struct {
	nc    *nats.Conn
	reply string
}

func (r *fakeRequest) publish(body []byte, opts []micro.RespondOpt) error {
	msg := &nats.Msg{Subject: r.reply, Data: body, Header: nats.Header{}}
	for _, o := range opts {
		o(msg)
	}
	return r.nc.PublishMsg(msg)
}

func (r *fakeRequest) Respond(body []byte, opts ...micro.RespondOpt) error {
	return r.publish(body, opts)
}

func (r *fakeRequest) Error(_, _ string, body []byte, opts ...micro.RespondOpt) error {
	return r.publish(body, opts)
}

func (r *fakeRequest) RespondJSON(any, ...micro.RespondOpt) error { return nil }

func (r *fakeRequest) Data() []byte { return nil }

func (r *fakeRequest) Headers() micro.Headers { return nil }

func (r *fakeRequest) Subject() string { return "" }

func (r *fakeRequest) Reply() string { return r.reply }

// A notification frame must never land behind the terminal frame. The consumer
// returns at the terminal and unsubscribes, so a msg frame published after it
// is a notification no client can ever read, on a request the gateway has
// already answered — the "exactly one response, then nothing" contract this
// package advertises.
//
// The proxy reaches this window for real. pkg/backend's mux hands a response
// to the blocked Call with a non-blocking channel send and immediately reads
// the next message; the caller's entry is not unregistered until Call wakes
// up. A backend notification arriving inside that window is dispatched to
// w.Msg on the read-loop goroutine while the proxy handler is already in
// w.End.
func TestNotificationNeverFollowsTerminalFrame(t *testing.T) {
	nc := runNATS(t, nil)

	const iterations = 500
	late := 0
	for i := 0; i < iterations; i++ {
		reply := nc.NewRespInbox()
		sub, err := nc.SubscribeSync(reply)
		require.NoError(t, err)

		w := &streamWriter{nc: nc, req: &fakeRequest{nc: nc, reply: reply}}

		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		go func() {
			defer wg.Done()
			<-start
			_ = w.Msg([]byte(`{"jsonrpc":"2.0","method":"notifications/progress"}`))
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
		}()
		close(start)
		wg.Wait()
		// Publishes on one connection arrive in publish order, so a sentinel
		// sent last marks the end of this iteration's traffic — draining on a
		// timeout instead would cost a wait per iteration.
		require.NoError(t, nc.PublishMsg(&nats.Msg{
			Subject: reply,
			Header:  nats.Header{HeaderFrame: []string{"sentinel"}},
		}))

		terminal := false
		for {
			msg, err := sub.NextMsg(5 * time.Second)
			require.NoError(t, err)
			kind := FrameKind(msg.Header.Get(HeaderFrame))
			if kind == "sentinel" {
				break
			}
			if terminal && kind == FrameMsg {
				late++
			}
			if kind.Terminal() {
				terminal = true
			}
		}
		require.NoError(t, sub.Unsubscribe())
	}

	assert.Zero(t, late, "notification frames were published after the terminal frame")
}

// Once terminal, Msg reports the stream is over rather than publishing.
func TestMsgAfterTerminalIsRefused(t *testing.T) {
	nc := runNATS(t, nil)
	reply := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(reply)
	require.NoError(t, err)

	w := &streamWriter{nc: nc, req: &fakeRequest{nc: nc, reply: reply}}
	require.NoError(t, w.Msg([]byte(`{"jsonrpc":"2.0","method":"notifications/progress"}`)))
	require.NoError(t, w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`)))
	require.Error(t, w.Msg([]byte(`{"jsonrpc":"2.0","method":"notifications/progress"}`)))
	require.NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: reply,
		Header:  nats.Header{HeaderFrame: []string{"sentinel"}},
	}))

	var kinds []FrameKind
	for {
		msg, err := sub.NextMsg(5 * time.Second)
		require.NoError(t, err)
		kind := FrameKind(msg.Header.Get(HeaderFrame))
		if kind == "sentinel" {
			break
		}
		kinds = append(kinds, kind)
	}
	assert.Equal(t, []FrameKind{FrameMsg, FrameEnd}, kinds)
}
