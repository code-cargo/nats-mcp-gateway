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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsNATS runs an embedded NATS with JetStream on and a small max_payload so
// tests can trip the oversize path with fast, small bodies.
func jsNATS(t *testing.T, maxPayload int32) *nats.Conn {
	t.Helper()
	return runNATS(t, &server.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		MaxPayload: maxPayload,
	})
}

// bigEndBody builds a JSON-RPC response comfortably over a 64KB max_payload.
func bigEndBody() []byte {
	return []byte(`{"jsonrpc":"2.0","id":"1","result":{"blob":"` + strings.Repeat("x", 200*1024) + `"}}`)
}

func TestObjectClaimsPutFetch(t *testing.T) {
	nc := jsNATS(t, 0)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	oc := &ObjectClaims{JS: js, MaxAge: time.Minute}
	ctx := context.Background()

	body := bytes.Repeat([]byte("payload"), 50_000) // ~350KB, multiple chunks
	id, err := oc.Put(ctx, "acme", body)
	require.NoError(t, err)
	require.Len(t, id, 32)

	id2, err := oc.Put(ctx, "acme", []byte("other"))
	require.NoError(t, err)
	assert.NotEqual(t, id, id2, "claim ids must be unique")

	got, err := oc.Fetch(ctx, "acme", id)
	require.NoError(t, err)
	assert.Equal(t, body, got)

	// Fetch deletes eagerly: the object is gone.
	_, err = oc.Fetch(ctx, "acme", id)
	require.Error(t, err, "a fetched claim must be deleted")

	// The bucket carries the TTL backstop and the tenant fence in its name.
	obs, err := js.ObjectStore(ctx, "MCP_CLAIMS_acme")
	require.NoError(t, err)
	status, err := obs.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, status.TTL())

	// Malformed references are rejected before touching the store.
	_, err = oc.Fetch(ctx, "bad tenant", id2)
	require.Error(t, err)
	_, err = oc.Fetch(ctx, "acme", "../escape")
	require.Error(t, err)
}

func TestClaimRoundTripOverWire(t *testing.T) {
	nc := jsNATS(t, 64*1024)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	claims := &ObjectClaims{JS: js}
	big := bigEndBody()

	serve(t, nc, ServerConfig{Claims: claims}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return w.End(big)
	})

	// Opted-in client: the oversize body arrives transparently as a normal
	// end frame.
	c, err := NewClient(nc, ClientConfig{Tenant: "acme", Claims: claims, Inactivity: 5 * time.Second})
	require.NoError(t, err)
	s, err := c.Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1)
	require.Equal(t, FrameEnd, frames[0].Kind)
	assert.Equal(t, big, frames[0].Body)

	// The object was deleted by the successful fetch.
	obs, err := js.ObjectStore(context.Background(), "MCP_CLAIMS_acme")
	require.NoError(t, err)
	objs, err := obs.List(context.Background())
	if err == nil {
		assert.Empty(t, objs, "claimed object must be deleted after fetch")
	} else {
		assert.ErrorIs(t, err, jetstream.ErrNoObjectsFound)
	}

	// A client that did NOT opt in still gets today's legible -32012.
	plain := client(t, nc, 5*time.Second)
	s, err = plain.Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames = collect(t, s)
	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	assert.Contains(t, string(frames[0].Body), fmt.Sprint(ErrCodePayloadTooLarge))
}

func TestClaimFallbackWhenJetStreamDisabled(t *testing.T) {
	// JetStream OFF: Put fails, and the gateway must degrade to -32012 —
	// never a hang, never a claim frame pointing nowhere.
	nc := runNATS(t, &server.Options{MaxPayload: 64 * 1024})
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	claims := &ObjectClaims{JS: js}

	serve(t, nc, ServerConfig{Claims: claims}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return w.End(bigEndBody())
	})

	c, err := NewClient(nc, ClientConfig{Tenant: "acme", Claims: claims, Inactivity: 5 * time.Second})
	require.NoError(t, err)
	s, err := c.Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	assert.Contains(t, string(frames[0].Body), fmt.Sprint(ErrCodePayloadTooLarge))
}

// failingClaims scripts ClaimStore behavior for failure-path tests.
type failingClaims struct {
	putID    string
	fetchErr error
}

func (f *failingClaims) Put(context.Context, string, []byte) (string, error) {
	return f.putID, nil
}

func (f *failingClaims) Fetch(context.Context, string, string) ([]byte, error) {
	return nil, f.fetchErr
}

func TestClaimFetchFailureIsStreamLost(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: 64 * 1024})
	serve(t, nc, ServerConfig{Claims: &failingClaims{putID: "deadbeef"}},
		func(ctx context.Context, in *Inbound, w StreamWriter) error {
			return w.End(bigEndBody())
		})

	c, err := NewClient(nc, ClientConfig{
		Tenant: "acme", Inactivity: 5 * time.Second,
		Claims: &failingClaims{fetchErr: errors.New("bucket gone")},
	})
	require.NoError(t, err)
	s, err := c.Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	require.NotNil(t, frames[0].Err)
	assert.Equal(t, ErrCodeStreamLost, frames[0].Err.Code)
	assert.Contains(t, frames[0].Err.Message, "re-issue")
}

func TestClaimToNonAcceptingClientIsRejected(t *testing.T) {
	// A misbehaving gateway sends a claim frame to a client that never opted
	// in. The empty body must NOT be misread as "cancelled" — the client
	// detects the claim header and fails legibly.
	nc := runNATS(t, nil)
	sub, err := nc.Subscribe("mcp.v1.req.acme._.test.>", func(m *nats.Msg) {
		_ = m.RespondMsg(&nats.Msg{Header: nats.Header{
			HeaderFrame: []string{string(FrameEnd)},
			HeaderClaim: []string{"deadbeef"},
		}})
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	c := client(t, nc, 2*time.Second) // no Claims configured
	s, err := c.Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	require.NotNil(t, frames[0].Err)
	assert.Contains(t, frames[0].Err.Message, "did not accept")
}
