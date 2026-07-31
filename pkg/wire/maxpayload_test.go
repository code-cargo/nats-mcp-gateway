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
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
)

// NATS weighs a message as body + serialized headers, so the sizes worth
// testing are not the obviously-oversize ones but the band immediately below
// max_payload, where a body that would fit on its own becomes a message the
// server refuses. Every one of these bodies is exactly max_payload: legal as a
// payload, illegal as a frame.
const boundaryMaxPayload = 64 * 1024

func boundaryBody() []byte { return bytes.Repeat([]byte("x"), boundaryMaxPayload) }

func TestEndAtPayloadBoundaryReportsTooLarge(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return w.End(boundaryBody())
	})

	const inactivity = 3 * time.Second
	start := time.Now()
	s, err := client(t, nc, inactivity).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)

	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	require.Nil(t, frames[0].Err,
		"the gateway must send its own error frame; a locally-synthesized one means it sent nothing")
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, ErrCodePayloadTooLarge, m.Error.Code)
	assert.Less(t, time.Since(start), inactivity,
		"the size must be reported, not waited out on the inactivity deadline")
}

func TestClaimCheckFiresAtPayloadBoundary(t *testing.T) {
	nc := jsNATS(t, boundaryMaxPayload)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	claims := &ObjectClaims{JS: js}
	body := boundaryBody()

	serve(t, nc, ServerConfig{Claims: claims}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return w.End(body)
	})

	c, err := NewClient(nc, ClientConfig{Tenant: "acme", Claims: claims, Inactivity: 3 * time.Second})
	require.NoError(t, err)
	s, err := c.Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)

	require.Len(t, frames, 1)
	require.Equal(t, FrameEnd, frames[0].Kind,
		"a body that does not fit the wire is exactly what claim-check exists for")
	assert.Equal(t, body, frames[0].Body)
}

func TestRequestAtPayloadBoundaryIsTypedTooLarge(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	req := testRequest("1", "tools/call")
	req.Body = boundaryBody()

	_, err := client(t, nc, 0).Do(context.Background(), req)
	require.Error(t, err)
	var werr *Error
	require.ErrorAs(t, err, &werr,
		"callers switch on the code, so an untyped publish failure is a miscategorized one")
	assert.Equal(t, ErrCodePayloadTooLarge, werr.Code)
}

func TestNotificationAtPayloadBoundaryIsTypedTooLarge(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	msgErr := make(chan error, 1)
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		msgErr <- w.Msg(boundaryBody())
		return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
	})

	s, err := client(t, nc, 3*time.Second).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)
	require.Len(t, frames, 1, "an oversize notification is dropped, never published")
	assert.Equal(t, FrameEnd, frames[0].Kind)

	var werr *Error
	require.ErrorAs(t, <-msgErr, &werr)
	assert.Equal(t, ErrCodePayloadTooLarge, werr.Code)
}
