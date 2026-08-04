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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
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

func TestOversizeHandlerErrorStillTerminates(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	// An err frame spends max_payload on the message twice, so two thirds of
	// the limit is already more than the whole frame can hold.
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return errors.New(strings.Repeat("e", boundaryMaxPayload*2/3))
	})

	const inactivity = 3 * time.Second
	start := time.Now()
	s, err := client(t, nc, inactivity).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)

	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	require.Nil(t, frames[0].Err, "the gateway must still be able to report its own failure")
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, jsonrpc.CodeInternalError, m.Error.Code)
	assert.Contains(t, m.Error.Message, "error detail dropped")
	assert.Equal(t, `"1"`, m.IDKey(), "the id is what the caller correlates on; it survives")
	assert.Less(t, time.Since(start), inactivity,
		"the failure must be reported, not waited out on the inactivity deadline")
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

// The tests above all pin the reject side. Those below pin the other edge,
// which is the one a "safe" refactor breaks silently: rounding the measured
// overhead up to a comfortable constant would still pass every test above
// while quietly diverting legal responses into claim-check or an error frame.
// A body of exactly max_payload minus the frame's headers is the largest thing
// this wire can carry, and it has to go out whole.
//
// So these spell the header serialization out rather than calling
// frameOverhead: a test that sizes its body from the very constant it is
// guarding shrinks in lockstep with the bug and proves only that the code
// agrees with itself. Hardcoding here is the point — production measures so it
// is never wrong, the test asserts so a wrong measurement is loud.
func wireHeaderBytes(kv ...string) int {
	n := len("NATS/1.0\r\n") + len("\r\n")
	for i := 0; i < len(kv); i += 2 {
		n += len(kv[i]) + len(": ") + len(kv[i+1]) + len("\r\n")
	}
	return n
}

func TestFrameOverheadMatchesTheWire(t *testing.T) {
	assert.EqualValues(t, wireHeaderBytes(HeaderFrame, string(FrameEnd)), endOverhead)
	assert.EqualValues(t, wireHeaderBytes(HeaderFrame, string(FrameMsg)), msgOverhead)
	assert.EqualValues(t,
		wireHeaderBytes(HeaderFrame, string(FrameErr),
			micro.ErrorHeader, "boom", micro.ErrorCodeHeader, "-32603"),
		errOverhead(-32603, "boom"),
		"micro copies the message into Nats-Service-Error, so an err frame pays for it twice")
}

func TestEndJustUnderBoundaryPublishesWhole(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	body := bytes.Repeat([]byte("x"),
		boundaryMaxPayload-wireHeaderBytes(HeaderFrame, string(FrameEnd)))
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return w.End(body)
	})

	s, err := client(t, nc, 3*time.Second).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)

	require.Len(t, frames, 1)
	require.Equal(t, FrameEnd, frames[0].Kind,
		"an exact fit must not be pushed into the oversize branch")
	assert.Equal(t, body, frames[0].Body)
}

func TestNotificationJustUnderBoundaryPublishes(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	msgErr := make(chan error, 1)
	body := bytes.Repeat([]byte("x"),
		boundaryMaxPayload-wireHeaderBytes(HeaderFrame, string(FrameMsg)))
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		msgErr <- w.Msg(body)
		return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
	})

	s, err := client(t, nc, 3*time.Second).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)

	require.NoError(t, <-msgErr, "an exact fit must not be dropped")
	require.Len(t, frames, 2)
	assert.Equal(t, FrameMsg, frames[0].Kind)
	assert.Equal(t, body, frames[0].Body)
}

// Sizing a request after its headers exist is only worth the reordering if the
// headers are actually caller-shaped. They are: Mcp-Name carries the tool name
// or resource URI verbatim, so the same body is sendable under one name and
// refused under another.
func TestRequestCeilingMovesWithToolName(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	c := client(t, nc, time.Second)

	// The header set Do builds for a request that carries no name.
	unnamed := wireHeaderBytes(
		HeaderWire, WireVersion,
		HeaderMethod, "tools/call",
		HeaderProtocolVersion, "2026-07-28",
	)

	req := testRequest("1", "tools/call")
	req.Body = bytes.Repeat([]byte("x"), boundaryMaxPayload-unnamed)
	s, err := c.Do(context.Background(), req)
	require.NoError(t, err, "an exact fit for the unnamed header set must be accepted")
	s.Close()

	req.Name = strings.Repeat("n", 200)
	_, err = c.Do(context.Background(), req)
	var werr *Error
	require.ErrorAs(t, err, &werr,
		"the same body must be refused once Mcp-Name takes part of the budget")
	assert.Equal(t, ErrCodePayloadTooLarge, werr.Code)
}

// Error data is supplementary; the message is what the caller reads. When the
// frame will not hold both, data goes first — and says so, because an error
// that silently arrives smaller than the one that happened sends the caller
// debugging the wire instead of their request.
func TestOversizeErrorDataIsShedBeforeMessage(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return w.Err(jsonrpc.CodeInvalidParams, "params rejected",
			map[string]string{"echo": strings.Repeat("d", boundaryMaxPayload)})
	})

	s, err := client(t, nc, 3*time.Second).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)

	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	require.Nil(t, frames[0].Err, "the gateway must send its own error frame")
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, jsonrpc.CodeInvalidParams, m.Error.Code, "the code the handler chose survives")
	assert.Contains(t, m.Error.Message, "params rejected", "so does the message")
	assert.Contains(t, m.Error.Message, "error data omitted", "and the loss is stated")
	assert.Empty(t, m.Error.Data)
	assert.Equal(t, `"1"`, m.IDKey())
}

// A JSON-RPC id large enough to fill the err frame by itself cannot be
// answered: shedding the id would produce a frame nobody can route, since the
// shim writes this body through to a client that correlates on the id and
// nothing else. The stream is left to end in the inactivity timeout instead,
// which the shim DOES report against the right id — slow and correct rather
// than prompt and uncorrelatable. This pins that as a decision, not an
// oversight; a frame arriving here with a null id is a regression.
func TestErrFrameNeverShedsTheID(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return errors.New("backend refused")
	})

	// The largest request this wire will carry, spent almost entirely on the id.
	unnamed := wireHeaderBytes(
		HeaderWire, WireVersion,
		HeaderMethod, "tools/call",
		HeaderProtocolVersion, "2026-07-28",
	)
	const envelope = `{"jsonrpc":"2.0","id":"","method":"tools/call"}`
	id := strings.Repeat("i", boundaryMaxPayload-unnamed-len(envelope))
	req := testRequest("1", "tools/call")
	req.Body = []byte(`{"jsonrpc":"2.0","id":"` + id + `","method":"tools/call"}`)

	const inactivity = 500 * time.Millisecond
	s, err := client(t, nc, inactivity).Do(context.Background(), req)
	require.NoError(t, err, "the request itself fits; only the answer does not")
	frames := collect(t, s)

	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	require.NotNil(t, frames[0].Err,
		"nothing reached the wire, so the failure is the client's to synthesize")
	assert.Equal(t, ErrCodeStreamLost, frames[0].Err.Code)
	assert.Empty(t, frames[0].Body, "a null-id body would be worse than none")
}

func TestOversizeCancelIsTypedTooLarge(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	// No gateway is served here: Cancel's guard is client-side and runs ahead
	// of the publish, so the stream only has to exist.
	s, err := client(t, nc, time.Second).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	defer s.Close()

	// The control frame is the one publish on this wire that carries no
	// headers, so max_payload exactly is still legal and the first illegal
	// size is one byte past it.
	require.NoError(t, s.Cancel(boundaryBody()), "a headerless publish gets the whole budget")

	var werr *Error
	require.ErrorAs(t, s.Cancel(bytes.Repeat([]byte("x"), boundaryMaxPayload+1)), &werr,
		"the last unchecked publish must not surface as a bare nats.ErrMaxPayload")
	assert.Equal(t, ErrCodePayloadTooLarge, werr.Code)
}

// micro.Request.Error refuses an empty description with ErrArgRequired, and by
// then errWithID has claimed the stream: the terminal frame never goes out and
// the caller is left on the same inactivity deadline. A backend returning a
// bare errors.New("") is all it takes.
func TestEmptyHandlerErrorStillTerminates(t *testing.T) {
	nc := runNATS(t, &server.Options{MaxPayload: boundaryMaxPayload})
	serve(t, nc, ServerConfig{}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		return errors.New("")
	})

	const inactivity = 3 * time.Second
	start := time.Now()
	s, err := client(t, nc, inactivity).Do(context.Background(), testRequest("1", "tools/call"))
	require.NoError(t, err)
	frames := collect(t, s)

	require.Len(t, frames, 1)
	require.Equal(t, FrameErr, frames[0].Kind)
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.NotEmpty(t, m.Error.Message, "an empty description is what micro refuses to publish")
	assert.Less(t, time.Since(start), inactivity,
		"the failure must be reported, not waited out on the inactivity deadline")
}
