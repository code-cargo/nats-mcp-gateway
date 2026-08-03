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

// Package wire is the NATS binding for MCP 2026-07-28: one client request in,
// a stream of zero-or-more notification frames out, exactly one terminal
// frame. It moves opaque JSON-RPC bytes and knows no MCP schema — when the
// MCP spec moves, this package does not.
//
// Two narrow exceptions, both properties of the binding rather than of MCP:
//
// Header VALUE encoding (mcpspec.EncodeHeaderValue). This binding writes tool
// names and resource URIs into NATS headers, and a value carrying CR/LF would
// corrupt the protocol frame, so the encoding is a property of writing a
// header at all. It has to live here because the subject's name token is
// derived from the RAW name — encoding earlier would change the token and with
// it the permission check.
//
// The control subject's cancellation notification (mcpspec.NotifCancelled).
// This binding defines {reply}.ctl and defines it as the cancellation channel,
// so validating what arrives there is the binding checking its own control
// plane. The alternative is cancelling on any byte that lands on a subject
// inside the caller's inbox.
package wire

import (
	"fmt"

	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// WireVersion is the value of the Mcp-Wire header. Bump on any incompatible
// change to subjects or framing.
const WireVersion = "1"

// Request headers. Mcp-Method / Mcp-Name / MCP-Protocol-Version mirror the
// names the 2026-07-28 spec requires on Streamable HTTP, so an intermediary
// can route on the NATS side exactly as it would on the HTTP side.
const (
	HeaderWire = "Mcp-Wire"
	// The MCP-defined names are aliased, not respelled. This package used to
	// keep its own copies to avoid depending on pkg/mcpspec; client.go now
	// imports it for header value encoding, so a second spelling would only
	// be a way for the two to drift — and pkg/proxy's integrity check compares
	// a header it looks up by wire's name against a value mcpspec encoded.
	HeaderMethod          = mcpspec.HeaderMethod
	HeaderName            = mcpspec.HeaderName
	HeaderProtocolVersion = mcpspec.HeaderProtocolVersion
)

// HeaderFrame carries the frame kind on every reply-stream message.
const HeaderFrame = "Mcp-Frame"

// Claim-check headers (see ClaimStore). Both sides opt in: a request carrying
// HeaderAcceptClaim "1" tells the gateway this client can dereference claims;
// an end frame carrying HeaderClaim has an EMPTY body and the real response
// parked in the claim store under the given id. The empty body would read as
// "cancelled" to a client that never opted in, which is why the gateway only
// claims for callers that advertised acceptance.
const (
	HeaderClaim       = "Mcp-Claim"
	HeaderAcceptClaim = "Mcp-Accept-Claim"
)

// claimMaxBody caps what the gateway will park in the claim store (64MiB).
// stdio backends are already line-capped at 16MiB; this bounds the otherwise
// unbounded HTTP read path. Beyond it, ErrCodePayloadTooLarge as always.
const claimMaxBody = 64 * 1024 * 1024

// FrameKind is the value of the Mcp-Frame header.
type FrameKind string

const (
	// FrameMsg carries one JSON-RPC notification. Non-terminal.
	FrameMsg FrameKind = "msg"
	// FrameEnd carries the JSON-RPC response — or an empty body, meaning the
	// request was cancelled and no response exists. Terminal.
	FrameEnd FrameKind = "end"
	// FrameErr carries a gateway-synthesized JSON-RPC error response.
	// Terminal.
	FrameErr FrameKind = "err"
	// FrameKA is an empty keepalive so an idle stream is distinguishable
	// from a dead one. Non-terminal, never surfaced to consumers.
	FrameKA FrameKind = "ka"
)

// Terminal reports whether the frame kind ends the stream.
func (k FrameKind) Terminal() bool { return k == FrameEnd || k == FrameErr }

// Frame is one message on a reply stream as surfaced to the client consumer.
// Remote frames carry Body (opaque JSON-RPC bytes). Locally-detected failures
// (no gateway, inactivity, oversize) surface as a terminal FrameErr with Err
// set and no Body — the caller owns the request id, so it synthesizes the
// JSON-RPC error itself.
type Frame struct {
	Kind FrameKind
	Body []byte
	Err  *Error
}

// Error is a wire-level failure with a JSON-RPC-compatible code.
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("wire: %d %s", e.Code, e.Message) }

// Wire-binding error codes, from the JSON-RPC implementation-defined range
// (-32000..-32019). Deliberately NOT -32001/-32002: the MCP spec retires
// -32002 (old resource-not-found) as never-reused, and crowding retired
// codes invites confusion.
const (
	// ErrCodeStreamLost: the reply stream broke (backend crash, gateway
	// drain, inactivity, slow consumer). The client must re-issue.
	ErrCodeStreamLost = -32010
	// ErrCodeNoGateway: NATS reported no responders for the subject.
	ErrCodeNoGateway = -32011
	// ErrCodePayloadTooLarge: a message exceeded the NATS max_payload.
	ErrCodePayloadTooLarge = -32012
	// ErrCodePermissionDenied: the NATS server refused the publish — the
	// caller's subject permissions do not cover this server/method/tool.
	ErrCodePermissionDenied = -32013
	// ErrCodeCredentialUnavailable: the gateway could not resolve backend
	// credentials for this (tenant, user, server). Unlike ErrCodeStreamLost,
	// re-issuing is not always the answer: the message says whether the
	// failure is transient (resolver unreachable — retry later) or
	// authoritative (the credential source said no — do not retry).
	ErrCodeCredentialUnavailable = -32014
)
