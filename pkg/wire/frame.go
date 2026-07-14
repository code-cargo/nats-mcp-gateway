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
package wire

import "fmt"

// WireVersion is the value of the Mcp-Wire header. Bump on any incompatible
// change to subjects or framing.
const WireVersion = "1"

// Request headers. Mcp-Method / Mcp-Name / MCP-Protocol-Version deliberately
// mirror the names the 2026-07-28 spec requires on Streamable HTTP (they are
// binding-level strings, not MCP schema; pkg/proxy asserts they stay in sync
// with pkg/mcpspec).
const (
	HeaderWire            = "Mcp-Wire"
	HeaderMethod          = "Mcp-Method"
	HeaderName            = "Mcp-Name"
	HeaderProtocolVersion = "MCP-Protocol-Version"
)

// HeaderFrame carries the frame kind on every reply-stream message.
const HeaderFrame = "Mcp-Frame"

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
)
