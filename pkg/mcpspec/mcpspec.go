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

// Package mcpspec holds every constant copied from the MCP specification, in
// one place: the 2026-07-28 draft was still changing as of 2026-07-13, so
// when the final revision ships this file is the entire re-verification
// surface. Verified against schema/draft/schema.ts and
// docs/specification/draft/basic/transports/streamable-http.mdx on
// 2026-07-13.
package mcpspec

// Protocol versions.
const (
	// ProtocolVersion is the primary revision the NATS wire carries, and the
	// version the shim injects and the gateway advertises by default.
	ProtocolVersion = "2026-07-28"
	// LegacyProtocolVersion is what today's real clients and servers speak;
	// bridged at the edges, never on the wire.
	LegacyProtocolVersion = "2025-11-25"
)

// SupportedProtocolVersions is the set of MCP revisions the wire accepts on a
// request. The wire is schema-agnostic — it moves opaque JSON-RPC envelopes —
// so revisions coexist: the gateway accepts any listed version and passes it
// through to the backend unchanged. As the ecosystem moves, append the new
// revision here (and keep the previous one for a window) so clients and
// servers can upgrade INDEPENDENTLY of the gateway fleet, rather than in
// lockstep. Order is newest-preferred-first; ProtocolVersion stays the head.
var SupportedProtocolVersions = []string{ProtocolVersion}

// IsSupportedProtocolVersion reports whether v is a revision the wire accepts.
func IsSupportedProtocolVersion(v string) bool {
	for _, s := range SupportedProtocolVersions {
		if v == s {
			return true
		}
	}
	return false
}

// Standard JSON-RPC codes live in pkg/jsonrpc. One spec note that matters:
// in 2026-07-28, resource-not-found is signaled with -32602 (invalid params);
// the dedicated -32002 of earlier revisions is retired and never reused.
//
// MCP-defined error codes. The spec reserves -32020..-32099, allocated
// sequentially; -32000..-32019 is the implementation-defined range (the wire
// binding's own codes live there — see pkg/wire).
const (
	// ErrHeaderMismatch: transport headers disagree with the body, or
	// required headers are missing/malformed. No data shape.
	ErrHeaderMismatch = -32020
	// ErrMissingRequiredClientCapability: data = {requiredCapabilities}.
	ErrMissingRequiredClientCapability = -32021
	// ErrUnsupportedProtocolVersion: data = {supported []string, requested string}.
	ErrUnsupportedProtocolVersion = -32022
)

// Request/notification _meta keys. The first three are REQUIRED on every
// request in 2026-07-28 — they replace the removed initialize handshake.
const (
	MetaProtocolVersion    = "io.modelcontextprotocol/protocolVersion"
	MetaClientInfo         = "io.modelcontextprotocol/clientInfo"
	MetaClientCapabilities = "io.modelcontextprotocol/clientCapabilities"
	// MetaLogLevel is optional and deprecated as of 2026-07-28 (SEP-2577).
	MetaLogLevel = "io.modelcontextprotocol/logLevel"
	// MetaSubscriptionID tags notifications emitted on a
	// subscriptions/listen response stream.
	MetaSubscriptionID = "io.modelcontextprotocol/subscriptionId"
)

// Result.resultType values. resultType is required in 2026-07-28; absent
// (from bridged legacy servers) MUST be read as "complete".
const (
	ResultTypeComplete      = "complete"
	ResultTypeInputRequired = "input_required"
)

// Methods the gateway or shim must treat specially. Everything else passes
// through opaquely.
const (
	MethodDiscover      = "server/discover"
	MethodListen        = "subscriptions/listen"
	MethodToolsCall     = "tools/call"
	MethodPromptsGet    = "prompts/get"
	MethodResourcesRead = "resources/read"

	// Legacy (2025-11-25) methods that exist only at the bridged edges.
	MethodInitialize      = "initialize"
	MethodPing            = "ping"
	MethodLoggingSetLevel = "logging/setLevel"

	NotifInitialized = "notifications/initialized"
	NotifCancelled   = "notifications/cancelled"
	NotifProgress    = "notifications/progress"
	NotifMessage     = "notifications/message"
)

// Streamable HTTP header names (also mirrored on the NATS wire). Note the
// casing: MCP is all-caps only on the protocol-version header.
const (
	HeaderProtocolVersion = "MCP-Protocol-Version"
	// HeaderMethod mirrors the body's method; required on all requests.
	HeaderMethod = "Mcp-Method"
	// HeaderName mirrors params.name (tools/call, prompts/get) or
	// params.uri (resources/read); required for exactly those methods.
	HeaderName = "Mcp-Name"
)
