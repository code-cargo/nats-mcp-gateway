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
// one place. Verified against the FINAL 2026-07-28 revision (schema commit
// f7e99af, the last before release): schema.ts plus the streamable-http,
// subscriptions and caching pages. The draft window is closed.
//
// A note for the next revision, because the last one taught it: this file was
// written expecting to be "the entire re-verification surface", and it was
// not. Two of the three things that moved after the 2026-07-13 draft were
// message SHAPES, not constants — serverInfo left DiscoverResult for result
// _meta, and subscriptions/listen gained a closure envelope — and those live
// in pkg/backend/legacy and pkg/shim. Centralizing the names does not
// centralize the structures. Re-verify those two packages as well.
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

// Request/notification _meta keys. protocolVersion and clientCapabilities are
// REQUIRED on every request in 2026-07-28 — they replace the removed
// initialize handshake. clientInfo is only SHOULD ("unless specifically
// configured not to"): it was required in the draft and made optional on
// 2026-07-16, so do NOT reintroduce enforcement for it.
const (
	MetaProtocolVersion    = "io.modelcontextprotocol/protocolVersion"
	MetaClientInfo         = "io.modelcontextprotocol/clientInfo"
	MetaClientCapabilities = "io.modelcontextprotocol/clientCapabilities"
	// MetaLogLevel is optional and deprecated as of 2026-07-28 (SEP-2577).
	MetaLogLevel = "io.modelcontextprotocol/logLevel"
	// MetaSubscriptionID tags every message on a subscriptions/listen
	// response stream — the acknowledgment, each notification, and the
	// graceful-closure response. Its value is the listen request's JSON-RPC id.
	MetaSubscriptionID = "io.modelcontextprotocol/subscriptionId"
)

// Result _meta keys. Added 2026-07-16, after this file's original
// verification pass: results gained a ResultMetaObject, and serverInfo moved
// OUT of DiscoverResult's top level into this key. A server that still
// answers server/discover with a top-level "serverInfo" is pre-final; readers
// accept both, writers emit only this.
//
// Both the _meta object and this key inside it are OPTIONAL — identifying
// yourself in a result is a SHOULD. So a result without _meta is conformant,
// and the bridge does not manufacture one; server/discover carries the
// identity, which is where a client looks for it.
const MetaServerInfo = "io.modelcontextprotocol/serverInfo"

// OpenTelemetry trace-context keys are reserved by the spec in the same _meta
// namespace table as the keys above. The gateway moves _meta opaquely, so it
// neither reads nor writes them — they are listed here so the table is the
// whole table, and so nothing else claims these names.
const (
	MetaTraceParent = "traceparent"
	MetaTraceState  = "tracestate"
	MetaBaggage     = "baggage"
)

// Result.resultType values. resultType is required in 2026-07-28; absent
// (from bridged legacy servers) MUST be read as "complete".
const (
	ResultTypeComplete      = "complete"
	ResultTypeInputRequired = "input_required"
)

// CacheableResult fields (SEP-2549). A server MUST carry both on every
// cacheable result whose resultType is "complete"; interim "input_required"
// results are not cacheable and carry neither.
const (
	// CacheScopePrivate: reusable only within one authorization context.
	CacheScopePrivate = "private"
	// CacheScopePublic: any shared cache may serve it to any caller. Only
	// correct when the result is identical for every user.
	CacheScopePublic = "public"
)

// IsCacheableMethod reports whether a method's complete results MUST carry
// ttlMs and cacheScope.
func IsCacheableMethod(method string) bool {
	switch method {
	case MethodDiscover, MethodToolsList, MethodPromptsList,
		MethodResourcesList, MethodResourcesTemplatesList, MethodResourcesRead:
		return true
	}
	return false
}

// Methods the gateway or shim must treat specially. Everything else passes
// through opaquely.
const (
	MethodDiscover      = "server/discover"
	MethodListen        = "subscriptions/listen"
	MethodToolsCall     = "tools/call"
	MethodPromptsGet    = "prompts/get"
	MethodResourcesRead = "resources/read"

	// The list methods are named only because their results are cacheable.
	MethodToolsList              = "tools/list"
	MethodPromptsList            = "prompts/list"
	MethodResourcesList          = "resources/list"
	MethodResourcesTemplatesList = "resources/templates/list"

	// Legacy (2025-11-25) methods that exist only at the bridged edges.
	MethodInitialize      = "initialize"
	MethodPing            = "ping"
	MethodLoggingSetLevel = "logging/setLevel"

	NotifInitialized = "notifications/initialized"
	NotifCancelled   = "notifications/cancelled"
	NotifProgress    = "notifications/progress"
	NotifMessage     = "notifications/message"

	// NotifSubscriptionsAcknowledged MUST be the first message a server sends
	// on a subscriptions/listen stream, before any notification on it.
	NotifSubscriptionsAcknowledged = "notifications/subscriptions/acknowledged"

	// Legacy list-changed notifications, fanned into listen streams.
	NotifToolsListChanged     = "notifications/tools/list_changed"
	NotifPromptsListChanged   = "notifications/prompts/list_changed"
	NotifResourcesListChanged = "notifications/resources/list_changed"
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
	// HeaderParamPrefix begins the headers mirrored from tool parameters
	// annotated with x-mcp-header: Mcp-Param-{Name} (SEP-2243).
	HeaderParamPrefix = "Mcp-Param-"
)

// SchemaHeaderAnnotation is the inputSchema property a server uses to mark a
// tool parameter for mirroring into an Mcp-Param-{Name} header.
const SchemaHeaderAnnotation = "x-mcp-header"
