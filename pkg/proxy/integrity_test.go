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

package proxy

import (
	"encoding/json"
	"fmt"
	"testing"

	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// TestHeaderNamesMatchSpec pins the wire's header strings to the spec's.
func TestHeaderNamesMatchSpec(t *testing.T) {
	assert.Equal(t, mcpspec.HeaderMethod, wire.HeaderMethod)
	assert.Equal(t, mcpspec.HeaderName, wire.HeaderName)
	assert.Equal(t, mcpspec.HeaderProtocolVersion, wire.HeaderProtocolVersion)
}

// inbound builds a wire.Inbound the way the wire server would, letting each
// test case override the parts under attack.
func inbound(t *testing.T, subject, method, name, body string) *wire.Inbound {
	t.Helper()
	msg, err := jsonrpc.Decode([]byte(body))
	require.NoError(t, err)
	parsed, err := wire.ParseSubject(subject, "")
	require.NoError(t, err)
	h := nats.Header{
		wire.HeaderWire:            []string{wire.WireVersion},
		wire.HeaderMethod:          []string{method},
		wire.HeaderProtocolVersion: []string{mcpspec.ProtocolVersion},
	}
	if name != "" {
		h.Set(wire.HeaderName, name)
	}
	return &wire.Inbound{Subject: *parsed, Header: h, Body: []byte(body), Msg: msg}
}

func body(method, name string, meta bool) string {
	params := "{"
	if name != "" {
		field := "name"
		if method == "resources/read" {
			field = "uri"
		}
		params += fmt.Sprintf("%q:%q,", field, name)
	}
	if meta {
		params += fmt.Sprintf(`"_meta":{%q:%q}`, mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion)
	} else {
		params += `"_meta":{}`
	}
	params += "}"
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"1","method":%q,"params":%s}`, method, params)
}

// TestIntegrityMatrix is the security test: every way subject, headers, and
// body can disagree must be rejected.
func TestIntegrityMatrix(t *testing.T) {
	tests := []struct {
		name     string
		subject  string
		hMethod  string // Mcp-Method header
		hName    string // Mcp-Name header
		body     string
		wantCode int // 0 = pass
	}{
		{
			name:    "tools/call all consistent",
			subject: "mcp.v1.req.acme.u1.gh.tools.call.get_issue",
			hMethod: "tools/call", hName: "get_issue",
			body: body("tools/call", "get_issue", true),
		},
		{
			name:    "tools/list all consistent",
			subject: "mcp.v1.req.acme.u1.gh.tools.list._",
			hMethod: "tools/list",
			body:    body("tools/list", "", true),
		},
		{
			name:    "resources/read uri on underscore subject",
			subject: "mcp.v1.req.acme.u1.gh.resources.read._",
			hMethod: "resources/read", hName: "file:///x/y",
			body: body("resources/read", "file:///x/y", true),
		},
		{
			name:    "THE attack: authorized subject, different body method",
			subject: "mcp.v1.req.acme.u1.gh.tools.list._",
			hMethod: "tools/list",
			body:    body("tools/call", "delete_repo", true),
			// header matches subject but not body
			wantCode: mcpspec.ErrHeaderMismatch,
		},
		{
			name:    "THE attack v2: authorized tool subject, different body tool",
			subject: "mcp.v1.req.acme.u1.gh.tools.call.get_issue",
			hMethod: "tools/call", hName: "get_issue",
			body:     body("tools/call", "delete_repo", true),
			wantCode: mcpspec.ErrHeaderMismatch,
		},
		{
			name:    "ACL dodge: token-safe name published to underscore subject",
			subject: "mcp.v1.req.acme.u1.gh.tools.call._",
			hMethod: "tools/call", hName: "delete_repo",
			body:     body("tools/call", "delete_repo", true),
			wantCode: mcpspec.ErrHeaderMismatch,
		},
		{
			name:    "unsafe name legitimately on underscore subject",
			subject: "mcp.v1.req.acme.u1.gh.tools.call._",
			hMethod: "tools/call", hName: "crème.brûlée",
			body: body("tools/call", "crème.brûlée", true),
		},
		{
			name:    "header method differs from body",
			subject: "mcp.v1.req.acme.u1.gh.tools.call.get_issue",
			hMethod: "tools/list", hName: "get_issue",
			body:     body("tools/call", "get_issue", true),
			wantCode: mcpspec.ErrHeaderMismatch,
		},
		{
			name:    "header name differs from body name",
			subject: "mcp.v1.req.acme.u1.gh.tools.call.get_issue",
			hMethod: "tools/call", hName: "other_tool",
			body:     body("tools/call", "get_issue", true),
			wantCode: mcpspec.ErrHeaderMismatch,
		},
		{
			name:     "named method missing name in body",
			subject:  "mcp.v1.req.acme.u1.gh.tools.call._",
			hMethod:  "tools/call",
			body:     `{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}`,
			wantCode: mcpspec.ErrHeaderMismatch,
		},
		{
			name:     "missing protocol version in _meta",
			subject:  "mcp.v1.req.acme.u1.gh.tools.list._",
			hMethod:  "tools/list",
			body:     body("tools/list", "", false),
			wantCode: mcpspec.ErrHeaderMismatch, // header says 2026-07-28, body says ""
		},
		{
			name:     "notification is not a request",
			subject:  "mcp.v1.req.acme.u1.gh.tools.list._",
			hMethod:  "tools/list",
			body:     `{"jsonrpc":"2.0","method":"tools/list","params":{}}`,
			wantCode: jsonrpc.CodeInvalidRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := inbound(t, tt.subject, tt.hMethod, tt.hName, tt.body)
			_, got := Check(in)
			if tt.wantCode == 0 {
				assert.Nil(t, got, "expected pass, got %v", got)
			} else {
				require.NotNil(t, got, "expected rejection")
				assert.Equal(t, tt.wantCode, got.Code)
			}
		})
	}
}

// metaJSON is the well-formed _meta fragment every request must carry.
func metaJSON() string {
	return fmt.Sprintf(`"_meta":{%q:%q}`, mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion)
}

// rawBody builds a request whose params are given verbatim, so a case can
// craft JSON that no serializer would produce. The matrix above builds every
// fixture through body(), which is exactly why it tested the check's LOGIC
// and never its PARSER.
func rawBody(method, params string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"1","method":%q,"params":%s}`, method, params)
}

// TestIntegrityRejectsKeySmuggling covers the bypass class where the gateway
// and the backend read different values out of the same bytes.
//
// The gateway forwards params verbatim, so any disagreement between the
// parser here and the parser at the far end is an authorization bypass. Two
// disagreements were live: encoding/json matches struct fields
// case-INSENSITIVELY, so {"name":"delete_repo","NAME":"get_issue"} probed to
// "get_issue" while a case-sensitive backend ran delete_repo; and a
// map-typed _meta field MERGED a forged _META into the real one, passing the
// protocol-version gate on a version the backend never sees.
//
// Every case here is a body that means one thing to some real parser and
// something else to another. None of them is authorizable.
func TestIntegrityRejectsKeySmuggling(t *testing.T) {
	m := metaJSON()
	const (
		callGetIssue = "mcp.v1.req.acme.u1.gh.tools.call.get_issue"
		callUnset    = "mcp.v1.req.acme.u1.gh.tools.call._"
		readUnset    = "mcp.v1.req.acme.u1.gh.resources.read._"
		listUnset    = "mcp.v1.req.acme.u1.gh.tools.list._"
	)
	tests := []struct {
		name    string
		subject string
		hMethod string
		hName   string
		params  string
	}{
		{
			// THE exploit: a caller holding only tools.call.get_issue reaches
			// delete_repo. Everything agrees on the forged NAME; the bytes the
			// backend parses say otherwise.
			name:    "case-smuggled tool name, exact key first",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: `{"name":"delete_repo","NAME":"get_issue",` + m + `}`,
		},
		{
			name:    "case-smuggled tool name, exact key last",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: `{"NAME":"get_issue","name":"delete_repo",` + m + `}`,
		},
		{
			name:    "case-smuggled tool name, mixed case",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: `{"name":"delete_repo","NaMe":"get_issue",` + m + `}`,
		},
		{
			// The inverse, and the reason exact-key reads alone are not the
			// whole fix: this gateway reads get_issue, but a backend whose
			// parser folds case (System.Text.Json under ASP.NET Core, by
			// default) reads delete_repo.
			name:    "case-smuggled tool name aimed at a case-folding backend",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: `{"name":"get_issue","NAME":"delete_repo",` + m + `}`,
		},
		{
			// The "_" slot the README claims is closed: a token-safe name
			// hiding under the unnamed-method subject.
			name:    "case-smuggled name hiding under the underscore token",
			subject: callUnset, hMethod: "tools/call", hName: "_",
			params: `{"name":"delete_repo","Name":"_",` + m + `}`,
		},
		{
			name:    "case-smuggled resource uri",
			subject: readUnset, hMethod: "resources/read", hName: "file:///allowed",
			params: `{"uri":"file:///etc/shadow","URI":"file:///allowed",` + m + `}`,
		},
		{
			name:    "case-smuggled prompt name",
			subject: "mcp.v1.req.acme.u1.gh.prompts.get.safe", hMethod: "prompts/get", hName: "safe",
			params: `{"name":"privileged","NAME":"safe",` + m + `}`,
		},
		{
			// Go, JavaScript, Python and Jackson all take the last duplicate,
			// so this body reads as "b" here. It is still refused: RFC 8259
			// leaves the resolution undefined and the gateway did not choose
			// the parser on the other end.
			name:    "duplicate name key, authorized as the last value",
			subject: "mcp.v1.req.acme.u1.gh.tools.call.b", hMethod: "tools/call", hName: "b",
			params: `{"name":"a","name":"b",` + m + `}`,
		},
		{
			name:    "duplicate name key, authorized as the first value",
			subject: "mcp.v1.req.acme.u1.gh.tools.call.a", hMethod: "tools/call", hName: "a",
			params: `{"name":"a","name":"b",` + m + `}`,
		},
		{
			// _META and _meta MERGE into one Go map rather than shadowing, so
			// the version gate saw 2026-07-28 while the backend's _meta was
			// empty — a request with no protocol version at all, forwarded.
			name:    "case-smuggled protocol version, real _meta empty",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: fmt.Sprintf(`{"name":"get_issue","_META":{%q:%q},"_meta":{}}`,
				mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion),
		},
		{
			// progressToken is read and REWRITTEN in pkg/backend.Mux.Call, far
			// past the point where a request can still be refused, and the
			// rewrite leaves a case-variant sibling untouched. Caught here or
			// not at all.
			name:    "case-colliding progressToken inside _meta",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: fmt.Sprintf(`{"name":"get_issue","_meta":{%q:%q,"progressToken":"mine","ProgressToken":"gt7"}}`,
				mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion),
		},
		{
			name:    "duplicate protocol version inside _meta",
			subject: listUnset, hMethod: "tools/list",
			params: fmt.Sprintf(`{"_meta":{%q:%q,%q:"1999-01-01"}}`,
				mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion, mcpspec.MetaProtocolVersion),
		},
		{
			// Nested, because arguments are mirrored into Mcp-Param-* headers
			// for HTTP backends. Ambiguity anywhere is ambiguity in something
			// the gateway acts on.
			name:    "duplicate key nested in arguments",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: `{"name":"get_issue","arguments":{"repo":"safe","repo":"evil"},` + m + `}`,
		},
		{
			name:    "duplicate key nested inside an array element",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: `{"name":"get_issue","arguments":{"xs":[{"k":1,"k":2}]},` + m + `}`,
		},
		{
			name:    "duplicate arguments key",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: `{"name":"get_issue","arguments":{"a":1},"arguments":{"a":2},` + m + `}`,
		},
		{
			name:    "case-colliding arguments key at params top level",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: `{"name":"get_issue","arguments":{"a":1},"ARGUMENTS":{"a":2},` + m + `}`,
		},
		{
			// U+017F lowercases to itself but FOLDS to "s", so a rule written
			// with strings.ToLower reads this as a distinct key while a
			// case-folding parser binds it to arguments. Unicode folding is
			// the only definition that closes it.
			name:    "arguments smuggled past lowercasing with U+017F",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: `{"name":"get_issue","arguments":{"region":"us"},"argumentſ":{"region":"eu"},` + m + `}`,
		},
		{
			// The dotted/dotless I family shares no Unicode fold orbit, so a
			// folding-only rule reads these as distinct keys while .NET's
			// OrdinalIgnoreCase (the ASP.NET Core default) binds them to uri.
			name:    "resource uri smuggled past folding with U+0131",
			subject: readUnset, hMethod: "resources/read", hName: "file:///allowed",
			params: `{"uri":"file:///allowed","ur\u0131":"file:///etc/shadow",` + m + `}`,
		},
		{
			name:    "resource uri smuggled past folding with U+0130",
			subject: readUnset, hMethod: "resources/read", hName: "file:///allowed",
			params: `{"uri":"file:///allowed","ur\u0130":"file:///etc/shadow",` + m + `}`,
		},
		{
			name:    "protocol version smuggled past lowercasing with U+017F",
			subject: callGetIssue, hMethod: "tools/call", hName: "get_issue",
			params: fmt.Sprintf(`{"name":"get_issue","_meta":{%q:%q,"io.modelcontextprotocol/protocolVerſion":"1999-01-01"}}`,
				mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := inbound(t, tt.subject, tt.hMethod, tt.hName, rawBody(tt.hMethod, tt.params))
			_, got := Check(in)
			require.NotNil(t, got, "smuggled body was authorized")
			assert.Equal(t, mcpspec.ErrHeaderMismatch, got.Code)
		})
	}
}

// TestIntegrityRejectsMalformedParams covers params that are syntactically
// fine but cannot be introspected. Every fixture in the original matrix went
// through body(), so none of these shapes had ever reached Check.
func TestIntegrityRejectsMalformedParams(t *testing.T) {
	m := metaJSON()
	tests := []struct {
		name     string
		method   string
		subject  string
		hName    string
		params   string
		wantCode int
	}{
		{
			name: "params is an array", method: "tools/list",
			subject: "mcp.v1.req.acme.u1.gh.tools.list._",
			params:  `[]`, wantCode: jsonrpc.CodeInvalidRequest,
		},
		{
			name: "params is a string", method: "tools/list",
			subject: "mcp.v1.req.acme.u1.gh.tools.list._",
			params:  `"hello"`, wantCode: jsonrpc.CodeInvalidRequest,
		},
		{
			name: "params is a number", method: "tools/list",
			subject: "mcp.v1.req.acme.u1.gh.tools.list._",
			params:  `7`, wantCode: jsonrpc.CodeInvalidRequest,
		},
		{
			// null params carry no _meta, so the version gate catches it —
			// the same answer as an object with no _meta, which is the point.
			name: "params is null", method: "tools/list",
			subject: "mcp.v1.req.acme.u1.gh.tools.list._",
			params:  `null`, wantCode: mcpspec.ErrHeaderMismatch,
		},
		{
			name: "null params on a named method", method: "tools/call",
			subject: "mcp.v1.req.acme.u1.gh.tools.call._",
			params:  `null`, wantCode: mcpspec.ErrHeaderMismatch,
		},
		{
			// "" is what an absent name decodes to and what the check treats
			// as "no name given", so a non-string name must not quietly
			// become one — it would authorize against the wrong token.
			name: "name is an object", method: "tools/call",
			subject: "mcp.v1.req.acme.u1.gh.tools.call._", hName: "_",
			params: `{"name":{"toString":"get_issue"},` + m + `}`, wantCode: jsonrpc.CodeInvalidRequest,
		},
		{
			name: "name is a number", method: "tools/call",
			subject: "mcp.v1.req.acme.u1.gh.tools.call._", hName: "_",
			params: `{"name":42,` + m + `}`, wantCode: jsonrpc.CodeInvalidRequest,
		},
		{
			name: "uri is an array", method: "resources/read",
			subject: "mcp.v1.req.acme.u1.gh.resources.read._", hName: "file:///x",
			params: `{"uri":["file:///x"],` + m + `}`, wantCode: jsonrpc.CodeInvalidRequest,
		},
		{
			name: "_meta is not an object", method: "tools/list",
			subject: "mcp.v1.req.acme.u1.gh.tools.list._",
			params:  `{"_meta":"2026-07-28"}`, wantCode: jsonrpc.CodeInvalidRequest,
		},
		{
			name: "protocol version is not a string", method: "tools/list",
			subject:  "mcp.v1.req.acme.u1.gh.tools.list._",
			params:   fmt.Sprintf(`{"_meta":{%q:20260728}}`, mcpspec.MetaProtocolVersion),
			wantCode: jsonrpc.CodeInvalidRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := inbound(t, tt.subject, tt.method, tt.hName, rawBody(tt.method, tt.params))
			_, got := Check(in)
			require.NotNil(t, got, "expected rejection")
			assert.Equal(t, tt.wantCode, got.Code)
		})
	}
}

// TestIntegrityLeavesToolArgumentsAlone pins the deliberate edge of the
// case-collision rule. params and _meta have specification-fixed field names,
// so a case-variant sibling there is always an attack. Tool arguments are
// opaque caller data whose keys the gateway does not read, and rejecting a
// legal argument object would make the gateway a schema validator for tools
// it knows nothing about. The arguments that ARE read — the ones an
// x-mcp-header annotation names — are held to the rule where they are read,
// in pkg/backend.valueAtPath.
func TestIntegrityLeavesToolArgumentsAlone(t *testing.T) {
	params := `{"name":"get_issue","arguments":{"Repo":"a","repo":"b"},` + metaJSON() + `}`
	in := inbound(t, "mcp.v1.req.acme.u1.gh.tools.call.get_issue", "tools/call", "get_issue",
		rawBody("tools/call", params))

	_, got := Check(in)
	assert.Nil(t, got)
}

// TestIntegrityAuthorizesTheBytesItForwards is the invariant the whole check
// exists to hold, asserted against the forwarded bytes rather than against
// Check's own reading of them: for every request Check accepts, an ordinary
// case-sensitive parser — which is what the backend is — reads out of the
// params the gateway is about to forward exactly the name the subject
// authorized and the header advertised.
//
// Stated this way the test cannot be fooled by the bug it is guarding, since
// it never asks the gateway what it thinks the name is.
func TestIntegrityAuthorizesTheBytesItForwards(t *testing.T) {
	m := metaJSON()
	bodies := []struct{ subject, method, hName, params string }{
		{
			"mcp.v1.req.acme.u1.gh.tools.call.get_issue", "tools/call", "get_issue",
			`{"name":"get_issue",` + m + `}`,
		},
		{
			"mcp.v1.req.acme.u1.gh.tools.call.get_issue", "tools/call", "get_issue",
			`{"name":"delete_repo","NAME":"get_issue",` + m + `}`,
		},
		{
			"mcp.v1.req.acme.u1.gh.tools.call._", "tools/call", "_",
			`{"name":"delete_repo","Name":"_",` + m + `}`,
		},
		{
			"mcp.v1.req.acme.u1.gh.tools.call.b", "tools/call", "b",
			`{"name":"a","name":"b",` + m + `}`,
		},
		{
			"mcp.v1.req.acme.u1.gh.prompts.get.safe", "prompts/get", "safe",
			`{"name":"safe",` + m + `}`,
		},
		{
			"mcp.v1.req.acme.u1.gh.resources.read._", "resources/read", "file:///allowed",
			`{"uri":"file:///etc/shadow","URI":"file:///allowed",` + m + `}`,
		},
		{
			"mcp.v1.req.acme.u1.gh.resources.read._", "resources/read", "file:///allowed",
			`{"uri":"file:///allowed",` + m + `}`,
		},
	}
	for _, b := range bodies {
		t.Run(b.params, func(t *testing.T) {
			in := inbound(t, b.subject, b.method, b.hName, rawBody(b.method, b.params))
			if _, cerr := Check(in); cerr != nil {
				return // rejected: nothing is forwarded, nothing to prove
			}

			// Read the forwarded bytes the way a backend does: a plain map,
			// exact keys, no struct tags anywhere near it.
			var forwarded map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(in.Msg.Params, &forwarded))
			field := "name"
			if b.method == mcpspec.MethodResourcesRead {
				field = "uri"
			}
			var executed string
			require.NoError(t, json.Unmarshal(forwarded[field], &executed))

			assert.Equal(t, b.hName, executed,
				"backend executes %q but Mcp-Name advertised %q", executed, b.hName)
			// Deliberately NOT wire.SubjectNameToken: this is the invariant,
			// and an invariant restated by calling the helper that decides it
			// is an invariant that agrees with any bug in the helper. The
			// cost of the second copy is that a method gaining a subject
			// token fails here until someone re-reads this assertion, which
			// is the review this file is for.
			wantToken := wire.NameUnset
			if b.method != mcpspec.MethodResourcesRead {
				wantToken = wire.NameToken(executed)
			}
			assert.Equal(t, wantToken, in.Subject.Name,
				"backend executes %q, which NATS did not authorize on this subject", executed)
		})
	}
}

// TestCheckAcceptsEverySubjectAConformantClientBuilds is the anti-drift test
// for the subject's name token, which is decided in two packages that cannot
// see each other: wire.BuildSubject picks the token a client publishes to, and
// Check picks the token it will accept. A rule written twice is a rule that
// eventually disagrees with itself, and the symptom is not an authorization
// hole but a permanent -32020 on requests where subject, header and body all
// honestly agree — the caller is told their subject token is wrong and has
// nothing to correct.
//
// Each case here is a request built exactly the way wire.Client builds one,
// sentinel-encoded header and all, so the two sides are compared through the
// wire's own construction rather than through a fixture that assumes the
// answer.
func TestCheckAcceptsEverySubjectAConformantClientBuilds(t *testing.T) {
	cases := []struct{ method, name string }{
		{mcpspec.MethodToolsCall, "get_issue"},
		{mcpspec.MethodToolsCall, "crème.brûlée"},
		{mcpspec.MethodToolsCall, "_"},
		{mcpspec.MethodPromptsGet, "safe"},
		{mcpspec.MethodResourcesRead, "file:///x/y"},
		{mcpspec.MethodResourcesRead, "https://example.test/a"},
		// The URI shape that has no scheme, which nothing in MCP forbids.
		{mcpspec.MethodResourcesRead, "readme"},
		{"tools/list", ""},
		{"resources/templates/list", ""},
	}
	for _, c := range cases {
		t.Run(c.method+" "+c.name, func(t *testing.T) {
			subj, err := wire.BuildSubject("", "acme", "u1", "gh", c.method, c.name)
			require.NoError(t, err)
			in := inbound(t, subj, c.method, "", body(c.method, c.name, true))
			if c.name != "" {
				in.Header.Set(wire.HeaderName, mcpspec.EncodeHeaderValue(c.name))
			}
			_, cerr := Check(in)
			assert.Nil(t, cerr,
				"a conformant client published to %s and the gateway refused it", subj)
		})
	}
}

func TestIntegrityUnsupportedVersion(t *testing.T) {
	// Header and body agree on a legacy version: consistent, but unsupported.
	b := fmt.Sprintf(`{"jsonrpc":"2.0","id":"1","method":"tools/list","params":{"_meta":{%q:%q}}}`,
		mcpspec.MetaProtocolVersion, mcpspec.LegacyProtocolVersion)
	in := inbound(t, "mcp.v1.req.acme.u1.gh.tools.list._", "tools/list", "", b)
	in.Header.Set(wire.HeaderProtocolVersion, mcpspec.LegacyProtocolVersion)

	_, got := Check(in)
	require.NotNil(t, got)
	assert.Equal(t, mcpspec.ErrUnsupportedProtocolVersion, got.Code)
	data, ok := got.Data.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, mcpspec.LegacyProtocolVersion, data["requested"])
}

func TestIntegrityAcceptsSentinelEncodedName(t *testing.T) {
	// A tool name outside the header-safe set MUST reach us base64-encoded.
	// Comparing the raw header to the body would reject a conformant client,
	// so the check decodes first.
	const name = "crème.brûlée"
	b := body(mcpspec.MethodToolsCall, name, true)
	in := inbound(t, "mcp.v1.req.acme.u1.gh.tools.call._", mcpspec.MethodToolsCall, "", b)
	encoded := mcpspec.EncodeHeaderValue(name)
	require.NotEqual(t, name, encoded, "the fixture must actually be encoded")
	in.Header.Set(wire.HeaderName, encoded)

	_, got := Check(in)
	assert.Nil(t, got)
}

func TestIntegrityStillCatchesMismatchUnderEncoding(t *testing.T) {
	// Decoding must not become a way to smuggle a different name past the
	// subject the caller was authorized for.
	b := body(mcpspec.MethodToolsCall, "crème.brûlée", true)
	in := inbound(t, "mcp.v1.req.acme.u1.gh.tools.call._", mcpspec.MethodToolsCall, "", b)
	in.Header.Set(wire.HeaderName, mcpspec.EncodeHeaderValue("délicieux"))

	_, got := Check(in)
	require.NotNil(t, got)
	assert.Equal(t, mcpspec.ErrHeaderMismatch, got.Code)
}

func TestIntegrityRejectsMalformedSentinel(t *testing.T) {
	b := body(mcpspec.MethodToolsCall, "crème.brûlée", true)
	in := inbound(t, "mcp.v1.req.acme.u1.gh.tools.call._", mcpspec.MethodToolsCall, "", b)
	in.Header.Set(wire.HeaderName, "=?base64?not!base64!?=")

	_, got := Check(in)
	require.NotNil(t, got)
	assert.Equal(t, mcpspec.ErrHeaderMismatch, got.Code)
}
