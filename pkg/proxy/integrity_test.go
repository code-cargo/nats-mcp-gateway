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
			got := Check(in)
			if tt.wantCode == 0 {
				assert.Nil(t, got, "expected pass, got %v", got)
			} else {
				require.NotNil(t, got, "expected rejection")
				assert.Equal(t, tt.wantCode, got.Code)
			}
		})
	}
}

func TestIntegrityUnsupportedVersion(t *testing.T) {
	// Header and body agree on a legacy version: consistent, but unsupported.
	b := fmt.Sprintf(`{"jsonrpc":"2.0","id":"1","method":"tools/list","params":{"_meta":{%q:%q}}}`,
		mcpspec.MetaProtocolVersion, mcpspec.LegacyProtocolVersion)
	in := inbound(t, "mcp.v1.req.acme.u1.gh.tools.list._", "tools/list", "", b)
	in.Header.Set(wire.HeaderProtocolVersion, mcpspec.LegacyProtocolVersion)

	got := Check(in)
	require.NotNil(t, got)
	assert.Equal(t, mcpspec.ErrUnsupportedProtocolVersion, got.Code)
	data, ok := got.Data.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, mcpspec.LegacyProtocolVersion, data["requested"])
}
