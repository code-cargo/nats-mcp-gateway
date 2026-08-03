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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSubject(t *testing.T) {
	tests := []struct {
		name    string
		tenant  string
		user    string
		server  string
		method  string
		reqName string
		want    string
		wantErr bool
	}{
		{"tools/call named", "acme", "u1", "github", "tools/call", "get_issue", "mcp.v1.req.acme.u1.github.tools.call.get_issue", false},
		{"unattributed user placeholder", "acme", "_", "github", "tools/list", "", "mcp.v1.req.acme._.github.tools.list._", false},
		{"empty user becomes placeholder", "acme", "", "github", "tools/list", "", "mcp.v1.req.acme._.github.tools.list._", false},
		{"three-token method", "acme", "u1", "github", "resources/templates/list", "", "mcp.v1.req.acme.u1.github.resources.templates.list._", false},
		{"discover", "acme", "u1", "github", "server/discover", "", "mcp.v1.req.acme.u1.github.server.discover._", false},
		{"listen", "t1", "u1", "weather", "subscriptions/listen", "", "mcp.v1.req.t1.u1.weather.subscriptions.listen._", false},
		{"resource uri falls back", "acme", "u1", "github", "resources/read", "file:///etc/hosts", "mcp.v1.req.acme.u1.github.resources.read._", false},
		{"unsafe tool name falls back", "acme", "u1", "github", "tools/call", "crème.brûlée", "mcp.v1.req.acme.u1.github.tools.call._", false},
		{"tool literally named underscore", "acme", "u1", "github", "tools/call", "_", "mcp.v1.req.acme.u1.github.tools.call._", false},
		{"bad tenant", "ac me", "u1", "github", "tools/list", "", "", true},
		{"bad user", "acme", "u 1", "github", "tools/list", "", "", true},
		{"bad server", "acme", "u1", "git hub", "tools/list", "", "", true},
		{"empty method", "acme", "u1", "github", "", "", "", true},
		{"method with empty segment", "acme", "u1", "github", "tools//call", "", "", true},
		{"method with unsafe segment", "acme", "u1", "github", "tools/c all", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildSubject("", tt.tenant, tt.user, tt.server, tt.method, tt.reqName)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestBuildSubjectKeepsResourceURIsOffTheSubject pins the one method whose
// name never reaches the subject.
//
// "A URI is never token-safe" is a property of the URIs seen so far, not of
// the grammar: MCP does not require a scheme, and a server is free to hand out
// a relative reference like "readme". Deriving the token from the URI anyway
// puts such a request on a subject pkg/proxy.Check refuses by rule, and the
// caller gets a -32020 naming a subject-token rule nobody wrote while subject,
// header and body all honestly agree.
//
// The token is withheld rather than the check widened because per-URI grants
// cannot be written honestly: the "_" fallback is the RULE for URIs, not the
// exception, so any grant that covers "file:///…" already covers everything a
// per-URI token could have carved out.
func TestBuildSubjectKeepsResourceURIsOffTheSubject(t *testing.T) {
	for _, uri := range []string{"readme", "config", "file:///x", "https://a/b"} {
		got, err := BuildSubject("", "acme", "u1", "gh", "resources/read", uri)
		require.NoError(t, err)
		assert.Equal(t, "mcp.v1.req.acme.u1.gh.resources.read."+NameUnset, got,
			"uri %q must not reach the subject's name token", uri)
	}
}

func TestParseSubjectRoundTrip(t *testing.T) {
	methods := []struct {
		method string
		name   string
	}{
		{"tools/call", "get_issue"},
		{"tools/list", ""},
		{"resources/templates/list", ""},
		{"server/discover", ""},
		{"subscriptions/listen", ""},
	}
	for _, m := range methods {
		t.Run(m.method, func(t *testing.T) {
			subj, err := BuildSubject("custom.prefix", "t1", "u9", "srv", m.method, m.name)
			require.NoError(t, err)
			parsed, err := ParseSubject(subj, "custom.prefix")
			require.NoError(t, err)
			assert.Equal(t, "t1", parsed.Tenant)
			assert.Equal(t, "u9", parsed.User)
			assert.Equal(t, "srv", parsed.Server)
			assert.Equal(t, m.method, parsed.Method)
			if m.name == "" {
				assert.Equal(t, NameUnset, parsed.Name)
			} else {
				assert.Equal(t, m.name, parsed.Name)
			}
		})
	}
}

func TestParseSubjectRejects(t *testing.T) {
	bad := []string{
		"mcp.v1.req.acme.u1.github",              // too few tokens (no method+name)
		"other.prefix.req.a.u.b.tools.list._",    // wrong prefix
		"mcp.v1.notreq.acme.u1.github.tools.l._", // wrong verb
	}
	for _, s := range bad {
		_, err := ParseSubject(s, "")
		assert.Error(t, err, s)
	}
}

func TestEndpointSubject(t *testing.T) {
	got, err := EndpointSubject("", "", "", "github")
	require.NoError(t, err)
	assert.Equal(t, "mcp.v1.req.*.*.github.>", got)

	_, err = EndpointSubject("", "", "", "bad name")
	assert.Error(t, err)
}

func TestEndpointSubjectScoped(t *testing.T) {
	// Fully scoped: one caller's slice (per-user pod).
	got, err := EndpointSubject("", "acme", "u1", "github")
	require.NoError(t, err)
	assert.Equal(t, "mcp.v1.req.acme.u1.github.>", got)

	// Tenant-only: the whole tenant's slice, user token wildcarded — an org
	// deployment serving all of one tenant's users.
	got, err = EndpointSubject("", "acme", "", "github")
	require.NoError(t, err)
	assert.Equal(t, "mcp.v1.req.acme.*.github.>", got)

	// A user without a tenant would scope by the attribution token alone,
	// spanning every tenant — never a shape we bind.
	_, err = EndpointSubject("", "", "u1", "github")
	assert.Error(t, err)

	_, err = EndpointSubject("", "bad tenant", "u1", "github")
	assert.Error(t, err)
	_, err = EndpointSubject("", "acme", "bad user", "github")
	assert.Error(t, err)
}
