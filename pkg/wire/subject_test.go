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
		server  string
		method  string
		reqName string
		want    string
		wantErr bool
	}{
		{"tools/call named", "acme", "github", "tools/call", "get_issue", "mcp.v1.req.acme.github.tools.call.get_issue", false},
		{"tools/list unnamed", "acme", "github", "tools/list", "", "mcp.v1.req.acme.github.tools.list._", false},
		{"three-token method", "acme", "github", "resources/templates/list", "", "mcp.v1.req.acme.github.resources.templates.list._", false},
		{"discover", "acme", "github", "server/discover", "", "mcp.v1.req.acme.github.server.discover._", false},
		{"listen", "t1", "weather", "subscriptions/listen", "", "mcp.v1.req.t1.weather.subscriptions.listen._", false},
		{"resource uri falls back", "acme", "github", "resources/read", "file:///etc/hosts", "mcp.v1.req.acme.github.resources.read._", false},
		{"unsafe tool name falls back", "acme", "github", "tools/call", "crème.brûlée", "mcp.v1.req.acme.github.tools.call._", false},
		{"tool literally named underscore", "acme", "github", "tools/call", "_", "mcp.v1.req.acme.github.tools.call._", false},
		{"bad tenant", "ac me", "github", "tools/list", "", "", true},
		{"bad server", "acme", "git hub", "tools/list", "", "", true},
		{"empty method", "acme", "github", "", "", "", true},
		{"method with empty segment", "acme", "github", "tools//call", "", "", true},
		{"method with unsafe segment", "acme", "github", "tools/c all", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildSubject("", tt.tenant, tt.server, tt.method, tt.reqName)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
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
			subj, err := BuildSubject("custom.prefix", "t1", "srv", m.method, m.name)
			require.NoError(t, err)
			parsed, err := ParseSubject(subj, "custom.prefix")
			require.NoError(t, err)
			assert.Equal(t, "t1", parsed.Tenant)
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
		"mcp.v1.req.acme.github",              // too few tokens
		"other.prefix.req.a.b.tools.list._",   // wrong prefix
		"mcp.v1.notreq.acme.github.tools.l._", // wrong verb
	}
	for _, s := range bad {
		_, err := ParseSubject(s, "")
		assert.Error(t, err, s)
	}
}

func TestEndpointSubject(t *testing.T) {
	got, err := EndpointSubject("", "github")
	require.NoError(t, err)
	assert.Equal(t, "mcp.v1.req.*.github.>", got)

	_, err = EndpointSubject("", "bad name")
	assert.Error(t, err)
}
