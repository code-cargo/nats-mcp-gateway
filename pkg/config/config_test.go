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

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func write(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoadValidWithEnvExpansion(t *testing.T) {
	t.Setenv("TEST_GH_TOKEN", "tok-123")
	cfg, err := Load(write(t, `{
		"nats": {"url": "nats://x:4222"},
		"servers": {
			"github": {
				"transport": "stdio",
				"command": "gh-mcp",
				"env": {"GITHUB_TOKEN": "${TEST_GH_TOKEN}"}
			},
			"weather": {
				"protocol": "2026-07-28",
				"transport": "http",
				"url": "https://weather/mcp",
				"headers": {"Authorization": "Bearer ${TEST_GH_TOKEN}"}
			}
		}
	}`))
	require.NoError(t, err)
	assert.Equal(t, "tok-123", cfg.Servers["github"].Env["GITHUB_TOKEN"])
	assert.Equal(t, "Bearer tok-123", cfg.Servers["weather"].Headers["Authorization"])
	assert.ElementsMatch(t, []string{"github", "weather"}, cfg.ServerNames())
}

func TestLoadRejects(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantIn  string
	}{
		{"bad server name", `{"servers":{"bad name":{"command":"x"}}}`, "not subject-token safe"},
		{"bad protocol", `{"servers":{"s":{"command":"x","protocol":"2024-01-01"}}}`, "unknown protocol"},
		{"stdio without command", `{"servers":{"s":{"transport":"stdio"}}}`, "requires command"},
		{"http without url", `{"servers":{"s":{"transport":"http"}}}`, "requires url"},
		{"unknown transport", `{"servers":{"s":{"transport":"grpc"}}}`, "unknown transport"},
		{"unknown field", `{"serverz":{}}`, "unknown field"},
		{"double underscore in server name", `{"servers":{"a__b":{"command":"x"}}}`, "reserved as the tool-namespacing delimiter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(write(t, tt.content))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantIn)
		})
	}
}

// An empty server set is the valid steady state of a gateway whose servers
// have all been removed via reload — Parse must accept it.
func TestParseEmptyServersValid(t *testing.T) {
	cfg, err := Parse([]byte(`{"nats":{"url":"nats://x:4222"}}`))
	require.NoError(t, err)
	assert.Empty(t, cfg.ServerNames())
}

// Parse and Load share one validator: the same bytes fail identically.
func TestParseRejectsSameAsLoad(t *testing.T) {
	_, err := Parse([]byte(`{"servers":{"s":{"transport":"grpc"}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown transport")
}

// Forward-compat: a strict Parse rejects an unknown field (typo protection for
// human-authored files), but ParseForwardCompatible ignores it — an older
// gateway must tolerate a config a newer controller emitted with fields it
// doesn't know yet, rather than reject the whole config mid-rolling-upgrade.
func TestParseForwardCompatibleIgnoresUnknownFields(t *testing.T) {
	raw := []byte(`{"servers":{"github":{"transport":"stdio","command":"gh-mcp","futureField":true}}}`)

	_, err := Parse(raw)
	require.Error(t, err, "strict Parse must reject unknown fields")
	assert.Contains(t, err.Error(), "unknown field")

	cfg, err := ParseForwardCompatible(raw)
	require.NoError(t, err, "forward-compatible Parse must ignore unknown fields")
	assert.Equal(t, []string{"github"}, cfg.ServerNames())
	// Known fields still parse; the unknown one is simply dropped.
	assert.Equal(t, "gh-mcp", cfg.Servers["github"].Command)
}

// ParseForwardCompatible still enforces validation — leniency is about
// unknown fields, not about accepting invalid config.
func TestParseForwardCompatibleStillValidates(t *testing.T) {
	_, err := ParseForwardCompatible([]byte(`{"servers":{"s":{"transport":"grpc"}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown transport")

	_, err = ParseForwardCompatible([]byte(`{"servers":{"a__b":{"command":"x"}}}`))
	require.Error(t, err, "reserved __ delimiter still rejected on the fetch path")
}

func TestAuthValidation(t *testing.T) {
	valid := []string{
		`{"servers":{"s":{"command":"x","auth":{"mode":"static"}}}}`,
		`{"servers":{"s":{"command":"x","auth":{"mode":"nats"}}}}`,
		`{"servers":{"s":{"command":"x","auth":{"mode":"exec","command":"helper"}}}}`,
		`{"servers":{"s":{"command":"x","auth":{"mode":"file","path":"/p","ttl":"30s"}}}}`,
		`{"servers":{"s":{"command":"x","auth":{"mode":"oauth-client-credentials","tokenUrl":"https://idp/t","clientId":"c","clientSecret":"s"}}}}`,
		`{"servers":{"s":{"command":"x","auth":{"mode":"oauth-token-exchange","tokenUrl":"https://idp/t","subjectTokenFile":"/tok/{user}"}}}}`,
		`{"servers":{"s":{"command":"x","auth":{"mode":"oauth-refresh","tokenUrl":"https://idp/t","clientId":"c","refreshTokenFile":"/rt/{user}"}}}}`,
	}
	for _, raw := range valid {
		_, err := Parse([]byte(raw))
		assert.NoError(t, err, raw)
	}

	invalid := []struct{ raw, wantIn string }{
		{`{"servers":{"s":{"command":"x","auth":{"mode":"wat"}}}}`, "unknown auth mode"},
		{`{"servers":{"s":{"command":"x","auth":{"mode":"exec"}}}}`, "requires command"},
		{`{"servers":{"s":{"command":"x","auth":{"mode":"file"}}}}`, "requires path"},
		{`{"servers":{"s":{"command":"x","auth":{"mode":"oauth-client-credentials","tokenUrl":"u"}}}}`, "requires tokenUrl, clientId, clientSecret"},
		{`{"servers":{"s":{"command":"x","auth":{"mode":"oauth-token-exchange","tokenUrl":"u"}}}}`, "subjectTokenFile"},
		{`{"servers":{"s":{"command":"x","auth":{"mode":"oauth-refresh","tokenUrl":"u"}}}}`, "refreshTokenFile"},
		{`{"servers":{"s":{"command":"x","auth":{"mode":"file","path":"/p","ttl":"soon"}}}}`, "auth ttl"},
	}
	for _, tt := range invalid {
		_, err := Parse([]byte(tt.raw))
		require.Error(t, err, tt.raw)
		assert.Contains(t, err.Error(), tt.wantIn)
	}
}

func TestAuthGrain(t *testing.T) {
	perUser := []string{AuthExec, AuthNATS, AuthOAuthTokenExchange, AuthOAuthRefresh}
	for _, mode := range perUser {
		a := &Auth{Mode: mode}
		assert.True(t, a.PerUser(), mode)
		assert.True(t, a.Dynamic(), mode)
	}
	shared := []string{AuthFile, AuthOAuthClientCredentials}
	for _, mode := range shared {
		a := &Auth{Mode: mode}
		assert.False(t, a.PerUser(), mode)
		assert.True(t, a.Dynamic(), mode)
	}
	// File grain follows the path: a {user}-templated path is per-user, so
	// it never silently resolves every caller as "_".
	assert.True(t, (&Auth{Mode: AuthFile, Path: "/creds/{user}.json"}).PerUser())
	assert.False(t, (&Auth{Mode: AuthFile, Path: "/creds/shared.json"}).PerUser())
	assert.False(t, (&Auth{Mode: AuthStatic}).Dynamic())
	assert.False(t, (&Auth{}).Dynamic())
	var nilAuth *Auth
	assert.False(t, nilAuth.Dynamic())
	assert.False(t, nilAuth.PerUser())
}

func TestNATSScopingValidation(t *testing.T) {
	cfg, err := Parse([]byte(`{"nats":{"tenant":"acme","user":"u1"},"servers":{}}`))
	require.NoError(t, err)
	assert.Equal(t, "acme", cfg.NATS.Tenant)
	assert.Equal(t, "u1", cfg.NATS.User)

	_, err = Parse([]byte(`{"nats":{"tenant":"acme"},"servers":{}}`))
	require.Error(t, err, "tenant without user must be rejected")
	assert.Contains(t, err.Error(), "both tenant and user")

	_, err = Parse([]byte(`{"nats":{"tenant":"bad tenant","user":"u1"},"servers":{}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not subject-token safe")
}

func TestClaimCheckValidation(t *testing.T) {
	cfg, err := Parse([]byte(`{"claimCheck":{"maxAge":"10m","maxBytes":1000000},"servers":{}}`))
	require.NoError(t, err)
	require.NotNil(t, cfg.ClaimCheck)
	assert.Equal(t, "10m", cfg.ClaimCheck.MaxAge)

	cfg, err = Parse([]byte(`{"claimCheck":{},"servers":{}}`))
	require.NoError(t, err, "empty block = enabled with defaults")
	require.NotNil(t, cfg.ClaimCheck)

	cfg, err = Parse([]byte(`{"servers":{}}`))
	require.NoError(t, err)
	assert.Nil(t, cfg.ClaimCheck, "absent block = disabled")

	_, err = Parse([]byte(`{"claimCheck":{"maxAge":"soon"},"servers":{}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "claimCheck.maxAge")

	_, err = Parse([]byte(`{"claimCheck":{"maxBytes":-1},"servers":{}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maxBytes")
}
