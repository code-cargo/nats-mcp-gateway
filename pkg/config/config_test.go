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
// human-authored files), but the machine-fed sources ignore it — an older
// gateway must tolerate a config a newer controller emitted with fields it
// doesn't know yet, rather than reject the whole config mid-rolling-upgrade.
func TestForwardCompatibleSourcesIgnoreUnknownFields(t *testing.T) {
	raw := []byte(`{"servers":{"github":{"transport":"stdio","command":"gh-mcp","futureField":true}}}`)

	_, err := Parse(raw)
	require.Error(t, err, "strict Parse must reject unknown fields")
	assert.Contains(t, err.Error(), "unknown field")

	for name, parse := range map[string]func([]byte) (*Config, error){
		"fetched": ParseFetched,
		"inline":  ParseInline,
	} {
		cfg, err := parse(raw)
		require.NoError(t, err, "%s: forward-compatible parse must ignore unknown fields", name)
		assert.Equal(t, []string{"github"}, cfg.ServerNames(), name)
		// Known fields still parse; the unknown one is simply dropped.
		assert.Equal(t, "gh-mcp", cfg.Servers["github"].Command, name)
	}
}

// The forward-compatible sources still enforce validation — leniency is about
// unknown fields, not about accepting invalid config.
func TestForwardCompatibleSourcesStillValidate(t *testing.T) {
	_, err := ParseFetched([]byte(`{"servers":{"s":{"transport":"grpc"}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown transport")

	_, err = ParseFetched([]byte(`{"servers":{"a__b":{"command":"x"}}}`))
	require.Error(t, err, "reserved __ delimiter still rejected on the fetch path")

	_, err = ParseInline([]byte(`{"servers":{"s":{"transport":"grpc"}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown transport")
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

// Every credential the gateway holds for an http server rides one of these two
// URLs: the backend url carries the injected Authorization header on every
// call, the token url carries the client secret, the RFC 8693 subject token
// and the refresh token. A one-character typo dropping the "s" put all of them
// on the wire in the clear, and nothing anywhere said so.
func TestPlaintextURLsRejected(t *testing.T) {
	rejected := []struct{ name, raw, wantIn string }{
		{
			"backend url",
			`{"servers":{"s":{"transport":"http","url":"http://weather.internal/mcp"}}}`,
			`server "s": url`,
		},
		{
			"token url",
			`{"servers":{"s":{"command":"x","auth":{"mode":"oauth-client-credentials","tokenUrl":"http://idp/t","clientId":"c","clientSecret":"sec"}}}}`,
			`server "s": auth tokenUrl`,
		},
		{
			"scheme net/http cannot speak",
			`{"servers":{"s":{"transport":"http","url":"ftp://weather.internal/mcp"}}}`,
			"must be https",
		},
		{
			"no scheme at all",
			`{"servers":{"s":{"transport":"http","url":"weather.internal:8080/mcp"}}}`,
			"must be https",
		},
	}
	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.raw))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantIn)
		})
	}

	// Loopback is exempt: the traffic never reaches a network anyone can read,
	// and `http://127.0.0.1:3000/mcp` is what every local MCP server serves.
	accepted := []struct{ name, raw string }{
		{"127.0.0.1", `{"servers":{"s":{"transport":"http","url":"http://127.0.0.1:3000/mcp"}}}`},
		{"other 127/8", `{"servers":{"s":{"transport":"http","url":"http://127.9.9.9:3000/mcp"}}}`},
		{"ipv6 loopback", `{"servers":{"s":{"transport":"http","url":"http://[::1]:3000/mcp"}}}`},
		{"localhost", `{"servers":{"s":{"transport":"http","url":"http://localhost:3000/mcp"}}}`},
		{"reserved .localhost", `{"servers":{"s":{"transport":"http","url":"http://mcp.localhost:3000/"}}}`},
		// url.Parse lowercases the scheme and leaves the host as written, so
		// the check has to do it: DNS does not distinguish these, and a
		// rejection that turns on the shift key is how allowPlaintext ends up
		// set fleet-wide.
		{"LOCALHOST", `{"servers":{"s":{"transport":"http","url":"http://LOCALHOST:3000/mcp"}}}`},
		{"mixed-case .localhost", `{"servers":{"s":{"transport":"http","url":"http://MCP.Localhost:3000/"}}}`},
		{"loopback token url", `{"servers":{"s":{"command":"x","auth":{"mode":"oauth-refresh","tokenUrl":"http://localhost:8080/t","clientId":"c","refreshTokenFile":"/rt/{user}"}}}}`},
		{"https anywhere", `{"servers":{"s":{"transport":"http","url":"https://weather.internal/mcp"}}}`},
	}
	for _, tt := range accepted {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.raw))
			assert.NoError(t, err)
		})
	}
}

// TestRejectedURLsAreRedacted keeps the credential out of the rejection.
//
// A URL that fails this check is the one most likely to be carrying a secret
// in the clear, and the message naming it does not stop at the operator's
// terminal: Parse re-validates on every reload, and pkg/configsource logs the
// error it keeps rejecting on every poll tick, so a bad document re-emits the
// secret for as long as it persists. Both halves of a URL carry one — the
// userinfo password and the query string an api_key is expanded into — and the
// unparseable case carries it through url.Error, which quotes the string it
// could not parse.
func TestRejectedURLsAreRedacted(t *testing.T) {
	const (
		pass  = "hunter2correcthorse"
		query = "SECRET123apikey"
	)
	for _, tt := range []struct {
		name, raw, wantIn string
	}{
		{
			"cleartext backend url",
			`{"servers":{"s":{"transport":"http","url":"http://svcacct:` + pass + `@internal.example.com/mcp?api_key=` + query + `"}}}`,
			"internal.example.com",
		},
		{
			"cleartext token url",
			`{"servers":{"s":{"command":"x","auth":{"mode":"oauth-refresh","tokenUrl":"http://svcacct:` + pass + `@idp.example.com/t?api_key=` + query + `","clientId":"c","refreshTokenFile":"/rt"}}}}`,
			"idp.example.com",
		},
		{
			"scheme net/http cannot speak",
			`{"servers":{"s":{"transport":"http","url":"ftp://svcacct:` + pass + `@files.example.com/mcp?api_key=` + query + `"}}}`,
			"files.example.com",
		},
		{
			// url.Parse fails here, and its *url.Error prints the raw string —
			// a password malformed enough to break parsing being exactly the
			// kind that gets mistyped.
			"unparseable",
			`{"servers":{"s":{"transport":"http","url":"https://svcacct:` + pass + `@example.com:not-a-port/mcp?api_key=` + query + `"}}}`,
			"is not a valid URL",
		},
		{
			// A colon-less userinfo is the shape a bare token takes, and
			// url.URL.Redacted leaves it verbatim.
			"token in userinfo",
			`{"servers":{"s":{"transport":"http","url":"http://` + pass + `@internal.example.com/mcp"}}}`,
			"internal.example.com",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.raw))
			require.Error(t, err)
			assert.NotContains(t, err.Error(), pass, "the credential reached the rejection")
			assert.NotContains(t, err.Error(), query, "a query-string secret reached the rejection")
			assert.Contains(t, err.Error(), tt.wantIn, "the rejection must still say which URL it refused")
		})
	}
}

// The opt-out exists for the deployment where the gateway legitimately speaks
// plaintext and something outside its view encrypts: a service mesh sidecar
// intercepting the pod's traffic, an SSH tunnel. Without it, hardening the
// default would leave those deployments no way to run at all.
func TestAllowPlaintextOptsOutPerServer(t *testing.T) {
	cfg, err := Parse([]byte(`{"servers":{"s":{
		"transport":"http","url":"http://mesh.svc.cluster.local/mcp","allowPlaintext":true,
		"auth":{"mode":"oauth-client-credentials","tokenUrl":"http://idp.svc/t","clientId":"c","clientSecret":"sec"}}}}`))
	require.NoError(t, err)
	assert.True(t, cfg.Servers["s"].AllowPlaintext)

	// The flag reaches auth.tokenUrl and not only url. Asserted on a server
	// with no url of its own, because the case above passes either way — a
	// tokenUrl nothing checks looks exactly like a tokenUrl the flag licensed.
	_, err = Parse([]byte(`{"servers":{"s":{"command":"x","allowPlaintext":true,
		"auth":{"mode":"oauth-client-credentials","tokenUrl":"http://idp.svc/t","clientId":"c","clientSecret":"sec"}}}}`))
	require.NoError(t, err)

	// It licenses plaintext for the one server that declares it, and says
	// nothing about any other.
	_, err = Parse([]byte(`{"servers":{
		"meshed":{"transport":"http","url":"http://a.svc/mcp","allowPlaintext":true},
		"plain":{"transport":"http","url":"http://b.svc/mcp"}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `server "plain"`)
}

func TestAuthGrain(t *testing.T) {
	// exec and nats hand the resolver (tenant, user, server) and let it
	// answer, so nothing in the document decides this: they default per-user,
	// which is the fail-closed reading.
	perUser := []string{AuthExec, AuthNATS}
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

// TestOAuthFileGrainFollowsTheTokenPath extends the file mode's rule to the two
// oauth modes that select their input the same way.
//
// Both read a file per resolve through the same {tenant}/{user}/{server}
// expansion (cred.OAuthTokenExchange's subject token, cred.FileTokenStore's
// refresh token), so a path without {user} hands every caller the same
// assertion and the exchange returns one credential for the whole tenant.
// Calling that per-user is not a mislabel an operator can live with: the proxy
// refuses an unattributed caller on a per-user server, and a deployment
// without per-user NATS auth sends the documented "_" on every request — so a
// projected service-account token or a fixed refreshTokenFile, both provably
// shared, answered -32014 to every request with no way to opt out.
func TestOAuthFileGrainFollowsTheTokenPath(t *testing.T) {
	assert.True(t, (&Auth{Mode: AuthOAuthTokenExchange, SubjectTokenFile: "/run/tok/{user}.jwt"}).PerUser())
	assert.False(t, (&Auth{Mode: AuthOAuthTokenExchange, SubjectTokenFile: "/var/run/secrets/sa/token"}).PerUser())
	assert.True(t, (&Auth{Mode: AuthOAuthRefresh, RefreshTokenFile: "/rt/{tenant}/{user}"}).PerUser())
	assert.False(t, (&Auth{Mode: AuthOAuthRefresh, RefreshTokenFile: "/rt/shared"}).PerUser())

	// Through a document, because the grain has to survive the parse that
	// deployments actually go through.
	cfg, err := Parse([]byte(`{"servers":{
		"shared":{"command":"x","auth":{"mode":"oauth-token-exchange","tokenUrl":"https://idp/t","subjectTokenFile":"/var/run/secrets/sa/token"}},
		"peruser":{"command":"x","auth":{"mode":"oauth-token-exchange","tokenUrl":"https://idp/t","subjectTokenFile":"/run/tok/{user}.jwt"}}}}`))
	require.NoError(t, err)
	assert.False(t, cfg.Servers["shared"].Auth.PerUser())
	assert.True(t, cfg.Servers["peruser"].Auth.PerUser())
}

// TestPerUserOptOut covers the escape hatch for the two modes that cannot
// derive their grain.
//
// The README's tier-2 recipe is an exec helper handed NATSMCP_CRED_TENANT and
// NATSMCP_CRED_SERVER that legitimately ignores the user, and a `nats`
// controller may answer per tenant just as legitimately. Both were per-user
// unconditionally, which under a deployment with no per-user NATS auth is a
// total outage — every request refused with -32014, naming a remedy that
// deployment cannot reach.
func TestPerUserOptOut(t *testing.T) {
	cfg, err := Parse([]byte(`{"servers":{
		"tenantwide":{"command":"x","auth":{"mode":"exec","command":"/opt/creds.sh","perUser":false}},
		"controller":{"command":"x","auth":{"mode":"nats","perUser":false}},
		"default":{"command":"x","auth":{"mode":"exec","command":"/opt/creds.sh"}},
		"explicit":{"command":"x","auth":{"mode":"exec","command":"/opt/creds.sh","perUser":true}}}}`))
	require.NoError(t, err)
	assert.False(t, cfg.Servers["tenantwide"].Auth.PerUser(), "an opted-out exec helper must resolve one credential per tenant")
	assert.False(t, cfg.Servers["controller"].Auth.PerUser())
	assert.True(t, cfg.Servers["default"].Auth.PerUser(), "the default stays fail-closed")
	assert.True(t, cfg.Servers["explicit"].Auth.PerUser())

	// Rejected on the modes that DO derive it, rather than silently ignored:
	// honoring an override against a shared path would pool per user over one
	// file, and dropping it quietly would leave an operator believing they had
	// shared a credential the gateway still resolves per caller.
	for _, raw := range []string{
		`{"servers":{"s":{"command":"x","auth":{"mode":"file","path":"/creds/shared.json","perUser":true}}}}`,
		`{"servers":{"s":{"command":"x","auth":{"mode":"oauth-token-exchange","tokenUrl":"https://idp/t","subjectTokenFile":"/t","perUser":false}}}}`,
		`{"servers":{"s":{"command":"x","auth":{"mode":"static","perUser":false}}}}`,
	} {
		_, err := Parse([]byte(raw))
		require.Error(t, err, raw)
		assert.Contains(t, err.Error(), "does not take perUser")
	}
}

func TestNATSScopingValidation(t *testing.T) {
	cfg, err := Parse([]byte(`{"nats":{"tenant":"acme","user":"u1"},"servers":{}}`))
	require.NoError(t, err)
	assert.Equal(t, "acme", cfg.NATS.Tenant)
	assert.Equal(t, "u1", cfg.NATS.User)

	// Tenant alone is valid: an org deployment serving all of one tenant's users.
	cfg, err = Parse([]byte(`{"nats":{"tenant":"acme"},"servers":{}}`))
	require.NoError(t, err)
	assert.Equal(t, "acme", cfg.NATS.Tenant)
	assert.Empty(t, cfg.NATS.User)

	// A user without a tenant is rejected.
	_, err = Parse([]byte(`{"nats":{"user":"u1"},"servers":{}}`))
	require.Error(t, err, "user without tenant must be rejected")
	assert.Contains(t, err.Error(), "requires a tenant")

	_, err = Parse([]byte(`{"nats":{"tenant":"bad tenant","user":"u1"},"servers":{}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not subject-token safe")
}

// The file source's equivalent of --inbox-prefix. It is validated here so a
// bad value fails the load rather than nats.Connect, whose "invalid custom
// prefix" names neither the setting nor the value — and whose own check misses
// spaces entirely.
func TestNATSInboxPrefixValidation(t *testing.T) {
	cfg, err := Parse([]byte(`{"nats":{"inboxPrefix":"_INBOX_acme.u_9f3a"},"servers":{}}`))
	require.NoError(t, err)
	assert.Equal(t, "_INBOX_acme.u_9f3a", cfg.NATS.InboxPrefix)

	cfg, err = Parse([]byte(`{"servers":{}}`))
	require.NoError(t, err)
	assert.Empty(t, cfg.NATS.InboxPrefix, "absent = the nats.go default inbox")

	for _, bad := range []string{"_INBOX acme", "_INBOX.>", "_INBOX.*", "_INBOX.acme.", "_INBOX..acme"} {
		_, err = Parse([]byte(`{"nats":{"inboxPrefix":"` + bad + `"},"servers":{}}`))
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "nats.inboxPrefix", bad)
	}
}

// The wire subject prefix is the front of every subscription this gateway
// binds, so it gets the same literal-token check the inbox prefix gets. A
// wildcard is the dangerous form and the one nothing downstream catches:
// "mcp.*" builds the endpoint subject "mcp.*.req.*.*.{server}.>", which micro
// accepts and NATS happily binds — the gateway then receives (and answers)
// traffic addressed to every other prefix in the account. The empty-token
// forms at least fail, but only at the first config apply, as nats.go's bare
// "invalid subject".
func TestNATSSubjectPrefixValidation(t *testing.T) {
	cfg, err := Parse([]byte(`{"nats":{"subjectPrefix":"acme.mcp"},"servers":{}}`))
	require.NoError(t, err)
	assert.Equal(t, "acme.mcp", cfg.NATS.SubjectPrefix)

	cfg, err = Parse([]byte(`{"servers":{}}`))
	require.NoError(t, err)
	assert.Empty(t, cfg.NATS.SubjectPrefix, "absent = the wire default prefix")

	for _, bad := range []string{"mcp.*", "mcp.>", "mcp v1", "mcp.v1.", ".mcp.v1", "mcp..v1"} {
		_, err = Parse([]byte(`{"nats":{"subjectPrefix":"` + bad + `"},"servers":{}}`))
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "nats.subjectPrefix", bad)
	}
}

// A queue group name is matched LITERALLY — "mcpgw.*" is not a pattern, it is
// a group whose name happens to contain a star — so a wildcard written in the
// belief that it scopes subjects silently means something other than what it
// says. The malformed forms do fail, but late and anonymously: micro rejects a
// space at the first config apply as "invalid endpoint queue group", naming
// neither the setting nor the value, with the process already connected.
func TestNATSQueueGroupValidation(t *testing.T) {
	cfg, err := Parse([]byte(`{"nats":{"queueGroup":"mcpgw.acme.u_9f3a"},"servers":{}}`))
	require.NoError(t, err)
	assert.Equal(t, "mcpgw.acme.u_9f3a", cfg.NATS.QueueGroup)

	for _, bad := range []string{"mcpgw.*", "mcpgw.>", "mcp gw", "mcpgw.", "mcpgw..acme"} {
		_, err = Parse([]byte(`{"nats":{"queueGroup":"` + bad + `"},"servers":{}}`))
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "nats.queueGroup", bad)
	}
}

// auth.subject is the front of a PUBLISH subject
// ({subject}.{tenant}.{user}.{server}), and NATS refuses to publish to a
// subject containing a wildcard. Unvalidated, the failure surfaces on the
// first credential resolution as "no responders available" — which sends the
// operator looking for a missing controller rather than at their own config.
func TestAuthSubjectValidation(t *testing.T) {
	cfg, err := Parse([]byte(`{"servers":{"gh":{"command":"x","auth":{"mode":"nats","subject":"ctl.creds"}}}}`))
	require.NoError(t, err)
	assert.Equal(t, "ctl.creds", cfg.Servers["gh"].Auth.Subject)

	for _, bad := range []string{"mcp.v1.cred.>", "mcp.v1.*.cred", "mcp.v1.cred.", "mcp v1.cred"} {
		_, err = Parse([]byte(`{"servers":{"gh":{"command":"x","auth":{"mode":"nats","subject":"` + bad + `"}}}}`))
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "auth subject", bad)
	}
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

// time.ParseDuration accepts "-30m", so a negative duration used to validate
// clean and then disappear: every consumer of these fields reads <= 0 as
// "unset" and substitutes its own default (pool 5m/1h, cred file 1m, claim
// bucket 5m). The operator who wrote a leading "-" got the default and no
// signal that their setting had been discarded. Zero stays legal — it IS the
// documented "use the default" sentinel ("0 = pool default, 5m") — but a
// negative one cannot be anything but a mistake.
func TestNegativeDurationsRejected(t *testing.T) {
	for _, tc := range []struct {
		doc     string
		wantErr string
	}{
		{`{"pool":{"idleTtl":"-30m"},"servers":{}}`, "pool.idleTtl"},
		{`{"pool":{"maxLifetime":"-1h"},"servers":{}}`, "pool.maxLifetime"},
		{`{"claimCheck":{"maxAge":"-5m"},"servers":{}}`, "claimCheck.maxAge"},
		{`{"servers":{"gh":{"command":"x","auth":{"mode":"file","path":"/c","ttl":"-10s"}}}}`, "auth ttl"},
	} {
		_, err := Parse([]byte(tc.doc))
		require.Error(t, err, tc.doc)
		assert.Contains(t, err.Error(), tc.wantErr)
		assert.Contains(t, err.Error(), "negative", tc.doc)
	}

	// Zero and positive values are untouched.
	_, err := Parse([]byte(`{"pool":{"idleTtl":"0s","maxLifetime":"24h"},"claimCheck":{"maxAge":"0"},"servers":{}}`))
	require.NoError(t, err)
}

func TestCacheScopeValidation(t *testing.T) {
	server := func(extra string) string {
		return `{"servers":{"gh":{"transport":"stdio","command":"x"` + extra + `}}}`
	}
	tests := []struct {
		name    string
		extra   string
		wantErr string
	}{
		{name: "absent", extra: ""},
		{name: "private", extra: `,"cacheScope":"private"`},
		{name: "public", extra: `,"cacheScope":"public"`},
		{
			// Not defaulted to private: a typo silently becoming "public"
			// would license shared caches across authorization contexts.
			name:    "unknown value",
			extra:   `,"cacheScope":"shared"`,
			wantErr: "unknown cacheScope",
		},
		{
			// The gateway can only stamp results it bridges. Accepting the
			// field on a modern server would let an operator believe they had
			// constrained sharing when nothing reads the setting.
			name:    "rejected on a modern server",
			extra:   `,"protocol":"2026-07-28","cacheScope":"private"`,
			wantErr: "applies only to bridged",
		},
		{name: "allowed on an explicit legacy server", extra: `,"protocol":"2025-11-25","cacheScope":"public"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(write(t, server(tc.extra)))
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
