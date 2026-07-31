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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A typo in a variable name must fail the load. Defaulting it to "" runs the
// backend with an empty credential, which surfaces much later as an opaque 401
// from the backend rather than as a config error naming the variable.
func TestExpandRejectsUndefinedVariable(t *testing.T) {
	_, err := Parse([]byte(`{"servers":{"gh":{"command":"x","env":{"GITHUB_TOKEN":"${TEST_GH_TOKN}"}}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TEST_GH_TOKN", "the error must name the variable to fix")
	assert.Contains(t, err.Error(), "GITHUB_TOKEN", "and where in the document it appears")
}

// Only ${VAR} is a reference. A bare $ belongs to the value — passwords and
// shell-ish arguments contain them — and must survive untouched.
func TestExpandLeavesBareDollarLiteral(t *testing.T) {
	cfg, err := Parse([]byte(`{"servers":{"gh":{"command":"x","args":["--fmt=$HOME"],` +
		`"env":{"PASSWORD":"s$cret","TRAILING":"ends$"}}}}`))
	require.NoError(t, err)
	assert.Equal(t, "s$cret", cfg.Servers["gh"].Env["PASSWORD"])
	assert.Equal(t, "ends$", cfg.Servers["gh"].Env["TRAILING"])
	assert.Equal(t, []string{"--fmt=$HOME"}, cfg.Servers["gh"].Args)
}

// $$ is the escape, so a value that must contain a literal ${...} is
// expressible instead of being an unavoidable "undefined variable" error.
func TestExpandDoubleDollarEscape(t *testing.T) {
	cfg, err := Parse([]byte(`{"servers":{"gh":{"command":"x","env":{"TEMPLATE":"$${NOT_A_VAR}","CASH":"$$"}}}}`))
	require.NoError(t, err)
	assert.Equal(t, "${NOT_A_VAR}", cfg.Servers["gh"].Env["TEMPLATE"])
	assert.Equal(t, "$", cfg.Servers["gh"].Env["CASH"])
}

// An expanded value is data, not document text: quotes and backslashes are
// exactly what a PEM key or a generated password contains, and they must
// arrive at the backend byte-for-byte rather than breaking the parse or being
// silently reinterpreted as JSON escapes.
func TestExpandValueWithQuoteAndBackslash(t *testing.T) {
	const pem = "-----BEGIN KEY-----\\nMIIB\"quoted\"\\n-----END KEY-----"
	t.Setenv("TEST_PEM", pem)
	cfg, err := Parse([]byte(`{"servers":{"gh":{"command":"x","env":{"KEY":"${TEST_PEM}"}}}}`))
	require.NoError(t, err)
	assert.Equal(t, pem, cfg.Servers["gh"].Env["KEY"])
}

// The reason expansion happens per field after decode: a variable's VALUE must
// never be able to add or replace fields in the document. Here the value ends
// the url string and appends a transport/command pair, turning an HTTP server
// into a subprocess spawn of the attacker's binary.
func TestExpandValueCannotInjectStructure(t *testing.T) {
	const injection = `https://weather.internal/mcp", "transport": "stdio", "command": "/bin/evil`
	t.Setenv("TEST_URL", injection)
	cfg, err := Parse([]byte(`{"servers":{"gh":{"transport":"http","url":"${TEST_URL}"}}}`))
	require.NoError(t, err)
	assert.Equal(t, injection, cfg.Servers["gh"].URL, "the value stays one opaque string")
	assert.Equal(t, "http", cfg.Servers["gh"].Transport)
	assert.Empty(t, cfg.Servers["gh"].Command, "no field the document did not declare")
}

// Expansion is the credential-injection point for documents the OPERATOR
// authored (file and inline). A fetched document comes from the controller,
// which holds the secrets itself; expanding it would let the control plane name
// any variable in the gateway pod's environment and read it back out through a
// backend argument.
func TestFetchedDoesNotExpandGatewayEnvironment(t *testing.T) {
	t.Setenv("GATEWAY_SECRET", "leak-me-not")
	raw := []byte(`{"servers":{"gh":{"command":"x","env":{"TOKEN":"${GATEWAY_SECRET}"}}}}`)

	_, err := ParseFetched(raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GATEWAY_SECRET", "the error must name the reference to remove")
	assert.NotContains(t, err.Error(), "leak-me-not", "and must not echo what it refused to read")

	// The same document from a source the operator authored is the documented
	// credential-injection point, so it expands.
	cfg, err := ParseInline(raw)
	require.NoError(t, err)
	assert.Equal(t, "leak-me-not", cfg.Servers["gh"].Env["TOKEN"])
}

// The escape works on the fetch path too, so a fetched document can still carry
// a value that legitimately contains "${" — the two paths differ only in
// whether a reference resolves.
func TestFetchedKeepsEscapedLiteral(t *testing.T) {
	cfg, err := ParseFetched([]byte(`{"servers":{"gh":{"command":"x","args":["$${TEMPLATE}"]}}}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"${TEMPLATE}"}, cfg.Servers["gh"].Args)
}

// Expansion reaches every string in the schema, not just the two the README
// puts in its example: an operator writing a credential into a NATS URL or a
// nested auth field is relying on the same mechanism.
func TestExpandReachesEveryStringField(t *testing.T) {
	t.Setenv("TEST_VAL", "resolved")
	cfg, err := Parse([]byte(`{
		"nats": {"url": "nats://gw:${TEST_VAL}@nats:4222", "queueGroup": "${TEST_VAL}"},
		"pool": {"idleTtl": "5m"},
		"servers": {"gh": {
			"transport": "http",
			"url": "https://${TEST_VAL}/mcp",
			"headers": {"Authorization": "Bearer ${TEST_VAL}"},
			"auth": {"mode": "oauth-client-credentials", "tokenUrl": "https://idp/t",
			         "clientId": "${TEST_VAL}", "clientSecret": "${TEST_VAL}"}
		}}
	}`))
	require.NoError(t, err)
	assert.Equal(t, "nats://gw:resolved@nats:4222", cfg.NATS.URL)
	assert.Equal(t, "resolved", cfg.NATS.QueueGroup)
	assert.Equal(t, "https://resolved/mcp", cfg.Servers["gh"].URL)
	assert.Equal(t, "Bearer resolved", cfg.Servers["gh"].Headers["Authorization"])
	assert.Equal(t, "resolved", cfg.Servers["gh"].Auth.ClientID)
	assert.Equal(t, "resolved", cfg.Servers["gh"].Auth.ClientSecret)
}

// Expansion applies to values only. A key is the name the runtime looks
// something up by, so a reference in one is rejected rather than passed
// through as a literal nobody would notice until a credential went missing.
func TestExpandRejectsReferenceInKey(t *testing.T) {
	t.Setenv("TEST_VAL", "resolved")
	_, err := Parse([]byte(`{"servers":{"gh":{"command":"x","env":{"${TEST_VAL}":"v"}}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in keys")
}

// A malformed reference is an error, not a best-effort splice: os.Expand's
// habit of eating "${" as bad syntax is how a truncated value reaches a
// backend looking almost right.
func TestExpandRejectsMalformedReference(t *testing.T) {
	for _, bad := range []string{"${TEST_VAL", "${}"} {
		_, err := Parse([]byte(`{"servers":{"gh":{"command":"x","env":{"K":"` + bad + `"}}}}`))
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), `servers["gh"].env["K"]`, bad)
	}
}
