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

package cmd

import (
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseCLI runs args through the real Kong tree, so a test sees exactly what
// the binary would.
func parseCLI(t *testing.T, args ...string) *CLI {
	t.Helper()
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Name("natsmcp"), kong.Vars{"version": "test"})
	require.NoError(t, err)
	_, err = parser.Parse(args)
	require.NoError(t, err)
	return cli
}

// The credentials env var was named NATSMCP_NATS_CREDS on the gateway and
// NATSMCP_CREDS on the shim and call. Setting the one you had read about on
// the other subcommand did not fail — it was ignored, and the process
// connected with no credentials at all, which against a server that permits
// anonymous connections means a shim that runs and quietly has no identity.
// Both names are accepted everywhere; neither is renamed, because a
// documented variable that stops working is a broken deployment.
func TestCredsEnvVarAcceptedUnderBothNames(t *testing.T) {
	for _, name := range []string{"NATSMCP_NATS_CREDS", "NATSMCP_CREDS"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "/creds/user.creds")
			cli := parseCLI(t, "gateway", "--config-subject", "cfg.get")
			assert.Equal(t, "/creds/user.creds", cli.Gateway.NatsCreds)

			cli = parseCLI(t, "shim", "--server", "s")
			assert.Equal(t, "/creds/user.creds", cli.Shim.Creds)

			cli = parseCLI(t, "call", "--server", "s", "--method", "tools/list")
			assert.Equal(t, "/creds/user.creds", cli.Call.Creds)
		})
	}
}

// Each subcommand's own documented name wins where both are set, so adding
// the alias cannot change what an existing deployment resolves to.
func TestCredsEnvVarPrefersTheSubcommandsOwnName(t *testing.T) {
	t.Setenv("NATSMCP_NATS_CREDS", "/creds/gateway.creds")
	t.Setenv("NATSMCP_CREDS", "/creds/client.creds")

	cli := parseCLI(t, "gateway", "--config-subject", "cfg.get")
	assert.Equal(t, "/creds/gateway.creds", cli.Gateway.NatsCreds)

	cli = parseCLI(t, "shim", "--server", "s")
	assert.Equal(t, "/creds/client.creds", cli.Shim.Creds)
}
