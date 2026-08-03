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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The shim's wire prefix gets the same check as the gateway's. The failure it
// prevents is milder — a wildcard prefix builds a publish subject NATS refuses,
// so nothing is widened — but it arrives once per request, as an opaque publish
// error, from a process whose entire job is to look like a local MCP server to
// the client that spawned it. Both checks run BEFORE the connect, so a
// malformed prefix is named without a NATS server in the picture.
func TestShimValidatesPrefixesBeforeConnecting(t *testing.T) {
	for _, bad := range []string{"mcp.*", "mcp.>", "mcp v1", "mcp.v1.", ".mcp.v1", "mcp..v1"} {
		err := runShim(&ShimCmd{
			Server:        "gh",
			NatsURL:       "nats://127.0.0.1:" + closedPort(t),
			SubjectPrefix: bad,
		}, &Globals{LogLevel: "error"})
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "--subject-prefix", bad)

		err = runShim(&ShimCmd{
			Server:      "gh",
			NatsURL:     "nats://127.0.0.1:" + closedPort(t),
			InboxPrefix: bad,
		}, &Globals{LogLevel: "error"})
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "--inbox-prefix", bad)
	}
}
