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

// Both prefix flags are checked at the cmd layer, which is what names the flag
// that is wrong. wire.NewClient rejects a malformed prefix too, but its error
// names the wire, not the setting the operator typed — and this process reads
// its settings from an .mcp.json entry, where nobody is watching a boot log.
//
// Both checks also run BEFORE the connect, so a malformed prefix is named
// without a NATS server in the picture. That is what this test pins: the error
// arrives from a shim pointed at a port nothing is listening on.
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
