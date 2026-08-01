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

	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
)

// JSON's null unmarshals into a map without complaint and leaves it nil, so
// the _meta injection that follows wrote to a nil map and took the process
// down. Every other non-object --params is rejected on the way in; null was
// the one that got past the check and panicked instead.
func TestCallAcceptsNullParams(t *testing.T) {
	_, url := natstest.Run(t, nil)
	call := func(params string) error {
		// Nothing serves this subject, so the request ends at no-responders —
		// far enough to prove --params was handled.
		return runCall(&CallCmd{
			Server: "fake", Method: "tools/list", Params: params,
			NatsURL: url, Tenant: "acme", User: "_",
		}, &Globals{})
	}

	assert.NotPanics(t, func() {
		require.NoError(t, call("null"), "null params means no params, as it does in the shim")
	})

	// The reason null slipped through in the first place: everything else that
	// is not an object is still refused here.
	for _, params := range []string{"5", `"s"`, "[]", "true"} {
		assert.Error(t, call(params), "--params %s is not an object", params)
	}
}
