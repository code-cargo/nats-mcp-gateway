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

package mcpspec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSupportedProtocolVersion(t *testing.T) {
	assert.True(t, IsSupportedProtocolVersion(ProtocolVersion), "the primary version is supported")
	assert.False(t, IsSupportedProtocolVersion(LegacyProtocolVersion), "legacy is bridged, not accepted on the wire")
	assert.False(t, IsSupportedProtocolVersion("2099-01-01"), "an unknown version is rejected")
	assert.False(t, IsSupportedProtocolVersion(""), "an empty version is rejected")
}

// The set is designed to hold more than one revision so clients and servers
// can upgrade independently of the gateway fleet. Prove the machinery accepts
// every listed version, using a temporary extra revision.
func TestSupportedSetAcceptsMultipleVersions(t *testing.T) {
	const future = "2099-01-01"
	orig := SupportedProtocolVersions
	SupportedProtocolVersions = []string{ProtocolVersion, future}
	t.Cleanup(func() { SupportedProtocolVersions = orig })

	assert.True(t, IsSupportedProtocolVersion(ProtocolVersion))
	assert.True(t, IsSupportedProtocolVersion(future))
	assert.False(t, IsSupportedProtocolVersion(LegacyProtocolVersion))
}
