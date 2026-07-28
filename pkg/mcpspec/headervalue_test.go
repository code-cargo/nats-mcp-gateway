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
	"github.com/stretchr/testify/require"
)

func TestEncodeHeaderValue(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// Left alone: encoding every value would make the wire unreadable for
		// no gain, and these all survive a header round trip as-is.
		{"plain ascii", "get_weather", "get_weather"},
		{"punctuation", "file:///projects/app/config.json", "file:///projects/app/config.json"},
		{"empty", "", ""},

		// The spec's own examples.
		{"non-ascii", "Hello, 世界", "=?base64?SGVsbG8sIOS4lueVjA==?="},
		{"padded", " padded ", "=?base64?IHBhZGRlZCA=?="},
		{"newline", "line1\nline2", "=?base64?bGluZTEKbGluZTI=?="},
		{"sentinel literal", "=?base64?literal?=", "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?="},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, EncodeHeaderValue(tc.in))
		})
	}
}

func TestHeaderValueRoundTrip(t *testing.T) {
	// Whatever went in must come back out byte-identical, because the decoded
	// value is compared against the request body — a lossy round trip would
	// surface as a spurious -32020 rather than as corruption.
	for _, s := range []string{
		"get_weather",
		"crème.brûlée",
		"file:///projects/app/config.json",
		"tab\there",
		"  ",
		"=?base64?literal?=",
		"Hello, 世界",
		"emoji 🎯 name",
	} {
		got, err := DecodeHeaderValue(EncodeHeaderValue(s))
		require.NoError(t, err, "round trip of %q", s)
		assert.Equal(t, s, got)
	}
}

func TestDecodeHeaderValuePassesThroughPlainValues(t *testing.T) {
	// Not sentinel-shaped, so not an encoding — and emphatically not an error.
	for _, s := range []string{"get_weather", "", "=?base64?unterminated", "?=", "=?BASE64?QQ==?="} {
		got, err := DecodeHeaderValue(s)
		require.NoError(t, err, "value %q", s)
		assert.Equal(t, s, got)
	}
}

func TestDecodeHeaderValueRejectsMalformedSentinels(t *testing.T) {
	for _, s := range []string{
		"=?base64?not!valid!base64?=",
		"=?base64?QQ==?QQ==?=", // stray '?' inside the payload
		"=?base64?/w==?=",      // decodes to 0xFF: valid base64, invalid UTF-8
	} {
		_, err := DecodeHeaderValue(s)
		assert.Error(t, err, "value %q must be rejected, not silently mangled", s)
	}
}

func TestSentinelMarkersAreCaseSensitive(t *testing.T) {
	// The spec pins the markers as lowercase. An uppercase lookalike is a
	// literal value, and treating it as an encoding would decode something
	// the sender never encoded.
	got, err := DecodeHeaderValue("=?BASE64?SGk=?=")
	require.NoError(t, err)
	assert.Equal(t, "=?BASE64?SGk=?=", got)
}
