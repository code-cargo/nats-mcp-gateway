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
	"encoding/base64"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Value Encoding for mirrored header values (Mcp-Name, Mcp-Param-{Name}).
//
// Tool and prompt names are only SHOULD-constrained to header-safe characters,
// and resource URIs are not constrained at all, so a value that must ride in a
// header cannot be assumed representable. The spec's answer is a sentinel:
//
//	Mcp-Name: =?base64?SGVsbG8sIOS4lueVjA==?=
//
// Both markers are lowercase and case-sensitive. Servers MUST decode before
// comparing a header to the corresponding body value — a raw comparison
// rejects a conformant client, which is why pkg/proxy decodes rather than
// string-matching.

const (
	// sentinelPrefix and sentinelSuffix delimit a base64-encoded header value.
	// Exact bytes, lowercase: the spec makes these case-sensitive so that a
	// literal value differing only in case is not mistaken for an encoding.
	sentinelPrefix = "=?base64?"
	sentinelSuffix = "?="
)

// EncodeHeaderValue returns s as a header value, applying the base64 sentinel
// encoding when s cannot ride in a header as-is. Values that are already safe
// pass through untouched, so the common case stays human-readable on the wire.
func EncodeHeaderValue(s string) string {
	if !needsHeaderEncoding(s) {
		return s
	}
	return sentinelPrefix + base64.StdEncoding.EncodeToString([]byte(s)) + sentinelSuffix
}

// DecodeHeaderValue reverses EncodeHeaderValue. A value that is not in
// sentinel form is returned verbatim — that is the overwhelmingly common case
// and is not an error.
func DecodeHeaderValue(v string) (string, error) {
	inner, ok := strings.CutPrefix(v, sentinelPrefix)
	if !ok {
		return v, nil
	}
	inner, ok = strings.CutSuffix(inner, sentinelSuffix)
	if !ok {
		// Starts like a sentinel but never closes. Not an encoding, and not
		// ours to repair: hand it back and let the comparison fail loudly.
		return v, nil
	}
	// '?' is outside the base64 alphabet, so the first "?=" is necessarily the
	// terminator. Anything else in there means the value was mangled.
	if strings.ContainsRune(inner, '?') {
		return "", fmt.Errorf("malformed base64 sentinel: stray %q in payload", "?")
	}
	raw, err := base64.StdEncoding.DecodeString(inner)
	if err != nil {
		return "", fmt.Errorf("malformed base64 sentinel: %w", err)
	}
	if !utf8.Valid(raw) {
		return "", fmt.Errorf("malformed base64 sentinel: payload is not valid UTF-8")
	}
	return string(raw), nil
}

// needsHeaderEncoding reports whether s must be sentinel-encoded.
//
// RFC 9110 field values admit visible ASCII (0x21-0x7E), space and horizontal
// tab; the spec additionally calls out leading/trailing whitespace as unsafe,
// since it is not preserved across intermediaries. Tab is legal mid-value but
// encoded here anyway — it survives the round trip either way, and one rule
// ("printable ASCII, no edge whitespace") is easier to hold than two.
func needsHeaderEncoding(s string) bool {
	if s == "" {
		return false
	}
	// A literal that looks like an encoding MUST be encoded, or a decoder
	// would unwrap a value the sender never wrapped.
	if strings.HasPrefix(s, sentinelPrefix) && strings.HasSuffix(s, sentinelSuffix) {
		return true
	}
	if s != strings.TrimSpace(s) {
		return true
	}
	// Invalid UTF-8 decodes to RuneError, which is itself outside the safe
	// range, so malformed input encodes rather than escaping unnoticed.
	return strings.IndexFunc(s, func(r rune) bool { return r < 0x20 || r > 0x7E }) >= 0
}
