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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Reading a request's params is an authorization operation, and it has one
// rule: read the same bytes the backend will read, the same way.
//
// The gateway authorizes a request by proving the subject NATS enforced
// agrees with the body it is about to forward — and it forwards params
// VERBATIM (pkg/backend.Mux.Call reuses the raw bytes). So the question a
// probe here answers is never "what does Go think params.name is". It is
// "what will the Python, TypeScript or Java MCP server on the other end read
// out of these exact bytes". Anywhere those two answers can differ is an
// authorization bypass, because the check and the execution disagree about
// what is being called.
//
// Two ways to make them differ, both of which were live in this codebase:
//
//   - A tagged Go struct. encoding/json matches object keys to struct fields
//     with a CASE-INSENSITIVE fallback, so a `json:"name"` field decodes
//     {"name":"delete_repo","NAME":"get_issue"} to "get_issue" while every
//     case-sensitive parser downstream reads "delete_repo". A caller granted
//     only tools.call.get_issue could therefore publish that body to its
//     authorized subject, pass the check on the forged NAME, and have the
//     backend run delete_repo. A map-typed field is worse still: _META and
//     _meta MERGE rather than shadow, so a forged _META satisfies the
//     protocol-version gate while the _meta the backend sees stays empty.
//     Hence Params is a map — its keys match exactly, like everyone else's.
//
//   - Duplicate keys. {"name":"a","name":"b"} has no defined meaning in
//     RFC 8259. Go, JavaScript, Python and Jackson all happen to take the
//     last one, but the gateway does not choose the backend's parser and an
//     ACL must not rest on a coincidence. DecodeParams rejects them outright.
//
// Reading keys exactly closes the bypass in one direction. It does not close
// the other: .NET's System.Text.Json matches case-insensitively by default
// under ASP.NET Core, so on such a backend {"name":"get_issue","NAME":"del"}
// executes del while an exact-key gateway happily authorizes get_issue. The
// gateway cannot know which parser is on the far end, so it refuses keys that
// differ only by case — Unicode folding, not lowercasing, since ſ folds to s.
//
// How widely that refusal applies depends on who owns the field names:
//
//   - params itself: the whole object, because 2026-07-28 fixes its fields.
//     Two keys there differing only by case are never both meant.
//   - _meta, and tool arguments: only the key actually being read, checked
//     against its siblings at the point of reading (Params.Raw, and
//     pkg/backend.valueAtPath for arguments). Both are open extension bags —
//     a third party's "com.acme/trace" and "com.acme/Trace" are two legal
//     keys the gateway never looks at, and failing the request over them
//     would reject conformant traffic to no one's benefit.
//
// Everything downstream of a successful DecodeParams may assume the document
// has no duplicate key at any depth, and that no key it goes on to READ has a
// case-colliding sibling.

// MetaField is the params key holding a request's _meta object.
const MetaField = "_meta"

// MaxParamsDepth bounds how deep the ambiguity scan will descend. Nesting is
// caller-controlled and the scan recurses, so it needs a floor under it;
// 1000 is far past anything a tool schema describes and far short of
// anything that troubles a goroutine stack.
const MaxParamsDepth = 1000

// ErrParamsNotObject reports params that are present but are not a JSON
// object. JSON-RPC permits an array here; MCP does not, and the gateway
// cannot name a tool inside one.
var ErrParamsNotObject = errors.New("params is not a JSON object")

// ErrParamsTooDeep reports params nested past MaxParamsDepth.
var ErrParamsTooDeep = fmt.Errorf("params nests deeper than %d levels", MaxParamsDepth)

// AmbiguousKeyError reports two keys in one object that some real parser
// will not distinguish: the same key twice, or two keys differing only by
// letter case. It is a distinct type because it is the one decode failure
// the proxy answers as an integrity mismatch rather than a malformed
// request — the body is valid JSON, it just does not have one meaning.
type AmbiguousKeyError struct {
	// Key is the offending key; Other is the one it collides with, equal to
	// Key when the same key simply appears twice.
	Key, Other string
}

func (e *AmbiguousKeyError) Error() string {
	if e.Key == e.Other {
		return fmt.Sprintf("duplicate key %q: the body has no single meaning", e.Key)
	}
	return fmt.Sprintf("keys %q and %q differ only by case: the body has no single meaning",
		e.Other, e.Key)
}

// Params is a decoded params object. Keys are matched EXACTLY — never with
// encoding/json's case-insensitive struct-field fallback — because exact is
// what the backend will do.
type Params map[string]json.RawMessage

// DecodeParams decodes a request's raw params. Absent, empty and null params
// all yield an empty Params, which is the same nothing they mean to a
// backend. Anything that is not an object, repeats a key at any depth, nests
// past MaxParamsDepth, or names two top-level fields that differ only by
// case is an error.
func DecodeParams(raw json.RawMessage) (Params, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return Params{}, nil
	}
	if err := scanUnambiguous(raw); err != nil {
		return nil, err
	}
	var p Params
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, ErrParamsNotObject
	}
	if p == nil { // literal null
		return Params{}, nil
	}
	if err := p.checkCaseUnique(); err != nil {
		return nil, err
	}
	return p, nil
}

// EqualFoldKey reports whether two JSON object keys are the same key to a
// parser that folds case. It is the single definition of "differs only by
// case" this gateway uses — pkg/backend applies the same relation to the
// arguments it mirrors into headers.
//
// Unicode simple folding, not lowercasing. The difference is not academic:
// U+017F (ſ, long s) lowercases to itself but folds to "s", so a ToLower
// comparison reads "argumentſ" as distinct from "arguments" while a
// case-folding parser binds both to the same field.
func EqualFoldKey(a, b string) bool { return strings.EqualFold(a, b) }

// foldKey canonicalizes a key for collision detection, mapping every rune to
// the lowest member of its Unicode fold orbit. It exists so a set of keys can
// be checked in one pass instead of pairwise, and it agrees with
// EqualFoldKey exactly — TestFoldKeyAgreesWithEqualFold pins that.
func foldKey(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		lowest := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			if f < lowest {
				lowest = f
			}
		}
		b.WriteRune(lowest)
	}
	return b.String()
}

// checkCaseUnique rejects two keys in p differing only by case. See the
// package comment: exact-key reads protect against a case-sensitive backend,
// and this protects against a case-folding one.
func (p Params) checkCaseUnique() error {
	folded := make(map[string]string, len(p))
	for key := range p {
		if prior, clash := folded[foldKey(key)]; clash {
			// Sorted so the message does not depend on Go's map iteration
			// order, which would make the error text nondeterministic.
			a, b := min(prior, key), max(prior, key)
			return &AmbiguousKeyError{Key: b, Other: a}
		}
		folded[foldKey(key)] = key
	}
	return nil
}

// unambiguous reports whether key can be read out of p without a sibling
// that some parser would bind to the same field.
//
// This is the rule for objects whose field names the gateway does NOT own:
// only the key actually being read has to be unambiguous. _meta is MCP's
// open extension bag, so a third party's "com.acme/trace" and
// "com.acme/Trace" are two legal keys the gateway never looks at, and
// refusing the whole request over them would reject conformant traffic.
// params itself is held to the stricter whole-object rule instead, because
// there the specification fixes the field set.
func (p Params) unambiguous(key string) error {
	for other := range p {
		if other != key && EqualFoldKey(other, key) {
			a, b := min(other, key), max(other, key)
			return &AmbiguousKeyError{Key: b, Other: a}
		}
	}
	return nil
}

// String returns the string at key. An absent key, and a key whose value is
// JSON null, both yield "" — a backend reads nothing from either. A key
// present with some other type is an error rather than "", because "" is
// what the caller compares against a header and treats as "no name given".
func (p Params) String(key string) (string, error) {
	raw, ok, err := p.Raw(key)
	if err != nil || !ok {
		return "", err
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("params %q is not a string", key)
	}
	return s, nil
}

// Raw returns the undecoded value at key, and whether it was present. Reading
// a key is where its case-uniqueness is enforced, so callers that reach for
// the raw bytes get the same protection as those that decode.
func (p Params) Raw(key string) (json.RawMessage, bool, error) {
	if err := p.unambiguous(key); err != nil {
		return nil, false, err
	}
	raw, ok := p[key]
	return raw, ok, nil
}

// Object returns the nested object at key, empty when the key is absent or
// null. The returned object's own keys are checked as they are read, not up
// front — see unambiguous for why _meta cannot be held to the whole-object
// rule.
func (p Params) Object(key string) (Params, error) {
	raw, ok, err := p.Raw(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return Params{}, nil
	}
	var obj Params
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("params %q is not a JSON object", key)
	}
	if obj == nil {
		return Params{}, nil
	}
	return obj, nil
}

// NameField returns the params key carrying method's MCP name — the value
// the subject's name token and the Mcp-Name header both mirror — or "" for
// the methods that carry no name.
//
// It exists so that there is exactly one copy of this switch. Three copies
// is how the proxy's authorization check and the HTTP backend's header
// derivation come to disagree about which field names a call.
func NameField(method string) string {
	switch method {
	case MethodToolsCall, MethodPromptsGet:
		return "name"
	case MethodResourcesRead:
		return "uri"
	}
	return ""
}

// Name returns the MCP name this request carries in params, and whether the
// method carries one at all. A named method with no name present returns
// ("", true, nil) — absent and empty are the same failure to the caller.
func (p Params) Name(method string) (name string, named bool, err error) {
	field := NameField(method)
	if field == "" {
		return "", false, nil
	}
	name, err = p.String(field)
	return name, true, err
}

// ProtocolVersion reads params._meta[MetaProtocolVersion], "" when absent.
func (p Params) ProtocolVersion() (string, error) {
	meta, err := p.Object(MetaField)
	if err != nil {
		return "", err
	}
	ver, err := meta.String(MetaProtocolVersion)
	if err != nil {
		var ambiguous *AmbiguousKeyError
		if errors.As(err, &ambiguous) {
			return "", err
		}
		return "", fmt.Errorf("_meta %q is not a string", MetaProtocolVersion)
	}
	return ver, nil
}

// scanUnambiguous walks the whole document rejecting any object that repeats
// a key. The WHOLE document, not just the top level: params._meta carries the
// protocol version and params.arguments is mirrored into Mcp-Param-* headers
// for HTTP backends, so ambiguity at any depth is ambiguity in something the
// gateway acts on.
//
// It is a byte scan rather than the obvious loop over json.Decoder.Token(),
// because this runs on every request before anything else does and params on
// a tools/call is bounded only by max_payload. Token() boxes every token it
// returns, which made the pass allocation-dominated and about five times the
// cost of the parse it precedes — a caller-controlled multiplier on the
// gateway's cheapest-to-reach code path. Here only object KEYS are
// materialized; values are stepped over without being interpreted, which is
// all a duplicate-key check ever needed.
//
// scanValueTokens in the tests is the readable Token()-based statement of the
// same rule, and FuzzScanUnambiguous holds the two to identical verdicts.
func scanUnambiguous(raw []byte) error {
	// One frame per open container, innermost last. A nil keys map marks an
	// array, where strings are values and repetition is legal.
	type frame struct {
		keys      map[string]struct{}
		expectKey bool
	}
	var stack []frame
	top := func() *frame {
		if len(stack) == 0 {
			return nil
		}
		return &stack[len(stack)-1]
	}

	for i := 0; i < len(raw); {
		switch c := raw[i]; c {
		case ' ', '\t', '\n', '\r':
			i++
		case '{', '[':
			if len(stack) >= MaxParamsDepth {
				return ErrParamsTooDeep
			}
			f := frame{expectKey: c == '{'}
			if c == '{' {
				f.keys = map[string]struct{}{}
			}
			stack = append(stack, f)
			i++
		case '}', ']':
			if len(stack) == 0 {
				return ErrParamsNotObject
			}
			stack = stack[:len(stack)-1]
			i++
		case ',':
			if f := top(); f != nil && f.keys != nil {
				f.expectKey = true
			}
			i++
		case ':':
			if f := top(); f != nil {
				f.expectKey = false
			}
			i++
		case '"':
			key, next, err := scanString(raw, i)
			if err != nil {
				return err
			}
			if f := top(); f != nil && f.keys != nil && f.expectKey {
				if _, dup := f.keys[key]; dup {
					return &AmbiguousKeyError{Key: key, Other: key}
				}
				f.keys[key] = struct{}{}
			}
			i = next
		default:
			// A number, true, false or null. Nothing inside one can be a key,
			// and its VALUE is never read here, so step over it without
			// interpreting it — this is where the speed comes from.
			j := i
			for j < len(raw) && !isJSONStructural(raw[j]) {
				j++
			}
			if j == i {
				return ErrParamsNotObject // unreachable on valid JSON
			}
			i = j
		}
	}
	if len(stack) != 0 {
		return ErrParamsNotObject // truncated
	}
	return nil
}

func isJSONStructural(c byte) bool {
	switch c {
	case '{', '}', '[', ']', ',', ':', '"', ' ', '\t', '\n', '\r':
		return true
	}
	return false
}

// scanString reads the JSON string starting at raw[i] (which must be the
// opening quote) and returns its DECODED value and the index just past the
// closing quote.
//
// Decoded, not raw, because two keys can be distinct byte strings and the
// same key to a parser. `"a"` and `"a"` is the obvious pair. The one a
// fuzzer finds is invalid UTF-8: encoding/json substitutes U+FFFD for every
// unreadable byte, so `{"\xf0":1,"\x80":2}` is two keys here and one key to
// a Go backend — ambiguity by exactly the definition this scan exists to
// reject. Both cases route through json.Unmarshal so the answer is the
// decoder's own, not a second guess at its rules.
func scanString(raw []byte, i int) (string, int, error) {
	escaped := false
	for j := i + 1; j < len(raw); j++ {
		switch raw[j] {
		case '\\':
			escaped = true
			j++ // the escaped byte cannot end the string
		case '"':
			if !escaped && utf8.Valid(raw[i+1:j]) {
				return string(raw[i+1 : j]), j + 1, nil
			}
			var s string
			if err := json.Unmarshal(raw[i:j+1], &s); err != nil {
				return "", 0, ErrParamsNotObject
			}
			return s, j + 1, nil
		}
	}
	return "", 0, ErrParamsNotObject // unterminated
}
