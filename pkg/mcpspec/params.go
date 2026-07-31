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
	"slices"
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
// differ only by case, under a relation wide enough to cover every rule a real
// backend applies — see EqualFoldKey, which is neither plain lowercasing nor
// plain folding.
//
// How widely that refusal applies depends on who owns the field names:
//
//   - params itself: the whole object, because 2026-07-28 fixes its fields.
//     Two keys there differing only by case are never both meant.
//   - _meta, and tool arguments: only keys the gateway actually reads, held
//     to the rule where they are read (Params.Raw, CheckReadableMeta, and
//     pkg/backend.valueAtPath for arguments). Both are open extension bags —
//     a third party's "com.acme/trace" and "com.acme/Trace" are two legal
//     keys the gateway never looks at, and failing the request over them
//     would reject conformant traffic to no one's benefit.
//
// Everything downstream of a successful DecodeParams may assume the document
// has no duplicate key at any depth.
//
// Freedom from case collisions is narrower, and is NOT automatic. params has
// it outright. Inside _meta and tool arguments it holds only for keys someone
// DECLARED, so reading a new key out of either means declaring it — in
// metaKeysRead for _meta, or along the walked path for arguments. An
// undeclared read is not a compile error and not a test failure; it is a key
// the gateway acts on and a backend may resolve differently, which is the
// entire bug this file exists to prevent.

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
// parser that ignores case. It is the single definition of "differs only by
// case" this gateway uses — pkg/backend applies the same relation to the
// arguments it mirrors into headers.
//
// The relation has to be a SUPERSET of every rule a real backend might apply,
// because the gateway does not know which parser is on the far end and a key
// it considers distinct is a key it will forward. Two rules are in play and
// neither contains the other:
//
//   - Unicode simple FOLDING, which Go's strings.EqualFold implements. U+017F
//     (ſ, long s) folds to "s" but lowercases to itself, so folding catches
//     "argumentſ" where a ToLower comparison does not.
//   - Per-rune case MAPPING, which Java's equalsIgnoreCase and .NET's
//     OrdinalIgnoreCase implement — and OrdinalIgnoreCase is what
//     System.Text.Json uses under ASP.NET Core, the case-insensitive backend
//     this whole file is written against. It catches the dotted/dotless I
//     family, whose members have no shared fold orbit: a .NET backend binds
//     "urı" to its uri property, and folding alone reads that as a distinct
//     key.
//
// foldRune covers both. TestFoldCoversCaseMapping enumerates all of Unicode
// to prove the exception table below is complete, so a Go release that moves
// the tables fails the build rather than silently reopening the gap.
func EqualFoldKey(a, b string) bool {
	for _, ra := range a {
		rb, size := utf8.DecodeRuneInString(b)
		if size == 0 {
			return false // b ran out first
		}
		if foldRune(ra) != foldRune(rb) {
			return false
		}
		b = b[size:]
	}
	return b == ""
}

// caseMapExceptions holds the runes where per-rune ToUpper/ToLower equates
// keys that simple folding keeps apart. Exhaustively, in Unicode 15.0.0, that
// is the dotted/dotless I family and nothing else: U+0130 lowercases to "i"
// and U+0131 uppercases to "I", but neither shares a fold orbit with either.
// Both are pinned to "I", the fold-orbit minimum of the ASCII pair.
var caseMapExceptions = map[rune]rune{
	'İ': 'I', // LATIN CAPITAL LETTER I WITH DOT ABOVE
	'ı': 'I', // LATIN SMALL LETTER DOTLESS I
}

// foldRune canonicalizes one rune: same output for two runes iff some real
// parser would treat them as the same character.
func foldRune(r rune) rune {
	if c, exceptional := caseMapExceptions[r]; exceptional {
		return c
	}
	lowest := r
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		if f < lowest {
			lowest = f
		}
	}
	return lowest
}

// foldKey canonicalizes a whole key, so a set of keys can be checked in one
// pass instead of pairwise. Equivalent to EqualFoldKey by construction.
func foldKey(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		b.WriteRune(foldRune(r))
	}
	return b.String()
}

// checkCaseUnique rejects two keys in p differing only by case. See the
// package comment: exact-key reads protect against a case-sensitive backend,
// and this protects against a case-folding one.
func (p Params) checkCaseUnique() error {
	folded := make(map[string]string, len(p))
	for key := range p {
		canonical := foldKey(key)
		if prior, clash := folded[canonical]; clash {
			// Sorted so the message does not depend on Go's map iteration
			// order, which would make the error text nondeterministic.
			a, b := min(prior, key), max(prior, key)
			return &AmbiguousKeyError{Key: b, Other: a}
		}
		folded[canonical] = key
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

// metaKeysRead are the request _meta keys this gateway acts on, and therefore
// the ones a request may not spell two ways. Everything else in _meta is a
// third party's business — see unambiguous.
//
// The list exists because the reads are NOT all in this package, so nothing
// else would keep them together. Add a key here when the gateway starts
// reading it, or CheckReadableMeta silently stops covering it.
var metaKeysRead = []string{MetaProtocolVersion, MetaProgressToken}

// CheckReadableMeta verifies that every _meta key the gateway acts on can be
// read unambiguously — no sibling a case-folding parser would bind instead.
//
// protocolVersion is read by the integrity check, a few lines from here.
// progressToken is read AND REWRITTEN much later, in pkg/backend.Mux.Call,
// which swaps the caller's token for a mux-unique one precisely because two
// concurrent callers may pick the same token. That rewrite replaces the exact
// key and leaves a "ProgressToken" sibling untouched, so a case-folding
// backend echoes the sibling back on its progress notifications — and the mux
// routes those by token to whichever call registered it, which is another
// caller sharing the tenant. Mux tokens are a counter, not a secret. Refusing
// the collision here is what keeps the rewrite meaningful.
func (p Params) CheckReadableMeta() error {
	meta, err := p.Object(MetaField)
	if err != nil {
		return err
	}
	for _, key := range metaKeysRead {
		if err := meta.unambiguous(key); err != nil {
			return err
		}
	}
	return nil
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
	var stack []scanFrame

	for i := 0; i < len(raw); {
		var top *scanFrame
		if n := len(stack); n > 0 {
			top = &stack[n-1]
		}
		switch c := raw[i]; c {
		case ' ', '\t', '\n', '\r':
			i++
		case '{', '[':
			if len(stack) >= MaxParamsDepth {
				return ErrParamsTooDeep
			}
			stack = append(stack, scanFrame{isObject: c == '{', expectKey: c == '{'})
			i++
		case '}', ']':
			if top == nil {
				return ErrParamsNotObject
			}
			stack = stack[:len(stack)-1]
			i++
		case ',':
			if top != nil && top.isObject {
				top.expectKey = true
			}
			i++
		case ':':
			if top != nil {
				top.expectKey = false
			}
			i++
		case '"':
			// Find the end first and decode only if this string is a KEY.
			// Most strings in a body are values — every field of every record
			// in a tool-call argument array — and materializing those just to
			// discard them was most of what this pass allocated.
			end, exact, err := scanStringEnd(raw, i)
			if err != nil {
				return err
			}
			if top != nil && top.isObject && top.expectKey {
				key, err := decodeStringSpan(raw, i, end, exact)
				if err != nil {
					return err
				}
				if err := top.addKey(key); err != nil {
					return err
				}
			}
			i = end
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

// smallObjectKeys is the size below which a frame compares keys by linear
// scan instead of hashing them into a map.
//
// The scan holds one frame per OPEN container, so an array of 50k small
// records opens and closes 50k of them. A map each cost more than the rest of
// the pass put together — ~10x the payload in allocations, on the path a
// caller reaches before any authorization happens. Real JSON objects are
// small and this bound is generous; beyond it the map's O(1) wins and the
// frame promotes.
const smallObjectKeys = 16

// scanFrame tracks one open container: which keys its object has already
// named, and whether the next string is a key or a value.
type scanFrame struct {
	isObject  bool
	expectKey bool
	// Exactly one of these is in use. keys is nil until the object outgrows
	// smallObjectKeys, at which point small is drained into it and abandoned.
	small []string
	keys  map[string]struct{}
}

// addKey records key, reporting an error if the object already named it.
func (f *scanFrame) addKey(key string) error {
	if f.keys != nil {
		if _, dup := f.keys[key]; dup {
			return &AmbiguousKeyError{Key: key, Other: key}
		}
		f.keys[key] = struct{}{}
		return nil
	}
	if slices.Contains(f.small, key) {
		return &AmbiguousKeyError{Key: key, Other: key}
	}
	if len(f.small) < smallObjectKeys {
		f.small = append(f.small, key)
		return nil
	}
	f.keys = make(map[string]struct{}, len(f.small)*2)
	for _, prior := range f.small {
		f.keys[prior] = struct{}{}
	}
	f.keys[key] = struct{}{}
	f.small = nil
	return nil
}

func isJSONStructural(c byte) bool {
	switch c {
	case '{', '}', '[', ']', ',', ':', '"', ' ', '\t', '\n', '\r':
		return true
	}
	return false
}

// scanStringEnd locates the end of the JSON string starting at raw[i] (which
// must be the opening quote). It returns the index just past the closing
// quote, and whether the span between the quotes is already the string's
// exact value — true when it holds no escape and is valid UTF-8.
//
// Splitting this from the decode is what lets the scan skip over string
// VALUES without allocating: only a key ever needs its text.
func scanStringEnd(raw []byte, i int) (end int, exact bool, err error) {
	escaped := false
	for j := i + 1; j < len(raw); j++ {
		switch raw[j] {
		case '\\':
			escaped = true
			j++ // the escaped byte cannot end the string
		case '"':
			return j + 1, !escaped && utf8.Valid(raw[i+1:j]), nil
		}
	}
	return 0, false, ErrParamsNotObject // unterminated
}

// decodeStringSpan returns the value of the JSON string occupying raw[i:end].
//
// Decoded, not raw, because two keys can be distinct byte strings and the
// same key to a parser. `"a"` and `"\u0061"` is the obvious pair. The one a
// fuzzer finds is invalid UTF-8: encoding/json substitutes U+FFFD for every
// unreadable byte, so `{"\xf0":1,"\x80":2}` is two keys here and one key to
// a Go backend — ambiguity by exactly the definition this scan exists to
// reject. Anything but the exact case routes through json.Unmarshal, so the
// answer is the decoder's own rather than a second guess at its rules.
func decodeStringSpan(raw []byte, i, end int, exact bool) (string, error) {
	if exact {
		return string(raw[i+1 : end-1]), nil
	}
	var s string
	if err := json.Unmarshal(raw[i:end], &s); err != nil {
		return "", ErrParamsNotObject
	}
	return s, nil
}
