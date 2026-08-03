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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecodeParamsMatchesKeysExactly is the property the gateway's
// authorization rests on. A tagged Go struct answers "get_issue" here,
// because encoding/json falls back to case-insensitive field matching; every
// case-sensitive parser downstream answers "delete_repo". Only a reading
// that cannot differ from the backend's is usable for an ACL.
func TestDecodeParamsMatchesKeysExactly(t *testing.T) {
	// Nothing case-colliding, so the decode succeeds and the read is exact.
	p, err := DecodeParams(json.RawMessage(`{"name":"delete_repo","uri":"file:///x"}`))
	require.NoError(t, err)

	name, err := p.String("name")
	require.NoError(t, err)
	assert.Equal(t, "delete_repo", name)

	// A struct field would have matched "NAME"; a map key does not exist.
	_, present := p["NAME"]
	assert.False(t, present)
}

func TestDecodeParamsRejectsAmbiguousKeys(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"duplicate at top level", `{"name":"a","name":"b"}`},
		{"case collision at top level", `{"name":"a","NAME":"b"}`},
		{"case collision, mixed case", `{"name":"a","NaMe":"b"}`},
		{"case collision on uri", `{"uri":"a","URI":"b"}`},
		{"case collision on _meta", `{"_meta":{},"_META":{}}`},
		{"case collision on arguments", `{"arguments":{},"ARGUMENTS":{}}`},
		{"duplicate nested one level", `{"arguments":{"repo":"a","repo":"b"}}`},
		{"duplicate nested in an array", `{"arguments":{"xs":[{"k":1,"k":2}]}}`},
		{"duplicate deep", `{"a":{"b":{"c":[[{"d":1,"d":2}]]}}}`},
		{"duplicate _meta key", `{"_meta":{"v":1,"v":2}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeParams(json.RawMessage(tt.input))
			require.Error(t, err)
			var ambiguous *AmbiguousKeyError
			assert.True(t, errors.As(err, &ambiguous),
				"an ambiguous body must be distinguishable from a malformed one: %v", err)
		})
	}
}

// TestScanFrameSlotReuse guards a false-POSITIVE the scanner's frame
// bookkeeping could reintroduce.
//
// scanUnambiguous pops a frame by reslicing and pushes the next one with
// append, so a new object lands in the slot its previous sibling used. That
// is safe only because the pushed composite literal zeroes the frame's key
// set. Rewrite the push to mutate the reused slot in place — a natural-enough
// optimization — and every one of these becomes a duplicate that is not
// there, rejecting traffic the gateway has no quarrel with.
//
// Fuzzing covers this, but only in a run that sets -fuzztime; `go test`
// replays the seed corpus alone, and no seed repeats a key across siblings.
func TestScanFrameSlotReuse(t *testing.T) {
	var promoted strings.Builder
	promoted.WriteString(`{"big":{`)
	for i := range smallObjectKeys + 9 {
		if i > 0 {
			promoted.WriteByte(',')
		}
		fmt.Fprintf(&promoted, `"k%d":1`, i)
	}
	promoted.WriteString(`},"next":{"k0":1,"k1":2}}`)

	for _, input := range []string{
		`{"xs":[{"k":1},{"k":2}]}`,
		`{"xs":[{"k":1},{"k":2},{"k":3},{"k":4}]}`,
		`{"a":{"k":1},"b":{"k":2}}`,
		`{"deep":{"x":{"k":1}},"other":{"y":{"k":2}}}`,
		// A sibling reusing the slot of an object that had PROMOTED to a map.
		promoted.String(),
	} {
		_, err := DecodeParams(json.RawMessage(input))
		assert.NoError(t, err, "sibling objects may repeat each other's keys: %s", input)
	}
}

// TestScanFramePromotionBoundary guards the matching false-NEGATIVE.
//
// A frame compares up to smallObjectKeys keys by linear scan and then
// promotes to a map, draining what it had already recorded. Lose that drain
// and duplicates among the first smallObjectKeys keys of a large object stop
// being detected — which is not a cosmetic failure but the authorization hole
// this whole file exists to close, reopened silently.
func TestScanFramePromotionBoundary(t *testing.T) {
	// n distinct keys, then one more repeating the second: straddles the
	// boundary as n crosses smallObjectKeys.
	body := func(n int, extra string) string {
		var sb strings.Builder
		sb.WriteByte('{')
		for i := range n {
			fmt.Fprintf(&sb, `"k%d":%d,`, i, i)
		}
		fmt.Fprintf(&sb, "%q:99}", extra)
		return sb.String()
	}
	for _, n := range []int{2, smallObjectKeys - 1, smallObjectKeys, smallObjectKeys + 1, smallObjectKeys + 24} {
		_, err := DecodeParams(json.RawMessage(body(n, "k1")))
		require.Error(t, err, "duplicate missed in a %d-key object", n)
		var ambiguous *AmbiguousKeyError
		assert.True(t, errors.As(err, &ambiguous))

		_, err = DecodeParams(json.RawMessage(body(n, "unique")))
		assert.NoError(t, err, "distinct keys flagged in a %d-key object", n)
	}
}

// TestDecodeParamsLeavesNestedCaseCollisionsAlone pins the edge of the
// case-uniqueness rule. Duplicate keys are rejected at every depth because no
// parser agrees on them; case collisions are rejected only where the spec
// fixes the field names, because deep inside tool arguments they are legal
// caller data the gateway does not read.
func TestDecodeParamsLeavesNestedCaseCollisionsAlone(t *testing.T) {
	_, err := DecodeParams(json.RawMessage(`{"name":"t","arguments":{"Repo":"a","repo":"b"}}`))
	assert.NoError(t, err)
}

// TestFoldCoversCaseMapping is what licenses the hand-written
// caseMapExceptions table. The gateway's key-collision rule has to be a
// superset of every case-insensitive relation a backend might apply, and Go's
// simple folding is not one: Java's equalsIgnoreCase and .NET's
// OrdinalIgnoreCase compare per-rune ToUpper/ToLower, which equates runes
// that folding keeps apart.
//
// Rather than trust a list someone assembled by hand, enumerate all of
// Unicode: group every rune by its ToUpper and by its ToLower image, then
// require every member of a group to share a foldRune. A Go release that
// moves the case tables fails here instead of silently reopening the gap.
func TestFoldCoversCaseMapping(t *testing.T) {
	for _, mapFn := range []func(rune) rune{unicode.ToUpper, unicode.ToLower} {
		groups := map[rune][]rune{}
		for r := rune(0); r <= unicode.MaxRune; r++ {
			if !utf8.ValidRune(r) {
				continue
			}
			groups[mapFn(r)] = append(groups[mapFn(r)], r)
		}
		for _, members := range groups {
			want := foldRune(members[0])
			for _, m := range members[1:] {
				require.Equal(t, want, foldRune(m),
					"U+%04X %q and U+%04X %q are the same key to a ToUpper/ToLower "+
						"comparison but fold apart", members[0], members[0], m, m)
			}
		}
	}
}

// TestEqualFoldKeyMatchesFoldKey keeps the pairwise and whole-key spellings
// of the relation in step. They drifted once: checkCaseUnique used
// strings.ToLower while pkg/backend used strings.EqualFold, and U+017F (ſ)
// lowercases to itself but folds to "s".
func TestEqualFoldKeyMatchesFoldKey(t *testing.T) {
	keys := []string{
		"name", "NAME", "NaMe", "uri", "URI", "urı", "urİ", "_meta", "_META",
		"arguments", "argumentſ", "ARGUMENTS", "cursor", "curſor",
		"ß", "ẞ", "SS", "k", "K", "K", // U+212A folds to k
		"straße", "STRASSE", "", "a", "á", "Á", "日本語",
		"İ", "ı", "i", "I", "ii", "i",
	}
	for _, a := range keys {
		for _, b := range keys {
			assert.Equal(t, foldKey(a) == foldKey(b), EqualFoldKey(a, b),
				"EqualFoldKey and foldKey disagree on %q vs %q", a, b)
		}
	}
}

// TestCaseCollisionCoversBothRelations is the params-level regression. The
// first pair is caught only by folding (ſ lowercases to itself); the rest
// only by case mapping (the dotted/dotless I family shares no fold orbit).
// A rule implementing either relation alone lets half of these through.
func TestCaseCollisionCoversBothRelations(t *testing.T) {
	for _, input := range []string{
		`{"arguments":{"region":"us"},"argumentſ":{"region":"eu"}}`,
		`{"uri":"file:///public/ok","urı":"file:///etc/shadow"}`,
		`{"uri":"file:///public/ok","urİ":"file:///etc/shadow"}`,
		`{"uri":"file:///public/ok","URI":"file:///etc/shadow"}`,
	} {
		_, err := DecodeParams(json.RawMessage(input))
		require.Error(t, err, "input %s was accepted", input)
		var ambiguous *AmbiguousKeyError
		assert.True(t, errors.As(err, &ambiguous))
	}
}

// TestMetaAllowsThirdPartyCaseCollisions is the counterpart rule. _meta is
// MCP's open extension bag, so keys the gateway never reads are none of its
// business — only the key it DOES read has to be unambiguous among its
// siblings. Holding the whole object to the stricter rule rejected
// conformant traffic.
func TestMetaAllowsThirdPartyCaseCollisions(t *testing.T) {
	p, err := DecodeParams(json.RawMessage(fmt.Sprintf(
		`{"_meta":{%q:%q,"com.acme/trace":"x","com.acme/Trace":"y"}}`,
		MetaProtocolVersion, ProtocolVersion,
	)))
	require.NoError(t, err)

	ver, err := p.ProtocolVersion()
	require.NoError(t, err, "an extension key the gateway never reads must not fail the request")
	assert.Equal(t, ProtocolVersion, ver)
}

// TestMetaRejectsCollisionOnTheKeyItReads is the other half: a sibling that
// a case-folding parser would bind to protocolVersion instead is refused.
func TestMetaRejectsCollisionOnTheKeyItReads(t *testing.T) {
	for _, forged := range []string{
		"io.modelcontextprotocol/protocolversion",
		"io.modelcontextprotocol/protocolVerſion",
		"IO.MODELCONTEXTPROTOCOL/PROTOCOLVERSION",
	} {
		p, err := DecodeParams(json.RawMessage(fmt.Sprintf(
			`{"_meta":{%q:%q,%q:"1999-01-01"}}`,
			MetaProtocolVersion, ProtocolVersion, forged,
		)))
		require.NoError(t, err)

		_, err = p.ProtocolVersion()
		require.Error(t, err, "forged key %q was ignored", forged)
		var ambiguous *AmbiguousKeyError
		assert.True(t, errors.As(err, &ambiguous))
	}
}

// TestCheckReadableMetaCoversProgressToken guards a key the gateway reads in
// ANOTHER package. pkg/backend.Mux.Call rewrites _meta.progressToken to a
// mux-unique value so two concurrent callers cannot collide; that rewrite
// replaces the exact key and leaves a case-variant sibling intact, so a
// case-folding backend echoes the sibling back and the notification routes to
// whoever registered that token. Declaring the key is what makes the rewrite
// mean anything.
func TestCheckReadableMetaCoversProgressToken(t *testing.T) {
	for _, forged := range []string{"ProgressToken", "PROGRESSTOKEN", "progressToken"} {
		p, err := DecodeParams(json.RawMessage(fmt.Sprintf(
			`{"name":"t","_meta":{%q:%q,%q:"gt7"}}`,
			MetaProtocolVersion, ProtocolVersion, forged,
		)))
		require.NoError(t, err, "the collision is inside _meta, so the decode itself allows it")

		err = p.CheckReadableMeta()
		if forged == MetaProgressToken {
			assert.NoError(t, err, "a single progressToken is ordinary")
			continue
		}
		require.Error(t, err, "%q would be bound by a case-folding backend", forged)
		var ambiguous *AmbiguousKeyError
		assert.True(t, errors.As(err, &ambiguous))
	}
}

// TestCheckReadableMetaLeavesUndeclaredKeysAlone is the counterpart: _meta is
// an open extension bag, and a key the gateway never reads may be spelled two
// ways without the request failing.
func TestCheckReadableMetaLeavesUndeclaredKeysAlone(t *testing.T) {
	p, err := DecodeParams(json.RawMessage(fmt.Sprintf(
		`{"name":"t","_meta":{%q:%q,"com.acme/trace":"x","com.acme/Trace":"y"}}`,
		MetaProtocolVersion, ProtocolVersion,
	)))
	require.NoError(t, err)
	assert.NoError(t, p.CheckReadableMeta())
}

// TestMetaKeysReadMatchesWhatIsRead is a tripwire, not a behavior test. The
// reads it covers live in other packages, so nothing else notices when the
// list and the readers drift apart.
func TestMetaKeysReadMatchesWhatIsRead(t *testing.T) {
	assert.Contains(t, metaKeysRead, MetaProtocolVersion, "read by pkg/proxy.Check")
	assert.Contains(t, metaKeysRead, MetaProgressToken, "read and rewritten by pkg/backend.Mux.Call")
	assert.NotContains(t, metaKeysRead, MetaSubscriptionID,
		"subscriptionId appears only on backend-originated messages, which no caller controls")
}

// TestRawEnforcesCaseUniqueness covers the accessor pkg/backend reads
// "arguments" through on its way to the Mcp-Param-* headers. Reaching for
// raw bytes must not be a way around the rule.
func TestRawEnforcesCaseUniqueness(t *testing.T) {
	p := Params{"arguments": json.RawMessage(`{"a":1}`), "ARGUMENTS": json.RawMessage(`{"a":2}`)}
	_, _, err := p.Raw("arguments")
	require.Error(t, err)
	var ambiguous *AmbiguousKeyError
	assert.True(t, errors.As(err, &ambiguous))
}

func TestDecodeParamsEmptyForms(t *testing.T) {
	for _, input := range []string{``, `null`, `{}`, `   `} {
		p, err := DecodeParams(json.RawMessage(input))
		require.NoError(t, err, "input %q", input)
		assert.Empty(t, p)

		// An empty Params must read like a body that simply omitted things,
		// not like an error: that is how unnamed methods pass the check.
		name, _, err := p.Name(MethodToolsCall)
		require.NoError(t, err)
		assert.Equal(t, "", name)
		ver, err := p.ProtocolVersion()
		require.NoError(t, err)
		assert.Equal(t, "", ver)
	}
}

func TestDecodeParamsRejectsNonObjects(t *testing.T) {
	for _, input := range []string{`[]`, `[{"name":"x"}]`, `"hello"`, `7`, `true`} {
		_, err := DecodeParams(json.RawMessage(input))
		assert.ErrorIs(t, err, ErrParamsNotObject, "input %q", input)
	}
}

func TestDecodeParamsRejectsDeepNesting(t *testing.T) {
	// Recursion depth is caller-controlled, so it needs a bound. Rejecting is
	// the safe direction: nothing legitimate nests this far.
	deep := strings.Repeat(`{"a":`, MaxParamsDepth+5) + `1` + strings.Repeat(`}`, MaxParamsDepth+5)
	_, err := DecodeParams(json.RawMessage(deep))
	assert.ErrorIs(t, err, ErrParamsTooDeep)

	shallow := strings.Repeat(`{"a":`, 50) + `1` + strings.Repeat(`}`, 50)
	_, err = DecodeParams(json.RawMessage(shallow))
	assert.NoError(t, err)
}

// TestParamsStringRejectsNonStrings matters because "" is the value the
// integrity check reads as "no name given". A name that is an object or a
// number must be an error rather than silently become "", which would
// authorize the request against the wrong subject token.
func TestParamsStringRejectsNonStrings(t *testing.T) {
	p, err := DecodeParams(json.RawMessage(`{"n":42,"o":{},"a":[],"b":true,"z":null}`))
	require.NoError(t, err)

	for _, key := range []string{"n", "o", "a", "b"} {
		_, err := p.String(key)
		assert.Error(t, err, "params %q must not read as a string", key)
	}

	// null and absent both mean "nothing here", which is not an error.
	for _, key := range []string{"z", "missing"} {
		v, err := p.String(key)
		require.NoError(t, err)
		assert.Equal(t, "", v)
	}
}

func TestNameFieldCoversEveryNamedMethod(t *testing.T) {
	assert.Equal(t, "name", NameField(MethodToolsCall))
	assert.Equal(t, "name", NameField(MethodPromptsGet))
	assert.Equal(t, "uri", NameField(MethodResourcesRead))
	assert.Equal(t, "", NameField(MethodToolsList))

	p, err := DecodeParams(json.RawMessage(`{"name":"t","uri":"file:///x"}`))
	require.NoError(t, err)

	// The name a method carries is the field ITS method names, never the
	// other one: reading uri for tools/call would authorize the wrong token.
	name, named, err := p.Name(MethodToolsCall)
	require.NoError(t, err)
	assert.True(t, named)
	assert.Equal(t, "t", name)

	uri, named, err := p.Name(MethodResourcesRead)
	require.NoError(t, err)
	assert.True(t, named)
	assert.Equal(t, "file:///x", uri)

	_, named, err = p.Name(MethodToolsList)
	require.NoError(t, err)
	assert.False(t, named)
}

func TestProtocolVersion(t *testing.T) {
	p, err := DecodeParams(json.RawMessage(
		`{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}`,
	))
	require.NoError(t, err)
	ver, err := p.ProtocolVersion()
	require.NoError(t, err)
	assert.Equal(t, ProtocolVersion, ver)

	// _meta that is not an object, and a version that is not a string, are
	// both errors rather than a silent "" that would fail as a mismatch and
	// tell the caller nothing.
	p, err = DecodeParams(json.RawMessage(`{"_meta":"2026-07-28"}`))
	require.NoError(t, err)
	_, err = p.ProtocolVersion()
	assert.Error(t, err)

	p, err = DecodeParams(json.RawMessage(
		`{"_meta":{"io.modelcontextprotocol/protocolVersion":20260728}}`,
	))
	require.NoError(t, err)
	_, err = p.ProtocolVersion()
	assert.Error(t, err)
}

func TestAmbiguousKeyErrorMessage(t *testing.T) {
	assert.Contains(t, (&AmbiguousKeyError{Key: "name", Other: "name"}).Error(), "duplicate key")
	assert.Contains(t, (&AmbiguousKeyError{Key: "NAME", Other: "name"}).Error(), "differ only by case")

	// Map iteration order must not leak into the message, or the same body
	// produces different error text run to run.
	for range 50 {
		_, err := DecodeParams(json.RawMessage(`{"name":"a","NAME":"b"}`))
		require.Error(t, err)
		assert.Equal(t, `keys "NAME" and "name" differ only by case: the body has no single meaning`,
			err.Error())
	}
}

// scanValueTokens is the readable statement of the duplicate-key rule, built
// on json.Decoder.Token(). scanUnambiguous is a byte scan instead, because
// Token() boxes every token and this runs ahead of everything else on every
// request. The fuzz test below is what makes the fast one trustworthy: it
// holds the two to identical verdicts on arbitrary input.
func scanValueTokens(dec *json.Decoder, depth int) error {
	if depth > MaxParamsDepth {
		return ErrParamsTooDeep
	}
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, isDelim := tok.(json.Delim)
	if !isDelim {
		return nil // a scalar has nothing to disambiguate
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyTok.(string)
			if !ok {
				return ErrParamsNotObject
			}
			if _, dup := seen[key]; dup {
				return &AmbiguousKeyError{Key: key, Other: key}
			}
			seen[key] = struct{}{}
			if err := scanValueTokens(dec, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := scanValueTokens(dec, depth+1); err != nil {
				return err
			}
		}
	}
	_, err = dec.Token()
	return err
}

// referenceScan classifies raw the way scanUnambiguous does, using only the
// stdlib tokenizer. It returns the offending key for a duplicate, "" for a
// clean document, and reports whether it rejected at all.
func referenceScan(t *testing.T, raw []byte) (dupKey string, rejected bool) {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	err := scanValueTokens(dec, 0)
	if err == nil {
		// Trailing garbage after a complete value: scanUnambiguous walks to
		// the end of the buffer and would see it, so the reference must too.
		if _, more := dec.Token(); more == nil {
			return "", true
		}
		return "", false
	}
	var ambiguous *AmbiguousKeyError
	if errors.As(err, &ambiguous) {
		return ambiguous.Key, true
	}
	return "", true
}

// FuzzScanUnambiguous is what licenses the hand-rolled byte scan. A scanner
// that misses a duplicate key is an authorization bypass and a scanner that
// invents one is an outage, so it is pinned to the stdlib tokenizer's verdict
// on arbitrary input rather than to a list of cases someone thought of.
func FuzzScanUnambiguous(f *testing.F) {
	seeds := []string{
		`{}`, `{"a":1}`, `{"a":1,"a":2}`, `{"a":1,"A":2}`,
		`{"a":{"b":1,"b":2}}`, `[{"a":1,"a":2}]`, `{"a":[1,2,{"b":1,"b":1}]}`,
		`{"a":"b:c,d{e}"}`, `{"a":"\"","a":2}`, `{"a":1,"a":2}`,
		`{"a\\":1,"a\\":2}`, `{"":1,"":2}`, `{"a":"[{"}`,
		`"top"`, `[]`, `null`, `7`, `true`, `-1.5e10`, `{"a":-1.5e10,"a":2}`,
		`{"a":1,}`, `{"a"}`, `{`, `}`, `[[[[]]]]`, ` { "a" : 1 } `,
		`{"emoji😀":1,"emoji😀":2}`, `{"a":"😀","a":2}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		// Only well-formed JSON is in scope: jsonrpc.Decode validates the
		// enclosing document before params ever reaches this package, so
		// malformed input is a shape the scanner never sees in production and
		// the two implementations owe no agreement on it.
		if !json.Valid(raw) {
			return
		}
		wantKey, wantRejected := referenceScan(t, raw)

		err := scanUnambiguous(raw)
		var ambiguous *AmbiguousKeyError
		gotKey := ""
		if errors.As(err, &ambiguous) {
			gotKey = ambiguous.Key
		}

		assert.Equal(t, wantRejected, err != nil,
			"verdict differs from the stdlib tokenizer on %q (got %v)", raw, err)
		if wantRejected && wantKey != "" {
			assert.Equal(t, wantKey, gotKey, "different duplicate key reported for %q", raw)
		}
	})
}

// BenchmarkDecodeParamsLargeArguments measures what the ambiguity scan costs
// on the body size that actually shows up: a tools/call with a large
// arguments blob.
func BenchmarkDecodeParamsLargeArguments(b *testing.B) {
	var sb strings.Builder
	sb.WriteString(`{"name":"upload","arguments":{"rows":[`)
	for i := range 20000 {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"id":`)
		sb.WriteString(strings.Repeat("9", 6))
		sb.WriteString(`,"text":"lorem ipsum dolor sit amet"}`)
	}
	sb.WriteString(`]}}`)
	raw := json.RawMessage(sb.String())
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeParams(raw); err != nil {
			b.Fatal(err)
		}
	}
}
