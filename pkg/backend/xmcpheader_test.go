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

package backend

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseToolAnnotationsAcceptsValidSchemas(t *testing.T) {
	// The spec's own example.
	schema := json.RawMessage(`{
		"type": "object",
		"properties": {
			"region": {"type": "string", "description": "where", "x-mcp-header": "Region"},
			"query":  {"type": "string"}
		},
		"required": ["region", "query"]
	}`)
	got, err := parseToolAnnotations(schema)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "Region", got[0].name)
	assert.Equal(t, []string{"region"}, got[0].path)
}

func TestParseToolAnnotationsAllowsNestedProperties(t *testing.T) {
	// Nesting is fine as long as every step is a `properties` key.
	schema := json.RawMessage(`{
		"type": "object",
		"properties": {
			"target": {
				"type": "object",
				"properties": {
					"zone": {"type": "string", "x-mcp-header": "Zone"}
				}
			}
		}
	}`)
	got, err := parseToolAnnotations(schema)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []string{"target", "zone"}, got[0].path)
}

func TestParseToolAnnotationsRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"empty name", `{"properties":{"a":{"type":"string","x-mcp-header":""}}}`},
		{"not a token", `{"properties":{"a":{"type":"string","x-mcp-header":"Bad Header"}}}`},
		{"carriage return", `{"properties":{"a":{"type":"string","x-mcp-header":"A\rB"}}}`},
		{"newline", `{"properties":{"a":{"type":"string","x-mcp-header":"A\nB"}}}`},
		{"number type", `{"properties":{"a":{"type":"number","x-mcp-header":"A"}}}`},
		{"object type", `{"properties":{"a":{"type":"object","x-mcp-header":"A"}}}`},
		{"array type", `{"properties":{"a":{"type":"array","x-mcp-header":"A"}}}`},
		{"missing type", `{"properties":{"a":{"x-mcp-header":"A"}}}`},
		{
			"case-insensitive collision",
			`{"properties":{"a":{"type":"string","x-mcp-header":"Region"},
			                "b":{"type":"string","x-mcp-header":"region"}}}`,
		},
		// Not statically reachable: a client cannot know which instance value
		// each of these would refer to.
		{"under items", `{"properties":{"a":{"type":"array","items":{"type":"string","x-mcp-header":"A"}}}}`},
		{"under oneOf", `{"oneOf":[{"properties":{"a":{"type":"string","x-mcp-header":"A"}}}]}`},
		{"under anyOf", `{"anyOf":[{"properties":{"a":{"type":"string","x-mcp-header":"A"}}}]}`},
		{"under allOf", `{"allOf":[{"properties":{"a":{"type":"string","x-mcp-header":"A"}}}]}`},
		{"under not", `{"not":{"properties":{"a":{"type":"string","x-mcp-header":"A"}}}}`},
		{"under if", `{"if":{"properties":{"a":{"type":"string","x-mcp-header":"A"}}}}`},
		{"under $defs", `{"$defs":{"x":{"properties":{"a":{"type":"string","x-mcp-header":"A"}}}}}`},
		{"at the schema root", `{"type":"string","x-mcp-header":"A"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseToolAnnotations(json.RawMessage(tc.schema))
			assert.Error(t, err, "this definition must invalidate the tool")
		})
	}
}

func TestParseToolAnnotationsIgnoresUnannotatedSchemas(t *testing.T) {
	// An ordinary schema, including composition keywords, is perfectly valid
	// — it just has nothing to mirror.
	for _, s := range []string{
		`{}`,
		`{"type":"object","properties":{"a":{"type":"string"}}}`,
		`{"oneOf":[{"properties":{"a":{"type":"string"}}}]}`,
		`not json at all`,
	} {
		got, err := parseToolAnnotations(json.RawMessage(s))
		require.NoError(t, err, "schema %q", s)
		assert.Empty(t, got)
	}
}

func TestParamHeaders(t *testing.T) {
	params := []headerParam{
		{name: "Region", path: []string{"region"}},
		{name: "Count", path: []string{"count"}},
		{name: "Dry", path: []string{"dry"}},
		{name: "Zone", path: []string{"target", "zone"}},
		{name: "Absent", path: []string{"nope"}},
		{name: "Null", path: []string{"nulled"}},
	}
	args := json.RawMessage(`{
		"region": "us-west1",
		"count": 42,
		"dry": true,
		"target": {"zone": "a"},
		"nulled": null
	}`)

	got, _ := paramHeaders(params, args)
	assert.Equal(t, "us-west1", got["Mcp-Param-Region"])
	assert.Equal(t, "42", got["Mcp-Param-Count"])
	assert.Equal(t, "true", got["Mcp-Param-Dry"])
	assert.Equal(t, "a", got["Mcp-Param-Zone"])
	// "Client MUST omit the header" for both an absent value and an explicit
	// null — the server correspondingly MUST NOT expect one.
	assert.NotContains(t, got, "Mcp-Param-Absent")
	assert.NotContains(t, got, "Mcp-Param-Null")
}

func TestParamHeadersEncodesUnsafeValues(t *testing.T) {
	got, _ := paramHeaders(
		[]headerParam{{name: "Greeting", path: []string{"g"}}},
		json.RawMessage(`{"g":"Hello, 世界"}`),
	)
	assert.Equal(t, "=?base64?SGVsbG8sIOS4lueVjA==?=", got["Mcp-Param-Greeting"],
		"a value that cannot ride in a header raw must be sentinel-encoded")
}

func TestParamHeadersRejectsUnsafeIntegers(t *testing.T) {
	// Beyond 2^53-1 a JSON round trip through a double is no longer lossless,
	// so header and body could disagree about the same number.
	got, _ := paramHeaders(
		[]headerParam{{name: "Big", path: []string{"n"}}},
		json.RawMessage(`{"n":9007199254740993}`),
	)
	assert.NotContains(t, got, "Mcp-Param-Big")

	got, _ = paramHeaders(
		[]headerParam{{name: "Ok", path: []string{"n"}}},
		json.RawMessage(`{"n":9007199254740991}`),
	)
	assert.Equal(t, "9007199254740991", got["Mcp-Param-Ok"])
}

func TestParseToolAnnotationsToleratesOrdinarySchemas(t *testing.T) {
	// Each of these is a well-formed tool. Rejecting any of them would delete
	// it from tools/list entirely, so a false positive here is far more
	// costly than simply not mirroring a header.
	tests := []struct {
		name   string
		schema string
		want   []headerParam
	}{
		{
			// A nullable string is a string for header purposes; a null value
			// just contributes no header.
			name:   "nullable primitive",
			schema: `{"properties":{"region":{"type":["string","null"],"x-mcp-header":"Region"}}}`,
			want:   []headerParam{{name: "Region", path: []string{"region"}}},
		},
		{
			// "x-mcp-header" appearing inside a DEFAULT is instance data, not
			// an annotation — a plausible schema for a tool that sets headers.
			name: "x-mcp-header inside a default value",
			schema: `{"properties":{"cfg":{"type":"object",
				"default":{"x-mcp-header":"X-Trace"}}}}`,
			want: nil,
		},
		{
			name: "x-mcp-header inside an enum",
			schema: `{"properties":{"key":{"type":"string",
				"enum":["x-mcp-header","other"]}}}`,
			want: nil,
		},
		{
			name: "x-mcp-header inside examples",
			schema: `{"properties":{"cfg":{"type":"object",
				"examples":[{"x-mcp-header":"X-Trace"}]}}}`,
			want: nil,
		},
		{
			// A real annotation alongside instance data that mentions the key.
			name: "both",
			schema: `{"properties":{
				"region":{"type":"string","x-mcp-header":"Region"},
				"cfg":{"type":"object","default":{"x-mcp-header":"ignored"}}}}`,
			want: []headerParam{{name: "Region", path: []string{"region"}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseToolAnnotations(json.RawMessage(tc.schema))
			require.NoError(t, err, "this tool must survive")
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseToolAnnotationsStillRejectsGenuineUnions(t *testing.T) {
	// Two real types is not a nullable primitive; there is no unambiguous
	// header rendering, so the tool is invalid as defined.
	_, err := parseToolAnnotations(json.RawMessage(
		`{"properties":{"a":{"type":["string","integer"],"x-mcp-header":"A"}}}`,
	))
	assert.Error(t, err)
}

func TestParseToolAnnotationsIsOrderStable(t *testing.T) {
	// Schema objects are Go maps, so discovery order is random. The result
	// has to be a stable value or the retry logic, which compares two parses
	// for equality, would see spurious changes.
	schema := json.RawMessage(`{"properties":{
		"c":{"type":"string","x-mcp-header":"Ccc"},
		"a":{"type":"string","x-mcp-header":"Aaa"},
		"b":{"type":"string","x-mcp-header":"Bbb"}}}`)
	first, err := parseToolAnnotations(schema)
	require.NoError(t, err)
	for range 20 {
		again, err := parseToolAnnotations(schema)
		require.NoError(t, err)
		assert.Equal(t, first, again)
	}
	assert.Equal(t, []string{"Aaa", "Bbb", "Ccc"},
		[]string{first[0].name, first[1].name, first[2].name})
}
