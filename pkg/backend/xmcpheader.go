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
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// x-mcp-header (SEP-2243): a server MAY mark tool parameters to be mirrored
// into Mcp-Param-{Name} request headers so intermediaries can route on them
// without parsing the body. Supporting it is a client MUST on Streamable
// HTTP, and the gateway is the client toward an HTTP backend.
//
// The annotation is a property of the TOOL, not of the call, so honoring it
// means knowing the tool's inputSchema. This package reads the schema off
// tools/list responses as they pass through — see httpConn.absorbToolsList —
// rather than fetching schemas up front, which keeps the common path
// schema-blind.
//
// The headers are DERIVED here, never forwarded from the caller. The spec's
// "an intermediary MUST forward Mcp-Param-* it does not recognize" governs
// HTTP proxies that pass a request through untouched; this gateway terminates
// the MCP request and reissues it, so it is the client and owes the backend
// headers it computed itself. That distinction is also what keeps the tenant
// boundary intact: a forwarded Mcp-Param-* would be caller-controlled input
// that the subject-permission integrity check cannot validate, since without
// the schema the gateway cannot know which property it claims to mirror.

// instanceKeywords are JSON Schema keywords whose values are example or
// literal DATA rather than subschemas. Nothing inside them is an annotation,
// so the walk does not descend into them.
var instanceKeywords = map[string]bool{
	"default": true, "const": true, "enum": true, "examples": true,
}

// headerParam is one honored annotation: which header to set, and the exact
// property path whose value fills it.
type headerParam struct {
	// name is the {Name} in Mcp-Param-{Name}.
	name string
	// path is the chain of `properties` keys from the schema root. Nested
	// objects are legal, so this is a path rather than a single key.
	path []string
}

// annotationError explains why a tool definition is unusable. The spec's
// remedy is to exclude the tool from tools/list rather than fail the listing,
// so a single malformed definition cannot take out a server's whole toolset.
type annotationError struct{ reason string }

func (e *annotationError) Error() string { return e.reason }

// parseToolAnnotations extracts every x-mcp-header annotation from one tool's
// inputSchema, or reports the tool invalid.
func parseToolAnnotations(schema json.RawMessage) ([]headerParam, error) {
	if len(schema) == 0 {
		return nil, nil
	}
	var root any
	if err := json.Unmarshal(schema, &root); err != nil {
		// The bytes are syntactically valid JSON — absorbToolsList decoded the
		// enclosing tool object, which scans them — so this is a value Go cannot
		// represent, in practice a number outside float64 (`1e999`). If the
		// schema names the annotation somewhere in there, we cannot say which
		// parameter it marks, and a tool whose headers we cannot derive fails
		// every call with -32020. Excluding it says so; keeping it would learn
		// zero params for a tool that needs some, which reads as "no headers
		// required" everywhere downstream. With the annotation absent, an
		// unreadable schema is simply none of this code's business.
		if bytes.Contains(schema, []byte(mcpspec.SchemaHeaderAnnotation)) {
			return nil, &annotationError{"inputSchema names x-mcp-header but cannot be read: " + err.Error()}
		}
		return nil, nil
	}
	w := &annotationWalk{seen: map[string]string{}}
	if err := w.walk(root, nil, true); err != nil {
		return nil, err
	}
	// The walk iterates schema objects as Go maps, so discovery order is
	// random. Sorting makes the result a stable value, which is what lets two
	// parses of the same schema be compared for equality.
	slices.SortFunc(w.found, func(a, b headerParam) int { return strings.Compare(a.name, b.name) })
	return w.found, nil
}

type annotationWalk struct {
	found []headerParam
	// seen maps the lowercased header name to the one first claiming it:
	// names must be unique case-insensitively across the whole inputSchema.
	seen map[string]string
}

// walk descends the schema. reachable tracks the spec's "statically
// reachable" rule: a chain consisting solely of `properties` keys. An
// annotation found anywhere else invalidates the tool, because a client
// cannot know which instance value it would refer to.
func (w *annotationWalk) walk(node any, path []string, reachable bool) error {
	obj, ok := node.(map[string]any)
	if !ok {
		// Arrays can still hide annotations (inside oneOf/allOf members), and
		// those are exactly the unreachable positions we must catch.
		if arr, isArr := node.([]any); isArr {
			for _, item := range arr {
				if err := w.walk(item, path, false); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if raw, present := obj[mcpspec.SchemaHeaderAnnotation]; present {
		if err := w.record(raw, obj, path, reachable); err != nil {
			return err
		}
	}

	for key, child := range obj {
		if key == mcpspec.SchemaHeaderAnnotation {
			continue
		}
		if instanceKeywords[key] {
			// These hold INSTANCE data, not subschemas. A `default` or `enum`
			// that happens to contain an "x-mcp-header" key is a value, not an
			// annotation — descending would read it as one and, because it is
			// unreachable, delete an entirely well-formed tool.
			continue
		}
		if key == "properties" {
			props, isObj := child.(map[string]any)
			if !isObj {
				continue
			}
			for name, sub := range props {
				if err := w.walk(sub, append(append([]string{}, path...), name), reachable); err != nil {
					return err
				}
			}
			continue
		}
		// Every other keyword — items, oneOf/anyOf/allOf/not, if/then/else,
		// $ref, $defs — breaks static reachability for everything beneath it.
		if err := w.walk(child, path, false); err != nil {
			return err
		}
	}
	return nil
}

func (w *annotationWalk) record(raw any, prop map[string]any, path []string, reachable bool) error {
	name, ok := raw.(string)
	if !ok {
		return &annotationError{"x-mcp-header must be a string"}
	}
	if name == "" {
		return &annotationError{"x-mcp-header must not be empty"}
	}
	if !isHTTPToken(name) {
		return &annotationError{fmt.Sprintf("x-mcp-header %q is not a valid HTTP field-name token", name)}
	}
	if !reachable || len(path) == 0 {
		return &annotationError{fmt.Sprintf(
			"x-mcp-header %q is not statically reachable through `properties` alone", name,
		)}
	}
	if prior, dup := w.seen[strings.ToLower(name)]; dup {
		return &annotationError{fmt.Sprintf(
			"x-mcp-header %q collides with %q (names are case-insensitively unique)", name, prior,
		)}
	}
	// Only primitives can be rendered as a header value. `number` is excluded
	// by name in the spec: its text form is not canonical, so a header and a
	// body could disagree while meaning the same value.
	switch typeOf(prop["type"]) {
	case "string", "integer", "boolean":
	case "number":
		return &annotationError{fmt.Sprintf("x-mcp-header %q on a `number` parameter", name)}
	default:
		return &annotationError{fmt.Sprintf(
			"x-mcp-header %q on a non-primitive parameter (want string, integer or boolean)", name,
		)}
	}

	w.seen[strings.ToLower(name)] = name
	w.found = append(w.found, headerParam{name: name, path: path})
	return nil
}

// typeOf reads a JSON Schema `type`, which may be a string or a list.
//
// A nullable primitive — `["string", "null"]` — is by far the most common
// union in the wild, and it renders exactly like the primitive it wraps: a
// null value simply contributes no header. Collapsing it here matters because
// the only alternative outcome is deleting the tool from tools/list, which is
// far too blunt a response to an ordinary schema.
func typeOf(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []any:
		var single string
		for _, member := range t {
			s, ok := member.(string)
			if !ok {
				return ""
			}
			if s == "null" {
				continue
			}
			if single != "" {
				return "" // a genuine union of two types; not renderable
			}
			single = s
		}
		return single
	}
	return ""
}

// isHTTPToken reports whether s is a valid HTTP field-name token
// (RFC 9110 §5.1 `1*tchar`). This is what forbids CR, LF and spaces.
func isHTTPToken(s string) bool {
	for i := 0; i < len(s); i++ {
		if !strings.ContainsRune(
			"!#$%&'*+-.^_`|~0123456789"+
				"abcdefghijklmnopqrstuvwxyz"+
				"ABCDEFGHIJKLMNOPQRSTUVWXYZ", rune(s[i]),
		) {
			return false
		}
	}
	return len(s) > 0
}

// paramHeaders renders the headers for one tools/call from its arguments.
// A parameter with no value at its path contributes no header — the spec
// makes that the client's obligation, and the server correspondingly must not
// expect one.
func paramHeaders(params []headerParam, arguments json.RawMessage) (map[string]string, []string) {
	if len(params) == 0 || len(arguments) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(arguments))
	dec.UseNumber() // keep integers exact; float64 would round large ids
	var args any
	if dec.Decode(&args) != nil {
		return nil, nil
	}

	out := map[string]string{}
	// skipped names a parameter that IS present but cannot be rendered — an
	// integer past the safe range, a float where the schema said integer. The
	// server will reject the call for the missing header, so this is worth
	// reporting; a parameter that is simply absent is not.
	var skipped []string
	for _, p := range params {
		v, ok := valueAtPath(args, p.path)
		if !ok {
			continue
		}
		s, ok := headerText(v)
		if !ok {
			skipped = append(skipped, p.name)
			continue
		}
		out[mcpspec.HeaderParamPrefix+p.name] = mcpspec.EncodeHeaderValue(s)
	}
	if len(out) == 0 {
		return nil, skipped
	}
	return out, skipped
}

func valueAtPath(node any, path []string) (any, bool) {
	for _, key := range path {
		obj, ok := node.(map[string]any)
		if !ok {
			return nil, false
		}
		node, ok = obj[key]
		if !ok {
			return nil, false
		}
	}
	if node == nil {
		return nil, false // explicit null: the header is omitted
	}
	return node, true
}

// maxSafeInteger is JavaScript's Number.MAX_SAFE_INTEGER (2^53-1), the bound
// the spec puts on integer parameters mirrored into headers.
const maxSafeInteger = 1<<53 - 1

// headerText renders a primitive as its header form.
func headerText(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case bool:
		return strconv.FormatBool(t), true
	case json.Number:
		n, err := strconv.ParseInt(t.String(), 10, 64)
		if err != nil {
			return "", false // not an integer; `number` is not headerable
		}
		// JavaScript's safe-integer range. Outside it a JSON round trip
		// through a double stops being lossless, so a header and a body could
		// disagree about what is nominally the same value.
		if n > maxSafeInteger || n < -maxSafeInteger {
			return "", false
		}
		return strconv.FormatInt(n, 10), true
	default:
		return "", false
	}
}
