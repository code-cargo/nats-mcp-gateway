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

package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
)

// lookup resolves one ${NAME} reference. It returns an error rather than an
// empty string for a name it will not resolve: an unresolved credential that
// defaults to "" runs the backend anyway and surfaces as an opaque 401 from
// some third party, hours from the typo that caused it.
type lookup func(name string) (string, error)

// envLookup reads the gateway's own environment — the documented
// credential-injection point for the documents an OPERATOR authored (the file
// and inline sources).
//
// Presence is the test, not non-emptiness: exporting a variable as empty is a
// choice an operator can make, while an unset one is a typo or a secret whose
// mount failed.
func envLookup(name string) (string, error) {
	v, ok := os.LookupEnv(name)
	if !ok {
		return "", fmt.Errorf("${%s} is not set in the gateway's environment (write $${ for a literal)", name)
	}
	return v, nil
}

// refuseLookup serves the fetch source. The controller owns the secrets and
// sends resolved values, so the gateway's environment is not a second
// credential source for the documents it receives — if it were, whoever can
// answer the config subject could name any variable the pod happens to carry
// (its cloud role credentials, its NATS password) and read it back out through
// a backend argument, environment entry, or URL.
//
// Refused loudly rather than left as the literal "${NAME}", which would reach
// the backend as a nonsense credential and fail as a 401 nobody can trace.
func refuseLookup(name string) (string, error) {
	return "", fmt.Errorf("${%s}: a fetched config is not expanded from the gateway's environment; "+
		"have the controller send the resolved value (write $${ for a literal)", name)
}

// expand rewrites every string VALUE in the decoded config, in place.
//
// It runs on the decoded struct and never on the document text. A variable's
// value is data: spliced into raw JSON, a password holding a quote ends its
// string early and breaks the parse, one holding a backslash is silently
// re-read as a JSON escape, and a crafted one (`x", "transport": "stdio",
// "command": "/bin/evil`) adds fields the document never declared. After
// decode, the worst a value can do is be a bad value, which validate() judges.
//
// The walk is reflective so the rule is "every string in the schema" rather
// than a list of fields to keep in sync: a field added later that quietly lost
// its expansion is the same silent failure this function exists to remove.
// Keys are not expanded (see below), and the result of an expansion is never
// re-scanned, so no value can smuggle in a second reference.
//
// Reflection makes that rule automatic for a field whose TYPE the walk can
// reach, and silently false for one whose type it cannot — an interface, or a
// json.RawMessage holding an undecoded subdocument. Such a field would keep
// its ${VAR} literally on the file path, and would slip past the REFUSAL on
// the fetch path, which is the security-relevant half. So an unreachable type
// is an error rather than a skip, and TestExpandReachesEveryTypeInTheSchema
// fails on one at build time rather than waiting for a document to contain it.
func expand(cfg *Config, resolve lookup) error {
	return expandValue(reflect.ValueOf(cfg).Elem(), "", resolve)
}

func expandValue(v reflect.Value, path string, resolve lookup) error {
	switch v.Kind() {
	case reflect.String:
		s, err := expandString(v.String(), resolve)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		v.SetString(s)
	case reflect.Pointer:
		if !v.IsNil() {
			return expandValue(v.Elem(), path, resolve)
		}
	case reflect.Struct:
		t := v.Type()
		for i := range t.NumField() {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			if err := expandValue(v.Field(i), joinPath(path, jsonName(f)), resolve); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		// A byte slice is an undecoded subdocument (json.RawMessage): its
		// strings are not fields of the schema, walking it would iterate bytes
		// and rewrite nothing, and the caller would never learn that the
		// ${VAR} inside it was neither expanded nor refused.
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return fmt.Errorf("%s: %s holds undecoded bytes, which ${VAR} expansion cannot reach; "+
				"decode it into named fields instead", path, v.Type())
		}
		for i := range v.Len() {
			if err := expandValue(v.Index(i), fmt.Sprintf("%s[%d]", path, i), resolve); err != nil {
				return err
			}
		}
	case reflect.Map:
		// Decoded from JSON objects, so every key is a string. Sorted, so a
		// document with two bad entries always names the same one.
		keys := v.MapKeys()
		slices.SortFunc(keys, func(a, b reflect.Value) int { return strings.Compare(a.String(), b.String()) })
		for _, k := range keys {
			key := k.String()
			// A key names something the runtime looks up by name — an
			// environment variable, an HTTP header, a server — so a ${VAR}
			// there is a mistake, not a reference. Rejected rather than passed
			// through: a variable left literally named "${TOKEN}" would go
			// unnoticed until the backend could not find its credential.
			if strings.Contains(key, "${") {
				return fmt.Errorf("%s: key %q: ${VAR} expands in values, not in keys", path, key)
			}
			// Map elements are not addressable, so expand a copy and store it
			// back over the original.
			elem := reflect.New(v.Type().Elem()).Elem()
			elem.Set(v.MapIndex(k))
			if err := expandValue(elem, fmt.Sprintf("%s[%q]", path, key), resolve); err != nil {
				return err
			}
			v.SetMapIndex(k, elem)
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		// Numbers and booleans hold no strings, so there is nothing to expand.
	default:
		// Anything else (an interface, a channel, a func) is a type the walk
		// cannot rewrite in place. Refused rather than skipped: see expand.
		return fmt.Errorf("%s: config field of type %s is not reached by ${VAR} expansion; "+
			"give it a concrete type the walk can rewrite", path, v.Type())
	}
	return nil
}

// expandString resolves the ${NAME} references in one value.
//
// ${NAME} is the only reference form, matching what the README documents. A
// bare $ is part of the value — generated passwords and shell-ish arguments
// carry them, and eating a "$WORD" out of a credential corrupts it silently.
// $$ is the one escape, so a value that must contain a literal ${...} stays
// expressible instead of being an unavoidable "not set" error.
func expandString(s string, resolve lookup) (string, error) {
	if !strings.Contains(s, "$") {
		return s, nil
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '$' || i+1 == len(s) {
			b.WriteByte(s[i])
			i++
			continue
		}
		switch s[i+1] {
		case '$':
			b.WriteByte('$')
			i += 2
		case '{':
			end := strings.IndexByte(s[i+2:], '}')
			if end < 0 {
				return "", errors.New("unterminated ${ (write $${ for a literal)")
			}
			name := s[i+2 : i+2+end]
			if name == "" {
				return "", errors.New("empty ${} reference (write $${} for a literal)")
			}
			v, err := resolve(name)
			if err != nil {
				return "", err
			}
			b.WriteString(v)
			i += end + 3
		default:
			b.WriteByte('$')
			i++
		}
	}
	return b.String(), nil
}

// jsonName is the field's name in the document, so an error points at what the
// operator actually wrote.
func jsonName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
	if name == "" {
		return f.Name
	}
	return name
}

func joinPath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}
