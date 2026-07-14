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

package wire

import (
	"fmt"
	"regexp"
	"strings"
)

// Subject grammar:
//
//	{prefix}.req.{tenant}.{server}.{method-tokens}.{name}
//
// The grammar IS the authorization model: NATS subject permissions on these
// tokens are the entire authz plane, so every rule here is security-relevant.
//
// The method occupies a variable number of tokens ("tools/call" -> "tools.call",
// "resources/templates/list" -> three tokens), so parsing anchors on position:
// name is always the LAST token, method is everything between server and name.
//
// The name token is params.name for named methods; the literal "_" for
// everything else, and as the fallback for names that are not subject-token
// safe. See NameToken for the two rules that keep the "_" fallback sound.

// DefaultPrefix is the default subject prefix.
const DefaultPrefix = "mcp.v1"

// NameUnset is the name token used when a method carries no name, or when the
// name is not subject-token safe.
const NameUnset = "_"

var tokenRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// TokenSafe reports whether s may appear as a single subject token.
func TokenSafe(s string) bool {
	return s != "" && tokenRe.MatchString(s)
}

// NameToken maps a raw MCP name (tool name, prompt name, resource URI) to its
// subject token. Token-safe names map to themselves; everything else maps to
// NameUnset. The integrity check enforces the inverse rules: a token-safe
// body name MUST arrive on its own subject (never on "_"), and "_" is
// accepted only when the body name is genuinely unsafe or the method carries
// no name — otherwise a caller could dodge per-tool ACLs by publishing safe
// names to the "_" subject.
func NameToken(name string) string {
	if name == NameUnset {
		// A tool literally named "_" is indistinguishable from the fallback;
		// treat it as unsafe rather than let it alias the wildcard slot.
		return NameUnset
	}
	if TokenSafe(name) {
		return name
	}
	return NameUnset
}

// BuildSubject constructs the request subject. method is the MCP form with
// slashes ("tools/call"); name is the raw MCP name ("" for methods that carry
// none).
func BuildSubject(prefix, tenant, server, method, name string) (string, error) {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if !TokenSafe(tenant) {
		return "", fmt.Errorf("wire: tenant %q is not subject-token safe", tenant)
	}
	if !TokenSafe(server) {
		return "", fmt.Errorf("wire: server %q is not subject-token safe", server)
	}
	segs := strings.Split(method, "/")
	if len(segs) == 0 || method == "" {
		return "", fmt.Errorf("wire: empty method")
	}
	for _, seg := range segs {
		if !TokenSafe(seg) {
			return "", fmt.Errorf("wire: method %q has non-token-safe segment %q", method, seg)
		}
	}
	nameTok := NameUnset
	if name != "" {
		nameTok = NameToken(name)
	}
	return prefix + ".req." + tenant + "." + server + "." + strings.Join(segs, ".") + "." + nameTok, nil
}

// ParsedSubject is the result of ParseSubject.
type ParsedSubject struct {
	Tenant string
	Server string
	Method string // MCP form, with slashes
	Name   string // the raw name token, possibly NameUnset
}

// ParseSubject decomposes a request subject. It trusts nothing: every token
// is re-validated, because the subject is the authorization statement the
// NATS server enforced and the rest of the gateway builds on.
func ParseSubject(subject, prefix string) (*ParsedSubject, error) {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	head := prefix + ".req."
	rest, ok := strings.CutPrefix(subject, head)
	if !ok {
		return nil, fmt.Errorf("wire: subject %q does not start with %q", subject, head)
	}
	toks := strings.Split(rest, ".")
	// tenant + server + at least one method token + name
	if len(toks) < 4 {
		return nil, fmt.Errorf("wire: subject %q has too few tokens", subject)
	}
	for _, tok := range toks {
		if !TokenSafe(tok) {
			return nil, fmt.Errorf("wire: subject %q has invalid token %q", subject, tok)
		}
	}
	return &ParsedSubject{
		Tenant: toks[0],
		Server: toks[1],
		Method: strings.Join(toks[2:len(toks)-1], "/"),
		Name:   toks[len(toks)-1],
	}, nil
}

// EndpointSubject is the micro endpoint subject fronting one server across
// all tenants: {prefix}.req.*.{server}.>
func EndpointSubject(prefix, server string) (string, error) {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if !TokenSafe(server) {
		return "", fmt.Errorf("wire: server %q is not subject-token safe", server)
	}
	return prefix + ".req.*." + server + ".>", nil
}
