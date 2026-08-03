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
	"unicode"

	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// Subject grammar:
//
//	{prefix}.req.{tenant}.{user}.{server}.{method-tokens}.{name}
//
// The grammar IS the authorization model: NATS subject permissions on these
// tokens are the entire authz plane, so every rule here is security-relevant.
//
// {user} carries the caller's identity for attribution. It is trustworthy for
// the same reason {tenant} is: NATS enforces which subjects a connection may
// publish to, so with a per-user auth model (e.g. NATS auth callout minting a
// JWT scoped to mcp.v1.req.{tenant}.{user}.>) a caller cannot publish under
// another user's token. Deployments without per-user auth use the "_"
// placeholder ("unattributed"); the gateway treats {user} as opaque and never
// derives authorization from it — NATS already did.
//
// The method occupies a variable number of tokens ("tools/call" -> "tools.call",
// "resources/templates/list" -> three tokens), so parsing anchors on position:
// name is always the LAST token, method is everything between server and name.
//
// The name token is params.name for named methods; the literal "_" for
// everything else, and as the fallback for names that are not subject-token
// safe. See NameToken for the two rules that keep the "_" fallback sound, and
// SubjectNameToken for which methods are named on the subject at all.

// DefaultPrefix is the default subject prefix.
const DefaultPrefix = "mcp.v1"

// NameUnset is the name token used when a method carries no name, or when the
// name is not subject-token safe.
const NameUnset = "_"

// UserUnattributed is the {user} token for deployments without per-user auth.
const UserUnattributed = "_"

var tokenRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// TokenSafe reports whether s may appear as a single subject token.
func TokenSafe(s string) bool {
	return s != "" && tokenRe.MatchString(s)
}

// ValidateSubjectPrefix checks that s is usable as a LITERAL subject prefix —
// a dotted run of safe tokens, no wildcard, no empty token. Callers prefix the
// error with the setting name.
//
// The wildcard rejection is the security-relevant one. A prefix is what a
// narrow NATS grant is written against, so a "*" or ">" inside it would widen
// the very subscription the prefix exists to narrow; an empty token (leading,
// doubled or trailing dot) subscribes to something other than what was
// written. nats.CustomInboxPrefix catches the first three of those, but only
// at connect time and with no mention of which setting was wrong.
//
// The WIRE prefix has no such downstream check at all: micro's endpoint-subject
// check permits "*" anywhere and NATS binds the result, so "mcp.*" quietly
// subscribes this instance to every prefix in the account.
func ValidateSubjectPrefix(s string) error {
	return validateTokenRun("subject prefix", s, wireTokenSafe)
}

// ValidateQueueGroup checks that s is usable as a NATS queue group — the same
// dotted run of literal tokens, for a different reason.
//
// A group name is compared LITERALLY: "mcpgw.*" is not a pattern, it is a
// group whose name contains a star, so a wildcard written in the belief that
// it scopes subjects does something other than what it says and nothing ever
// complains. The malformed forms do fail, but late and anonymously — micro
// rejects a space at the first config apply as "invalid endpoint queue group",
// naming neither the setting nor the value, by which point the process has
// connected and looks healthy.
func ValidateQueueGroup(s string) error {
	return validateTokenRun("queue group", s, wireTokenSafe)
}

// ValidateInboxPrefix checks an inbox prefix, which is held to the narrow
// alphabet the other two are not: nats.CustomInboxPrefix is the consumer, the
// setting has been validated since before it had company, and no deployment
// was relying on a wider one.
func ValidateInboxPrefix(s string) error {
	return validateTokenRun("inbox prefix", s, TokenSafe)
}

// validateTokenRun walks a dotted run of literal subject tokens. what names
// the run in the error ("subject prefix", "queue group"); safe decides the
// alphabet, which differs by who wrote the value.
func validateTokenRun(what, s string, safe func(string) bool) error {
	for _, tok := range strings.Split(s, ".") {
		switch {
		case tok == "":
			return fmt.Errorf("invalid %s %q: empty token (leading, doubled, or trailing dot)", what, s)
		case strings.ContainsAny(tok, "*>"):
			return fmt.Errorf("invalid %s %q: wildcards (* and >) are not allowed", what, s)
		case !safe(tok):
			return fmt.Errorf("invalid %s %q: token %q is not usable as a subject token", what, s, tok)
		}
	}
	return nil
}

// wireTokenSafe is what the WIRE carries in a token, as opposed to what this
// project generates.
//
// TokenSafe stays narrow on purpose: it guards names that arrive from a caller
// — tenant, user, server, claim id — where a small alphabet is itself the
// security property. A subject prefix, a queue group and an auth subject are
// none of those. They are typed once into a config by the operator, two of
// them went unvalidated until the checks were added, and NATS carries far more
// than A-Za-z0-9_- in a token. Holding them to the caller-derived alphabet
// refuses "$MCP.v1", "mcp:v1", "tenant.acmé", "mcpgw/acme" and "ctl:creds" —
// values that were being served before anything validated them. On the fetch
// path that refusal is fatal at boot but survivable on reload, so the fleet
// keeps running and only restarting pods fail, which is the worst way to find
// out.
//
// What the checks are FOR is unchanged, because the hazard was never the
// alphabet: a wildcard, a space or an empty token in the wire prefix widens
// the authz-bearing subscription. validateTokenRun still refuses those, and a
// NUL would truncate the subject at the socket.
func wireTokenSafe(tok string) bool {
	return !strings.ContainsFunc(tok, func(r rune) bool {
		return unicode.IsSpace(r) || !unicode.IsPrint(r)
	})
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

// SubjectNameToken maps a method and its raw MCP name to the subject's name
// token. It is the only copy of that rule: BuildSubject picks the token a
// conformant client publishes to and pkg/proxy.Check picks the token it will
// accept, in packages that cannot see each other drift apart. When they did,
// the symptom was not a hole but a permanent -32020 on requests where subject,
// header and body all honestly agreed.
//
// Which methods are named at all is mcpspec.NameField's answer, so a method
// gaining a name there gains its subject token on both sides at once — rather
// than a Mcp-Name check plus a silent seat in the unnamed "_" slot, which is a
// per-name ACL that looks enforced and is not.
//
// A URI-named method is the deliberate exception, and it is keyed off the
// FIELD rather than off resources/read by name, because the reason is a
// property of URIs and not of that one method: for URIs the "_" fallback is
// the rule rather than the exception — every scheme-bearing URI contains a
// ":" and is not token-safe, so any grant broad enough to cover
// "file:///etc/passwd" already covers everything a per-URI token could have
// carved out. Deriving a token from the token-safe minority would make
// per-URI grants look expressible while changing nothing about what they
// authorize. Naming the method instead would hand that shape to the next
// uri-carrying method (resources/subscribe is the obvious one) silently, with
// both sides agreeing and no test to notice. URI-named methods are named in
// the header and the body, and authorized on the subject as one method.
func SubjectNameToken(method, name string) string {
	if field := mcpspec.NameField(method); field == "" || field == mcpspec.URIParam {
		return NameUnset
	}
	return NameToken(name)
}

// BuildSubject constructs the request subject. user is the caller's identity
// token ("_" / UserUnattributed when there is no per-user auth); method is the
// MCP form with slashes ("tools/call"); name is the raw MCP name ("" for
// methods that carry none).
func BuildSubject(prefix, tenant, user, server, method, name string) (string, error) {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if user == "" {
		user = UserUnattributed
	}
	if !TokenSafe(tenant) {
		return "", fmt.Errorf("wire: tenant %q is not subject-token safe", tenant)
	}
	if !TokenSafe(user) {
		return "", fmt.Errorf("wire: user %q is not subject-token safe", user)
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
	nameTok := SubjectNameToken(method, name)
	return prefix + ".req." + tenant + "." + user + "." + server + "." + strings.Join(segs, ".") + "." + nameTok, nil
}

// ParsedSubject is the result of ParseSubject.
type ParsedSubject struct {
	Tenant string
	User   string // caller identity for attribution; "_" when unattributed
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
	// tenant + user + server + at least one method token + name
	if len(toks) < 5 {
		return nil, fmt.Errorf("wire: subject %q has too few tokens", subject)
	}
	for _, tok := range toks {
		if !TokenSafe(tok) {
			return nil, fmt.Errorf("wire: subject %q has invalid token %q", subject, tok)
		}
	}
	return &ParsedSubject{
		Tenant: toks[0],
		User:   toks[1],
		Server: toks[2],
		Method: strings.Join(toks[3:len(toks)-1], "/"),
		Name:   toks[len(toks)-1],
	}, nil
}

// EndpointSubject is the micro endpoint subject fronting one server. The
// (tenant, user) pair selects one of three scopes, an unset token widening to
// the "*" wildcard:
//
//	both unset       {prefix}.req.*.*.{server}.>            central fleet, all callers
//	tenant only      {prefix}.req.{tenant}.*.{server}.>     one org, all its users
//	tenant + user    {prefix}.req.{tenant}.{user}.{server}.>  one caller (per-user pod)
//
// A user without a tenant is rejected: scoping by the attribution token alone
// would span every tenant, never a shape we bind. The tenant-only form is how
// a per-org deployment claims its whole org's slice; the fully-scoped form is
// what a per-user pod binds so NATS routes only that user's traffic to it.
func EndpointSubject(prefix, tenant, user, server string) (string, error) {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if !TokenSafe(server) {
		return "", fmt.Errorf("wire: server %q is not subject-token safe", server)
	}
	if tenant == "" && user != "" {
		return "", fmt.Errorf("wire: endpoint scoping with a user requires a tenant (got user=%q)", user)
	}
	tenantTok := "*"
	if tenant != "" {
		if !TokenSafe(tenant) {
			return "", fmt.Errorf("wire: tenant %q is not subject-token safe", tenant)
		}
		tenantTok = tenant
	}
	userTok := "*"
	if user != "" {
		if !TokenSafe(user) {
			return "", fmt.Errorf("wire: user %q is not subject-token safe", user)
		}
		userTok = user
	}
	return prefix + ".req." + tenantTok + "." + userTok + "." + server + ".>", nil
}
