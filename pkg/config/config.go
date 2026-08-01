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

// Package config loads the gateway's JSON configuration. JSON (not YAML)
// because .mcp.json and claude_desktop_config.json already are, and because
// encoding/json costs no new dependency.
//
// There is one entry point per config source, because the two properties that
// distinguish them are security-relevant and must be visible at the call site:
// whether unknown fields are rejected, and whether ${VAR} is expanded from the
// gateway's environment. Expansion is the credential-injection point for a
// document the OPERATOR authored (Parse, ParseInline); a document the
// controller sends is not expanded (ParseFetched), because the controller
// resolves secrets itself and must not be able to read the gateway pod's
// environment back out through one.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// Config is the gateway configuration.
type Config struct {
	NATS       NATS              `json:"nats"`
	Servers    map[string]Server `json:"servers"`
	Pool       Pool              `json:"pool"`
	ClaimCheck *ClaimCheck       `json:"claimCheck"`
}

// ClaimCheck enables parking oversize responses in a JetStream Object Store
// (presence = enabled; requires JetStream on the NATS server). Boot-fixed,
// like the pool.
type ClaimCheck struct {
	// MaxAge is the per-tenant bucket TTL (Go duration, default "5m") — the
	// cleanup backstop behind the client's eager delete.
	MaxAge string `json:"maxAge"`
	// MaxBytes caps each tenant's bucket (default 1GiB).
	MaxBytes int64 `json:"maxBytes"`
}

// NATS is the gateway's connection settings.
type NATS struct {
	URL           string `json:"url"`
	CredsFile     string `json:"credsFile"`
	SubjectPrefix string `json:"subjectPrefix"`
	// InboxPrefix moves this process's own request/reply inboxes (the config
	// fetch, the `nats` cred resolver, JetStream) off the account-wide
	// _INBOX.> namespace, so its NATS identity can be granted just this
	// prefix. Empty keeps the nats.go default.
	InboxPrefix string `json:"inboxPrefix"`
	QueueGroup  string `json:"queueGroup"`
	// Tenant/User scope this instance's subjects. Tenant alone binds
	// {prefix}.req.{tenant}.*.{server}.> — an org deployment fronting its
	// servers for all of one tenant's users. Tenant+User binds
	// {prefix}.req.{tenant}.{user}.{server}.> — the per-user pod shape,
	// serving one identity, typically with that identity's credentials
	// injected into its environment. Both empty is the all-callers central
	// gateway; user without tenant is invalid.
	Tenant string `json:"tenant"`
	User   string `json:"user"`
}

// Server declares one fronted MCP server.
type Server struct {
	// Protocol the backend speaks: "2025-11-25" (default — that is what
	// exists in the wild) or "2026-07-28".
	Protocol string `json:"protocol"`
	// Transport: "stdio" or "http".
	Transport string `json:"transport"`

	// stdio transport.
	Command string            `json:"command"`
	Args    []string          `json:"args"`
	Env     map[string]string `json:"env"`

	// http transport.
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`

	// AllowPlaintext permits an http:// url and auth.tokenUrl for this server.
	// Both carry credentials — the injected Authorization header on every call,
	// and the client secret / subject token / refresh token respectively — so
	// plaintext is rejected by default and loopback is the only automatic
	// exemption.
	//
	// The case this exists for is the deployment where something outside the
	// gateway's view encrypts: a service mesh sidecar intercepting the pod's
	// traffic, an SSH tunnel. Those are legitimate and invisible from here, and
	// without a way to say so the check would simply be removed by the people
	// running them.
	AllowPlaintext bool `json:"allowPlaintext"`

	// DiscoverTTLMs is the freshness hint served as ttlMs on the results this
	// gateway synthesizes or bridges for a legacy server (default 300000).
	//
	// Applies only to a bridged legacy server; a 2026-07-28 server sets its
	// own hints and this field is ignored for it.
	//
	// It covers server/discover and the *list* methods — the server's shape,
	// which changes rarely and whose changes this bridge forwards as
	// */list_changed. It deliberately does NOT apply to resources/read: that
	// returns content, for which a 2025-11-25 server offers no invalidation
	// signal, so those results are always hinted as immediately stale.
	DiscoverTTLMs int `json:"discoverTtlMs"`

	// CacheScope is served as cacheScope on every cacheable result the
	// gateway bridges for a LEGACY server: "private" (default) or "public".
	// A 2026-07-28 server sets its own and is passed through untouched, so
	// setting this on one is rejected rather than silently ignored.
	//
	// The default is deliberately the restrictive one. Backends are pooled
	// per (server, tenant[, user, credential-generation]) and credentials are
	// injected per caller, so a result is generally NOT safe for a shared
	// cache to hand to a different authorization context — which is exactly
	// what "public" licenses. Set it only for a server whose listings are
	// provably identical for every caller.
	CacheScope string `json:"cacheScope"`

	// Auth selects how backend credentials are resolved (absent = static =
	// env/headers above). Additive by design: an older gateway ignoring this
	// field (the forward-compatible parse) runs the server with no resolved
	// credentials — per-user modes keep secrets out of env/headers, so it
	// fails closed rather than leaking a shared secret.
	Auth *Auth `json:"auth"`
}

// Auth mode names. Modes marked per-user resolve a distinct credential per
// caller; the rest resolve one credential shared by the server's callers.
const (
	AuthStatic                 = "static"                   // config env/headers (default)
	AuthExec                   = "exec"                     // per-user: credential-helper command
	AuthFile                   = "file"                     // mounted/rotated credentials file
	AuthOAuthClientCredentials = "oauth-client-credentials" // RFC 6749 service identity
	AuthOAuthTokenExchange     = "oauth-token-exchange"     // per-user: RFC 8693
	AuthOAuthRefresh           = "oauth-refresh"            // per-user: refresh_token grant
	AuthNATS                   = "nats"                     // per-user: controller over request/reply
)

// Auth configures a server's credential resolution (pkg/backend/cred). Only
// the fields for the selected mode apply; per-user modes must not put user
// secrets in the server's env/headers.
type Auth struct {
	Mode string `json:"mode"`

	// exec mode.
	Command string            `json:"command,omitempty"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`

	// file mode. Path may contain {tenant}/{user}/{server}; TTL (Go
	// duration) applies when the file carries no expiresAt.
	Path string `json:"path,omitempty"`
	TTL  string `json:"ttl,omitempty"`

	// oauth-* modes.
	TokenURL         string `json:"tokenUrl,omitempty"`
	ClientID         string `json:"clientId,omitempty"`
	ClientSecret     string `json:"clientSecret,omitempty"`
	Scope            string `json:"scope,omitempty"`
	Audience         string `json:"audience,omitempty"`
	SubjectTokenFile string `json:"subjectTokenFile,omitempty"` // oauth-token-exchange
	SubjectTokenType string `json:"subjectTokenType,omitempty"` // oauth-token-exchange
	RefreshTokenFile string `json:"refreshTokenFile,omitempty"` // oauth-refresh

	// nats mode: subject prefix override. Unset derives it from the wire
	// subject prefix ("{subjectPrefix}.cred"), so one configured prefix
	// governs both the wire and the cred exchange.
	Subject string `json:"subject,omitempty"`
}

// Dynamic reports whether credentials come from a resolver rather than the
// server's static env/headers.
func (a *Auth) Dynamic() bool {
	return a != nil && a.Mode != "" && a.Mode != AuthStatic
}

// PerUser reports whether resolved credentials differ per caller — these
// servers are pooled per user, the rest stay shared per tenant.
func (a *Auth) PerUser() bool {
	if a == nil {
		return false
	}
	switch a.Mode {
	case AuthExec, AuthNATS, AuthOAuthTokenExchange, AuthOAuthRefresh:
		return true
	case AuthFile:
		// A {user}-templated path is per-user by construction; without it
		// the file is one shared credential. Deciding from the path keeps
		// the grain truthful — a fixed shared grain here would silently
		// resolve every caller's file as user "_".
		return strings.Contains(a.Path, "{user}")
	}
	return false
}

// requireHTTPS rejects a URL that would carry credentials in the clear.
// allowPlaintext is the server's opt-out; loopback is exempt unconditionally,
// because that traffic reaches no network anyone can read and http://127.0.0.1
// is what a local MCP server serves.
//
// An unparseable or non-http(s) URL is rejected here too: net/http is the only
// thing that ever dials these, so anything else is a config error that would
// otherwise surface as a failed request much later.
func requireHTTPS(server, field, raw string, allowPlaintext bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("server %q: %s %q is not a valid URL: %w", server, field, raw, err)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if allowPlaintext || isLoopbackHost(u.Hostname()) {
			return nil
		}
		return fmt.Errorf(
			"server %q: %s %q sends credentials in cleartext; use https, or set allowPlaintext if something outside the gateway encrypts this hop (a service mesh sidecar, a tunnel)",
			server, field, raw,
		)
	default:
		return fmt.Errorf("server %q: %s %q must be https (or http to loopback)", server, field, raw)
	}
}

// isLoopbackHost reports whether a URL host can only reach this machine.
// "localhost" and anything under .localhost count: RFC 6761 reserves them to
// resolve to a loopback address, and rejecting the name every developer types
// while accepting the literal address it resolves to would just teach people
// to switch the check off.
func isLoopbackHost(host string) bool {
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// validate rejects unknown modes and missing per-mode parameters.
func (a *Auth) validate(server string, allowPlaintext bool) error {
	switch a.Mode {
	case "", AuthStatic, AuthNATS:
	case AuthExec:
		if a.Command == "" {
			return fmt.Errorf("server %q: auth mode %q requires command", server, a.Mode)
		}
	case AuthFile:
		if a.Path == "" {
			return fmt.Errorf("server %q: auth mode %q requires path", server, a.Mode)
		}
	case AuthOAuthClientCredentials:
		if a.TokenURL == "" || a.ClientID == "" || a.ClientSecret == "" {
			return fmt.Errorf("server %q: auth mode %q requires tokenUrl, clientId, clientSecret", server, a.Mode)
		}
	case AuthOAuthTokenExchange:
		if a.TokenURL == "" || a.SubjectTokenFile == "" {
			return fmt.Errorf("server %q: auth mode %q requires tokenUrl and subjectTokenFile", server, a.Mode)
		}
	case AuthOAuthRefresh:
		if a.TokenURL == "" || a.ClientID == "" || a.RefreshTokenFile == "" {
			return fmt.Errorf("server %q: auth mode %q requires tokenUrl, clientId, refreshTokenFile", server, a.Mode)
		}
	default:
		return fmt.Errorf("server %q: unknown auth mode %q", server, a.Mode)
	}
	if a.TTL != "" {
		if err := validateDuration(fmt.Sprintf("server %q: auth ttl", server), a.TTL); err != nil {
			return err
		}
	}
	// The cred subject is a PUBLISH prefix ({subject}.{tenant}.{user}.{server})
	// and NATS will not publish to a subject holding a wildcard. Unchecked, the
	// mistake surfaces on the first credential resolution as "no responders
	// available" — which points the operator at a missing controller instead of
	// at this line.
	if a.Subject != "" {
		if err := wire.ValidateSubjectPrefix(a.Subject); err != nil {
			return fmt.Errorf("server %q: auth subject: %w", server, err)
		}
	}
	if a.TokenURL != "" {
		if err := requireHTTPS(server, "auth tokenUrl", a.TokenURL, allowPlaintext); err != nil {
			return err
		}
	}
	return nil
}

// Pool mirrors backend.PoolConfig knobs.
type Pool struct {
	MaxConcurrent     int    `json:"maxConcurrent"`
	MaxProcsPerTenant int    `json:"maxProcsPerTenant"`
	IdleTTL           string `json:"idleTtl"`
	MaxLifetime       string `json:"maxLifetime"`
}

// Load reads a config FILE and parses it strictly (unknown fields are an
// error), because a hand-authored file's typo should fail loudly.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	cfg, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return cfg, nil
}

// Parse expands ${VAR} from the gateway's environment, decodes STRICTLY
// (unknown fields rejected), and validates config bytes. Use for
// human-authored config (the file source), where an unknown key almost
// certainly means a typo.
//
// A config with zero servers is valid: it is the legitimate steady state of a
// gateway whose servers have all been removed.
func Parse(raw []byte) (*Config, error) {
	return parse(raw, true, envLookup)
}

// ParseInline parses the inline document (--config-json / NATSMCP_CONFIG_JSON).
// ${VAR} expands from the gateway's environment: the document is plain-only and
// whatever creates the pod injects that pod's credentials beside it, so
// expansion is how they meet.
//
// Unknown fields are ignored, for the reason described on ParseFetched — an
// inline document is machine-generated (the controller mints the pod spec), so
// a newer controller's additive field must not crash-loop an older gateway
// through a rollback.
func ParseInline(raw []byte) (*Config, error) {
	return parse(raw, false, envLookup)
}

// ParseFetched parses a config the controller served over NATS. ${VAR} is
// REFUSED rather than expanded: the controller holds the secrets and sends
// resolved values, and a document from the control plane must not be able to
// name a variable in the gateway pod's environment and read it back out
// through a backend argument (see refuseLookup).
//
// Unknown fields are ignored, so a gateway can consume a config emitted by a
// NEWER controller that added fields this build does not know yet. During a
// rolling upgrade the whole fleet reads the same controller-emitted config, and
// an older gateway must not reject it wholesale just because a newer field
// appeared.
//
// Discipline this implies: new config fields must be ADDITIVE and safe for an
// older gateway to ignore. A change that an old gateway ignoring would make
// UNSAFE (e.g. a mandatory sandbox flag) must instead be gated behind an
// explicit, rejected version bump — never a silently-dropped field.
func ParseFetched(raw []byte) (*Config, error) {
	return parse(raw, false, refuseLookup)
}

func parse(raw []byte, strict bool, resolve lookup) (*Config, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if strict {
		dec.DisallowUnknownFields()
	}
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	// Decode, then expand, then validate. Expanding the document text instead
	// would let a variable's value close its JSON string and splice in fields;
	// validating before expansion would judge the placeholder rather than the
	// value that actually reaches the backend.
	if err := expand(&cfg, resolve); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// validateDuration parses a config duration and rejects a negative one.
//
// Zero stays legal because it is the documented "use the default" sentinel:
// every consumer of these fields tests <= 0 and substitutes its own default
// (pool 5m/1h, cred file 1m, claim bucket 5m), and the equivalent flags say so
// ("0 = pool default, 5m"). A negative value lands in that same branch — so
// "-30m" is not a shorter TTL, it is the default TTL, silently, with nothing
// logged for the operator who asked for something else.
func validateDuration(what, v string) error {
	d, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if d < 0 {
		return fmt.Errorf("%s: must not be negative (got %q)", what, v)
	}
	return nil
}

func (c *Config) validate() error {
	if cc := c.ClaimCheck; cc != nil {
		if cc.MaxAge != "" {
			if err := validateDuration("claimCheck.maxAge", cc.MaxAge); err != nil {
				return err
			}
		}
		if cc.MaxBytes < 0 {
			return fmt.Errorf("claimCheck.maxBytes must be >= 0")
		}
	}
	if c.Pool.IdleTTL != "" {
		if err := validateDuration("pool.idleTtl", c.Pool.IdleTTL); err != nil {
			return err
		}
	}
	if c.Pool.MaxLifetime != "" {
		if err := validateDuration("pool.maxLifetime", c.Pool.MaxLifetime); err != nil {
			return err
		}
	}
	// The wire prefix fronts every subscription this gateway binds, and it is
	// the one setting nothing downstream re-checks: micro accepts "*" in an
	// endpoint subject and NATS binds it, so an unvalidated "mcp.*" widens the
	// authz-bearing subscription to every prefix in the account and never
	// fails.
	if c.NATS.SubjectPrefix != "" {
		if err := wire.ValidateSubjectPrefix(c.NATS.SubjectPrefix); err != nil {
			return fmt.Errorf("nats.subjectPrefix: %w", err)
		}
	}
	if c.NATS.QueueGroup != "" {
		if err := wire.ValidateQueueGroup(c.NATS.QueueGroup); err != nil {
			return fmt.Errorf("nats.queueGroup: %w", err)
		}
	}
	if c.NATS.InboxPrefix != "" {
		if err := wire.ValidateSubjectPrefix(c.NATS.InboxPrefix); err != nil {
			return fmt.Errorf("nats.inboxPrefix: %w", err)
		}
	}
	if c.NATS.SubjectPrefix != "" {
		if err := wire.ValidateSubjectPrefix(c.NATS.SubjectPrefix); err != nil {
			return fmt.Errorf("nats.subjectPrefix: %w", err)
		}
	}
	if c.NATS.Tenant == "" && c.NATS.User != "" {
		return fmt.Errorf("nats: scoping with a user requires a tenant (got user=%q)", c.NATS.User)
	}
	if c.NATS.Tenant != "" && !wire.TokenSafe(c.NATS.Tenant) {
		return fmt.Errorf("nats: tenant %q is not subject-token safe (%s)", c.NATS.Tenant, `A-Za-z0-9_-`)
	}
	if c.NATS.User != "" && !wire.TokenSafe(c.NATS.User) {
		return fmt.Errorf("nats: user %q is not subject-token safe (%s)", c.NATS.User, `A-Za-z0-9_-`)
	}
	for name, s := range c.Servers {
		if !wire.TokenSafe(name) {
			return fmt.Errorf("server name %q is not subject-token safe (%s)", name, `A-Za-z0-9_-`)
		}
		// "__" is reserved as the tool-namespacing delimiter (mcp__<server>__
		// <tool>) that aggregating clients use to flatten many servers into one
		// toolset. A server named with "__" could collide with a different
		// (server, tool) pair's namespaced name, so forbid it up front.
		if strings.Contains(name, "__") {
			return fmt.Errorf("server name %q must not contain \"__\" (reserved as the tool-namespacing delimiter)", name)
		}
		switch s.Protocol {
		case "", mcpspec.LegacyProtocolVersion, mcpspec.ProtocolVersion:
		default:
			return fmt.Errorf("server %q: unknown protocol %q", name, s.Protocol)
		}
		// Rejected rather than defaulted: a typo'd cacheScope silently
		// becoming "private" is fine, but silently becoming "public" would
		// license shared caches to cross authorization contexts.
		switch s.CacheScope {
		case "", mcpspec.CacheScopePrivate, mcpspec.CacheScopePublic:
		default:
			return fmt.Errorf("server %q: unknown cacheScope %q (want %q or %q)",
				name, s.CacheScope, mcpspec.CacheScopePrivate, mcpspec.CacheScopePublic)
		}
		// cacheScope is consumed only by the legacy bridge — a modern server
		// emits its own hints and is passed through untouched. Rejected rather
		// than ignored so an operator cannot come away believing they had
		// constrained sharing when nothing reads the setting.
		//
		// discoverTtlMs gets no such check even though it is equally
		// bridge-only: it predates this validation, so configs already carry it
		// on modern servers. Failing them now would break a running fleet's
		// hot reload (pkg/configsource) to correct a harmless no-op. Its doc
		// comment says where it applies.
		if s.CacheScope != "" && s.Protocol == mcpspec.ProtocolVersion {
			return fmt.Errorf(
				"server %q: cacheScope applies only to bridged %s servers; a %s server sets its own",
				name, mcpspec.LegacyProtocolVersion, mcpspec.ProtocolVersion,
			)
		}
		switch s.Transport {
		case "", "stdio":
			if s.Command == "" {
				return fmt.Errorf("server %q: stdio transport requires command", name)
			}
		case "http":
			if s.URL == "" {
				return fmt.Errorf("server %q: http transport requires url", name)
			}
			if err := requireHTTPS(name, "url", s.URL, s.AllowPlaintext); err != nil {
				return err
			}
		default:
			return fmt.Errorf("server %q: unknown transport %q", name, s.Transport)
		}
		if s.Auth != nil {
			if err := s.Auth.validate(name, s.AllowPlaintext); err != nil {
				return err
			}
		}
	}
	return nil
}

// ServerNames returns the configured server names, sorted, so callers (the
// wire's initial server set, reload logs) see a deterministic order.
func (c *Config) ServerNames() []string {
	names := make([]string, 0, len(c.Servers))
	for name := range c.Servers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
