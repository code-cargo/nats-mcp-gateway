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
	"errors"
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

// Auth mode names. Whether a mode resolves a distinct credential per caller or
// one shared by the server's callers is Auth.PerUser's answer, not the mode's:
// the file-reading modes take it from a {user} placeholder in the path they
// read, and exec and nats default to per-user and take an explicit "perUser".
const (
	AuthStatic                 = "static"                   // config env/headers (default)
	AuthExec                   = "exec"                     // credential-helper command
	AuthFile                   = "file"                     // mounted/rotated credentials file
	AuthOAuthClientCredentials = "oauth-client-credentials" // RFC 6749 service identity
	AuthOAuthTokenExchange     = "oauth-token-exchange"     // RFC 8693
	AuthOAuthRefresh           = "oauth-refresh"            // refresh_token grant
	AuthNATS                   = "nats"                     // controller over request/reply
)

// Auth configures a server's credential resolution (pkg/backend/cred). Only
// the fields for the selected mode apply; per-user modes must not put user
// secrets in the server's env/headers.
type Auth struct {
	Mode string `json:"mode"`

	// exec mode. WaitDelay (Go duration) bounds how long the helper's stdout
	// may stay open after it exits — a grandchild holding the pipe, typically
	// a cloud CLI that backgrounds a refresh. Reaching it FAILS the resolve
	// rather than extending it, so a helper whose child legitimately holds the
	// pipe longer than the default needs this raised; unset takes the default.
	Command   string            `json:"command,omitempty"`
	Args      []string          `json:"args,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	WaitDelay string            `json:"waitDelay,omitempty"`

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

	// PerUserOverride declares the credential grain for the two modes that
	// cannot derive one: exec and nats hand the resolver (tenant, user,
	// server) and let it answer, so nothing in this document says whether what
	// comes back is one credential per caller or one per tenant. The default
	// stays per-user (fail closed); "perUser": false is how a tenant-level
	// helper or controller says otherwise.
	//
	// Without it, a deployment with no per-user NATS auth — where every caller
	// sends the documented "_" token — takes -32014 on every request to that
	// server, and the remedy the refusal names (stand up per-user NATS auth) is
	// not one a shared-credential deployment can reach. Rejected on the other
	// modes, which read their grain off a {user}-templated path.
	PerUserOverride *bool `json:"perUser,omitempty"`
}

// Dynamic reports whether credentials come from a resolver rather than the
// server's static env/headers.
func (a *Auth) Dynamic() bool {
	return a != nil && a.Mode != "" && a.Mode != AuthStatic
}

// PerUser reports whether resolved credentials differ per caller — these
// servers are pooled per user, the rest stay shared per tenant.
//
// The grain is derived from the document wherever the document can answer it:
// the three file-reading modes select their credential with a {user}
// placeholder, so a path without one is a provably shared credential (a
// projected service-account token, a fixed refresh token) and pooling it per
// user would multiply processes over one file. Claiming per-user there also
// costs the deployment everything: the proxy refuses an unattributed caller on
// a per-user server, so a shared credential mislabelled per-user answers
// -32014 to every request a deployment without per-user NATS auth can make.
//
// exec and nats cannot be read off the document — the resolver decides — so
// they default to per-user and take PerUserOverride.
func (a *Auth) PerUser() bool {
	if a == nil {
		return false
	}
	if a.PerUserOverride != nil {
		return *a.PerUserOverride
	}
	switch a.Mode {
	case AuthExec, AuthNATS:
		return true
	case AuthFile:
		// A {user}-templated path is per-user by construction; without it
		// the file is one shared credential. Deciding from the path keeps
		// the grain truthful — a fixed shared grain here would silently
		// resolve every caller's file as user "_".
		return strings.Contains(a.Path, "{user}")
	case AuthOAuthTokenExchange:
		// The subject token IS the caller's identity assertion, and
		// cred.OAuthTokenExchange expands the same placeholders as the file
		// mode when it reads the file. One fixed file therefore exchanges one
		// assertion for every caller.
		return strings.Contains(a.SubjectTokenFile, "{user}")
	case AuthOAuthRefresh:
		// Likewise through cred.FileTokenStore, which both reads and rotates
		// the token at the expanded path.
		return strings.Contains(a.RefreshTokenFile, "{user}")
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
//
// Every rejection names the URL through redactURL. These messages travel much
// further than a boot failure — Parse re-validates on every reload, and
// pkg/configsource logs the error it keeps rejecting on each poll tick — so a
// document carrying a secret in its userinfo or query string would re-emit it
// for as long as it stayed broken.
func requireHTTPS(server, field, raw string, allowPlaintext bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("server %q: %s %q is not a valid URL: %w",
			server, field, redactURL(raw), parseReason(err))
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
			server, field, redactURL(raw),
		)
	default:
		return fmt.Errorf("server %q: %s %q must be https (or http to loopback)", server, field, redactURL(raw))
	}
}

// redacted is spelled the way net/url.URL.Redacted spells it, so it reads as a
// redaction rather than as somebody's actual password.
const redacted = "xxxxx"

// redactURL renders a URL for an error message with its credential-bearing
// parts gone. Scheme, host and path survive, which is what identifies the
// server being rejected; userinfo and query string do not, because that is
// where a secret gets expanded into.
//
// u.Redacted() is not enough on either half: it masks a userinfo PASSWORD and
// leaves a colon-less userinfo — the shape a bare token takes — verbatim, and
// it never touches the query, which is where an api_key rides.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		// Unparseable is when a secret is most likely to be in there (an
		// unescaped byte in a password is the usual cause) and nothing here can
		// locate its parts, so keep the scheme and drop the rest.
		if scheme, _, ok := strings.Cut(raw, "://"); ok && scheme != "" {
			return scheme + "://" + redacted
		}
		return redacted
	}
	if u.Opaque != "" {
		// An opaque URL ("ftp:user:pw@h/p") parses without error and populates
		// neither User nor RawQuery, so every branch below finds nothing and
		// the whole credential goes out verbatim. Nothing here can locate the
		// parts of a form net/url declined to break up, and this reaches a
		// message pkg/configsource re-logs on every poll tick, so keep the
		// scheme — which is what the rejection is usually about — and drop it.
		return u.Scheme + ":" + redacted
	}
	if u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			// The username stays: "connecting as the wrong identity" is a
			// diagnosis this line can deliver, and it is never the secret when
			// a password sits beside it.
			u.User = url.UserPassword(u.User.Username(), redacted)
		} else {
			u.User = url.User(redacted)
		}
	}
	if u.RawQuery != "" {
		// Whole, not per parameter: which key held the secret is not worth the
		// second parse, and a query this gateway never reads has nothing else
		// to say about which backend was rejected.
		u.RawQuery = redacted
	}
	return u.String()
}

// parseReason unwraps a *url.Error to the reason it carries. Its own Error()
// quotes the string it could not parse, and a password malformed enough to
// break parsing is the likeliest kind to be mistyped — %w on it would put the
// credential straight back beside the copy redactURL just removed. What
// survives is bounded: an escape error quotes the two offending bytes.
func parseReason(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return uerr.Err
	}
	return err
}

// isLoopbackHost reports whether a URL host can only reach this machine.
// "localhost" and anything under .localhost count: RFC 6761 reserves them to
// resolve to a loopback address, and rejecting the name every developer types
// while accepting the literal address it resolves to would just teach people
// to switch the check off.
//
// Lowercased first, because url.Parse lowercases the SCHEME and leaves the
// host exactly as written — so "http://Localhost:3000/mcp" arrives here
// spelled differently from the same host, and DNS does not distinguish them.
// A rejection that turns on the shift key is the surest way to get
// allowPlaintext set fleet-wide.
func isLoopbackHost(host string) bool {
	host = strings.ToLower(host)
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
	// Rejected rather than ignored, for the same reason cacheScope is: every
	// other mode derives its grain from whether the path it reads carries
	// {user}, so honoring an override that contradicts the path would pool per
	// user against one shared file — and silently ignoring it would leave an
	// operator believing they had shared a credential the gateway is still
	// resolving per caller.
	if a.PerUserOverride != nil {
		switch a.Mode {
		case AuthExec, AuthNATS:
		default:
			return fmt.Errorf(
				"server %q: auth mode %q does not take perUser (only %q and %q do); the rest are per-user exactly when the path they read contains {user}",
				server, a.Mode, AuthExec, AuthNATS)
		}
	}
	if a.TTL != "" {
		if err := validateDuration(fmt.Sprintf("server %q: auth ttl", server), a.TTL); err != nil {
			return err
		}
	}
	if a.WaitDelay != "" {
		if a.Mode != AuthExec {
			return fmt.Errorf("server %q: auth mode %q does not take waitDelay (only %q does)",
				server, a.Mode, AuthExec)
		}
		if err := validateDuration(fmt.Sprintf("server %q: auth waitDelay", server), a.WaitDelay); err != nil {
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
	// The wire prefix fronts every subscription this gateway binds, and
	// nothing below this layer objects to a wildcard in it: micro accepts "*"
	// in an endpoint subject and NATS binds it, so an unvalidated "mcp.*"
	// widens the authz-bearing subscription to every prefix in the account.
	// wire.Serve makes the same check as the one place every config source
	// reaches; this is what names the config field and fails before the
	// connection is opened.
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
		if err := wire.ValidateInboxPrefix(c.NATS.InboxPrefix); err != nil {
			return fmt.Errorf("nats.inboxPrefix: %w", err)
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
