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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend/cred"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend/legacy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
	"github.com/code-cargo/nats-mcp-gateway/pkg/configsource"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/proxy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/reconcile"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// minRecommendedPayload is the NATS max_payload below which large MCP
// results (tools/list, base64 content) will start failing with -32012.
const minRecommendedPayload = 8 * 1024 * 1024

// sourceKind identifies which of the three mutually-exclusive config sources
// the flags selected. Resolving it ONCE, in selectSource, is deliberate: the
// selection used to be re-derived from the raw flag fields in three places
// (the count check, the source constructor, the boot log), and they drifted —
// the log kept reporting a NATS fetch, with an empty subject, for an inline
// gateway. Everything downstream switches on this value instead.
type sourceKind int

const (
	sourceFile sourceKind = iota
	sourceInline
	sourceFetch
)

// selectSource resolves the selected config source, rejecting zero or many.
// This is the only place that knows the set of sources.
func (c *GatewayCmd) selectSource() (sourceKind, error) {
	var kind sourceKind
	n := 0
	if c.Config != "" {
		kind, n = sourceFile, n+1
	}
	if c.ConfigJSON != "" {
		kind, n = sourceInline, n+1
	}
	if c.ConfigSubject != "" {
		kind, n = sourceFetch, n+1
	}
	if n != 1 {
		return 0, fmt.Errorf("exactly one of --config, --config-subject, or --config-json is required")
	}
	return kind, nil
}

// describe renders the source for the boot log.
func (k sourceKind) describe(c *GatewayCmd) string {
	switch k {
	case sourceFile:
		return "file:" + c.Config
	case sourceInline:
		return "inline"
	default:
		return "nats:" + c.ConfigSubject
	}
}

// reloadsOnSighup reports whether SIGHUP re-reads this source. Only the file
// source does; the fetch source reloads on its own (change events plus the
// refetch interval) and the inline document never changes.
func (k sourceKind) reloadsOnSighup() bool { return k == sourceFile }

// bootParams are the boot-time settings that do NOT hot-reload: the gateway's
// own NATS connection and the wire's subject prefix / queue group / scoping.
type bootParams struct {
	url         string
	credsFile   string
	prefix      string
	inboxPrefix string
	queueGroup  string
	tenant      string
	user        string
	// pool is already parsed — the config schema carries durations as strings,
	// and the flags carry them as durations, so the conversion happens once
	// here rather than at the NewPool call.
	pool          backend.PoolConfig
	claimCheck    bool
	claimMaxAge   time.Duration
	claimMaxBytes int64
	// inline is the already-parsed --config-json document (nil for the other
	// sources): bootParams has to parse it to read pool/claimCheck, so the
	// source reuses that parse rather than doing a second one.
	inline *config.Config
}

// poolFromConfig converts the config schema's pool block to the pool's own
// shape. Durations are strings in the schema and validated at parse, so a
// failure here is unreachable and a zero means "unset" — NewPool defaults it.
func poolFromConfig(p config.Pool) backend.PoolConfig {
	out := backend.PoolConfig{
		MaxConcurrent:     p.MaxConcurrent,
		MaxProcsPerTenant: p.MaxProcsPerTenant,
	}
	if p.IdleTTL != "" {
		out.IdleTTL, _ = time.ParseDuration(p.IdleTTL)
	}
	if p.MaxLifetime != "" {
		out.MaxLifetime, _ = time.ParseDuration(p.MaxLifetime)
	}
	return out
}

func runGateway(c *GatewayCmd, g *Globals, version string) error {
	log := NewLogger(g)

	kind, err := c.selectSource()
	if err != nil {
		return err
	}

	// Resolve boot params. File mode reads them from the file (one time); the
	// fetch and inline sources take them from flags — fetch because we must
	// connect before we can fetch, inline because its document is plain-only
	// (the NATS URL, not the document, carries the credential).
	boot, err := c.bootParams(kind)
	if err != nil {
		return err
	}

	opts := []nats.Option{nats.Name("natsmcp-gateway"), nats.MaxReconnects(-1)}
	if boot.credsFile != "" {
		opts = append(opts, nats.UserCredentials(boot.credsFile))
	}
	if boot.inboxPrefix != "" {
		// Every request/reply this process issues — the config fetch, the
		// `nats` cred resolver, JetStream — derives its reply subject from the
		// connection, so this one option moves them all under a prefix the
		// identity can be granted ALONE. Without it that identity needs
		// `_INBOX.>`, the account's entire reply namespace: for a pod running
		// third-party MCP server code beside the gateway, a child process that
		// lifts the connection's credential could then subscribe there and read
		// every other tenant's credential replies.
		opts = append(opts, nats.CustomInboxPrefix(boot.inboxPrefix))
	}
	nc, err := nats.Connect(boot.url, opts...)
	if err != nil {
		return fmt.Errorf("connect NATS %s: %w", boot.url, err)
	}
	defer nc.Close()

	if mp := nc.MaxPayload(); mp < minRecommendedPayload {
		log.Warn("NATS max_payload is small for MCP traffic; large tool results will fail",
			"max_payload", mp, "recommended", minRecommendedPayload,
			"fix", "set max_payload: 8MB in nats-server config")
	}

	// Build the config source before the serving machinery, so a malformed
	// inline document fails the boot here rather than after the pool and wire
	// are up. The NATS source needs the connection; file/inline don't.
	source, reload := c.buildSource(kind, boot, nc, log)

	// The reconciler holds the live config; the pool factory resolves each
	// server's definition through it, so an evicted server always respawns
	// from the newest config. The credential registry lives beside it so the
	// resolvers' caches survive requests but not an auth-config change.
	var rec *reconcile.Reconciler
	registry := &credRegistry{nc: nc, log: log, prefix: boot.prefix}
	registry.live = func(name string) (config.Server, bool) {
		cur := rec.Current()
		if cur == nil {
			return config.Server{}, false
		}
		s, ok := cur.Servers[name]
		return s, ok
	}
	factory := func(key backend.Key) (backend.Backend, error) {
		cur := rec.Current()
		if cur == nil {
			return nil, fmt.Errorf("no config loaded yet")
		}
		s, ok := cur.Servers[key.Server]
		if !ok {
			return nil, fmt.Errorf("unknown server %q", key.Server)
		}
		resolver, _ := registry.lookup(key.Server, s)
		return buildBackend(key, s, resolver, log)
	}
	pool := backend.NewPool(boot.pool, factory, log)
	defer pool.Shutdown()

	// Claim-check: park oversize responses in a JetStream Object Store for
	// claim-accepting clients. JetStream being down is a boot warning, not a
	// boot failure — Put failures degrade to -32012 at request time.
	var claims wire.ClaimStore
	if boot.claimCheck {
		js, jsErr := jetstream.New(nc)
		if jsErr != nil {
			return fmt.Errorf("claim-check: %w", jsErr)
		}
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := js.AccountInfo(probeCtx); err != nil {
			log.Warn("claim-check enabled but JetStream is unreachable; oversize responses will fail with -32012 until it recovers", "err", err)
		}
		probeCancel()
		claims = &wire.ObjectClaims{JS: js, MaxAge: boot.claimMaxAge, MaxBytes: boot.claimMaxBytes}
	}

	// Start the wire with an EMPTY server set; the first applied config
	// populates it.
	px := proxy.New(pool, log)
	px.Creds = func(server string) (*cred.CachedResolver, bool) {
		cur := rec.Current()
		if cur == nil {
			return nil, false
		}
		s, ok := cur.Servers[server]
		if !ok {
			return nil, false
		}
		return registry.lookup(server, s)
	}
	// The queue-group default (scoped-aware) lives in wire.Serve, so an
	// empty group here is correct for every config source.
	ws, err := wire.Serve(nc, wire.ServerConfig{
		Prefix:     boot.prefix,
		QueueGroup: boot.queueGroup,
		Tenant:     boot.tenant,
		User:       boot.user,
		Version:    normalizeVersion(version),
		Claims:     claims,
	}, px.Handler())
	if err != nil {
		return err
	}
	rec = reconcile.New(ws, pool, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signals: SIGHUP reloads the file source; INT/TERM drain.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for s := range sig {
			if s == syscall.SIGHUP {
				// Answer either way, and answer accurately: an operator who
				// HUPs cannot otherwise tell "received and ignored" from
				// "signal never arrived" — and the fetch source DOES reload
				// (on change events and the refetch interval), just not from
				// this signal, so it must not be told its config is frozen.
				switch {
				case kind.reloadsOnSighup():
					log.Info("SIGHUP: reloading config", "source", kind.describe(c))
				case kind == sourceFetch:
					log.Info("SIGHUP ignored: the fetch source reloads on change events and --config-refetch, not on SIGHUP",
						"source", kind.describe(c))
				default:
					log.Info("SIGHUP ignored: the inline config is fixed for this process's life",
						"source", kind.describe(c))
				}
				if reload != nil {
					reload()
				}
				continue
			}
			log.Info("draining", "signal", s.String())
			cancel()
			return
		}
	}()

	log.Info("gateway starting", "nats", boot.url, "source", kind.describe(c))

	// Run the config loop. It returns when ctx is cancelled (signal) or on a
	// fatal initial-config error.
	runErr := configsource.Run(ctx, log, source, func(cfg *config.Config) error {
		_, err := rec.Apply(cfg)
		if err == nil {
			registry.prune(cfg.Servers)
		}
		return err
	})

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	drainErr := ws.Shutdown(shutdownCtx)

	if runErr != nil && ctx.Err() == nil {
		return runErr // a real error, not a clean signal-driven shutdown
	}
	return drainErr
}

// bootParams resolves the non-reloadable boot settings for the selected mode.
func (c *GatewayCmd) bootParams(kind sourceKind) (bootParams, error) {
	if kind == sourceFile {
		cfg, err := config.Load(c.Config)
		if err != nil {
			return bootParams{}, err
		}
		url := cfg.NATS.URL
		if url == "" {
			url = nats.DefaultURL
		}
		boot := bootParams{
			url:         url,
			credsFile:   cfg.NATS.CredsFile,
			prefix:      cfg.NATS.SubjectPrefix,
			inboxPrefix: cfg.NATS.InboxPrefix, // validated in config.validate()
			queueGroup:  cfg.NATS.QueueGroup,
			tenant:      cfg.NATS.Tenant,
			user:        cfg.NATS.User,
			pool:        poolFromConfig(cfg.Pool),
		}
		if cc := cfg.ClaimCheck; cc != nil {
			boot.claimCheck = true
			if cc.MaxAge != "" {
				boot.claimMaxAge, _ = time.ParseDuration(cc.MaxAge) // validated at load
			}
			boot.claimMaxBytes = cc.MaxBytes
		}
		return boot, nil
	}
	// Fetch and inline sources: connection, prefix, queue group and scope come
	// from flags. The file source validated these in config.validate(); do the
	// same here, because an unsafe token would otherwise sail past boot (an
	// empty server set never reaches EndpointSubject) and only fail once the
	// first config arrives.
	if c.ScopeTenant == "" && c.ScopeUser != "" {
		return bootParams{}, fmt.Errorf("--scope-user requires --scope-tenant (got user=%q)", c.ScopeUser)
	}
	if c.ScopeTenant != "" && !wire.TokenSafe(c.ScopeTenant) {
		return bootParams{}, fmt.Errorf("--scope-tenant %q is not subject-token safe (%s)", c.ScopeTenant, `A-Za-z0-9_-`)
	}
	if c.ScopeUser != "" && !wire.TokenSafe(c.ScopeUser) {
		return bootParams{}, fmt.Errorf("--scope-user %q is not subject-token safe (%s)", c.ScopeUser, `A-Za-z0-9_-`)
	}
	// Validated here rather than left to nats.Connect: the option's own check
	// misses spaces, and its "invalid custom prefix" names neither the setting
	// nor the value.
	if c.InboxPrefix != "" {
		if err := wire.ValidateSubjectPrefix(c.InboxPrefix); err != nil {
			return bootParams{}, fmt.Errorf("--inbox-prefix: %w", err)
		}
	}
	// wire.Serve rejects this too — this is what names the flag, and what
	// fails before we open a connection.
	if c.SubjectPrefix != "" {
		if err := wire.ValidateSubjectPrefix(c.SubjectPrefix); err != nil {
			return bootParams{}, fmt.Errorf("--subject-prefix: %w", err)
		}
	}

	boot := bootParams{
		url:         c.NatsURL,
		credsFile:   c.NatsCreds,
		prefix:      c.SubjectPrefix,
		inboxPrefix: c.InboxPrefix,
		queueGroup:  c.QueueGroup,
		tenant:      c.ScopeTenant,
		user:        c.ScopeUser,
		// Pool limits from flags, the same way claim-check settings come from
		// flags here: the fetch source has no document at boot to read them
		// from, and pool sizing is boot-fixed (the pool is built before the
		// first config arrives), so a fetched config could never supply them.
		// The inline document overrides these below — it DOES exist at boot.
		pool: backend.PoolConfig{
			MaxConcurrent:     c.PoolMaxConcurrent,
			MaxProcsPerTenant: c.PoolMaxProcsPerTenant,
			IdleTTL:           c.PoolIdleTTL,
			MaxLifetime:       c.PoolMaxLifetime,
		},
		claimCheck:    c.ClaimCheck,
		claimMaxAge:   c.ClaimMaxAge,
		claimMaxBytes: c.ClaimMaxBytes,
	}

	if kind == sourceInline {
		// Parse up front so a malformed NATSMCP_CONFIG_JSON fails the boot
		// loudly instead of leaving the gateway serving nothing. Parsed
		// forward-compatibly for the same reason the fetch source is: an
		// inline document is machine-generated (the controller mints the pod
		// spec), so a newer controller's additive field must not crash-loop an
		// older gateway through a rollback.
		cfg, err := config.ParseForwardCompatible([]byte(c.ConfigJSON))
		if err != nil {
			return bootParams{}, fmt.Errorf("config-json: %w", err)
		}
		// The inline document is PLAIN-ONLY: the token-bearing URL and the
		// scope must not sit in a pod spec, so they arrive as flags/env.
		// Silently ignoring a nats block would be dangerous rather than merely
		// surprising — a document asking to be scoped to one tenant would
		// serve every tenant — so reject it outright.
		if cfg.NATS != (config.NATS{}) {
			return bootParams{}, fmt.Errorf(`config-json: the "nats" block is not honored inline; use --nats-url/--nats-creds/--subject-prefix/--queue-group/--scope-tenant/--scope-user (or their NATSMCP_* env vars)`)
		}
		// pool and claimCheck carry no credentials, so the document does own
		// them — a per-org deployment fronting many users has to be able to
		// raise maxProcsPerTenant above the default. A pool block in the
		// document replaces the flag values wholesale; omit it to use them.
		if cfg.Pool != (config.Pool{}) {
			boot.pool = poolFromConfig(cfg.Pool)
		}
		if cc := cfg.ClaimCheck; cc != nil {
			boot.claimCheck = true
			if cc.MaxAge != "" {
				boot.claimMaxAge, _ = time.ParseDuration(cc.MaxAge) // validated at parse
			}
			boot.claimMaxBytes = cc.MaxBytes
		}
		boot.inline = cfg
	}
	return boot, nil
}

// buildSource constructs the config source and, for the file source, a SIGHUP
// reload hook (nil for the others). It cannot fail: the inline document was
// already parsed in bootParams, which is where a malformed one is rejected.
func (c *GatewayCmd) buildSource(kind sourceKind, boot bootParams, nc *nats.Conn, log *slog.Logger) (configsource.Source, func()) {
	switch kind {
	case sourceFile:
		f := configsource.NewFile(c.Config, c.ReloadInterval)
		return f, f.Reload
	case sourceInline:
		// The document is fixed for the pod's life, so it emits once and
		// never reloads.
		return configsource.Static(boot.inline), nil
	default:
		return &configsource.NATS{
			Conn:           nc,
			RequestSubject: c.ConfigSubject,
			EventSubject:   c.ConfigEventsSubject,
			Refetch:        c.ConfigRefetch,
			Logger:         log,
		}, nil
	}
}

// buildBackend resolves a server definition into a Backend (stdio/http,
// modern or legacy-bridged), injecting resolver-supplied credentials over the
// config's static env/headers.
func buildBackend(key backend.Key, s config.Server, resolver *cred.CachedResolver, log *slog.Logger) (backend.Backend, error) {
	blog := log.With("server", key.Server, "tenant", key.Tenant)

	credUser := key.CredSet
	if credUser == "" {
		credUser = wire.UserUnattributed
	}
	var creds *cred.Credentials
	if resolver != nil {
		// Steady-state this is a cache hit: the proxy resolved the same key
		// to build the pool key just before spawning us.
		c, gen, err := resolver.ResolveGen(context.Background(), key.Tenant, credUser, key.Server)
		if err != nil {
			return nil, fmt.Errorf("resolving credentials for %s/%s: %w", key.Tenant, key.Server, err)
		}
		if gen != key.CredVersion {
			// The credentials rotated between the proxy's resolve and ours:
			// building now would file generation-N+1 material under an
			// N-stamped key, aliasing the pool. Fail the spawn — the caller
			// gets a retryable error and the re-issue keys the new
			// generation.
			return nil, fmt.Errorf("credentials for %s/%s rotated during spawn (generation %d != %d), re-issue",
				key.Tenant, key.Server, gen, key.CredVersion)
		}
		creds = c
	}

	modern := s.Protocol == mcpspec.ProtocolVersion
	var inner backend.Backend
	if s.Transport == "http" {
		hb := &backend.HTTPBackend{URL: s.URL, Headers: s.Headers, Legacy: !modern, Logger: blog}
		if resolver != nil {
			hb.TokenSource = &credTokenSource{
				resolver: resolver,
				tenant:   key.Tenant, user: credUser, server: key.Server,
			}
		}
		inner = hb
	} else {
		env := s.Env
		if creds != nil && len(creds.Env) > 0 {
			// Merge, not replace: config env keeps non-secrets (AWS_REGION),
			// resolved credentials override.
			env = make(map[string]string, len(s.Env)+len(creds.Env))
			maps.Copy(env, s.Env)
			maps.Copy(env, creds.Env)
		}
		inner = &backend.StdioBackend{Command: s.Command, Args: s.Args, Env: env, Logger: blog}
	}
	var out backend.Backend = inner
	if !modern {
		// Default: 2025-11-25, because that is what exists in the wild.
		out = &legacy.Backend{
			Inner:         inner,
			DiscoverTTLMs: s.DiscoverTTLMs,
			CacheScope:    s.CacheScope,
			Logger:        blog,
		}
	}
	// Clamp the backend's life to the credentials' expiry — but only when the
	// reply actually carried material. The clamp exists to bound how long a
	// leaked credential stays usable; a reply with an expiry and no env or
	// headers has nothing to leak, and honoring it anyway would kill and
	// respawn the child on the responder's cadence, paying an npx/uvx cold
	// start every cycle for a server that has no credentials at all.
	if creds != nil && !creds.ExpiresAt.IsZero() && (len(creds.Env) > 0 || len(creds.Headers) > 0) {
		out = &expiringBackend{Backend: out, expiresAt: creds.ExpiresAt}
	}
	return out, nil
}

// expiringBackend tells the pool (backend.Expiring) to clamp the entry's
// lifetime to the injected credentials' expiry.
type expiringBackend struct {
	backend.Backend
	expiresAt time.Time
}

func (b *expiringBackend) CredExpiresAt() time.Time { return b.expiresAt }

// credTokenSource adapts a CachedResolver to backend.TokenSource for one
// pooled connection's identity, so each HTTP request carries the current
// bearer and a 401 forces a refetch.
type credTokenSource struct {
	resolver             *cred.CachedResolver
	tenant, user, server string
}

func (t *credTokenSource) Headers(ctx context.Context) (map[string]string, error) {
	c, _, err := t.resolver.ResolveGen(ctx, t.tenant, t.user, t.server)
	if err != nil {
		return nil, err
	}
	return c.Headers, nil
}

func (t *credTokenSource) Invalidate() { t.resolver.Invalidate(t.tenant, t.user, t.server) }

// credRegistry caches one CachedResolver per server. The resolver (and its
// credential cache) survives across requests; a changed auth definition on
// config reload rebuilds it, exactly as the pool evicts changed servers.
type credRegistry struct {
	nc  *nats.Conn
	log *slog.Logger
	// prefix is the boot subject prefix the WIRE serves on. The `nats` cred
	// mode's default subject derives from it, so one configured prefix governs
	// both halves of the control-plane contract: a deployment that moves its
	// wire also moves its cred requests, instead of publishing them to a
	// hardcoded subject its own JWT denies.
	prefix string
	// live returns the server's CURRENT definition, or false if it is no
	// longer configured. Consulted before inserting a new entry: a request
	// holding a pre-reload config snapshot must not resurrect a
	// just-pruned server's resolver (whose cache would then retain
	// credential material until the next reload — possibly forever). Nil
	// disables the check (tests).
	live func(name string) (config.Server, bool)

	mu      sync.Mutex
	entries map[string]*credRegEntry
}

type credRegEntry struct {
	// authPtr is the *config.Auth this entry was built from. Config reloads
	// swap whole *Config values and never mutate Auth in place, so pointer
	// equality answers the per-request "did the auth change?" check without
	// marshaling; authJSON is the reload-time fallback that keeps the
	// resolver (and its credential cache) across a benign reload.
	authPtr  *config.Auth
	authJSON string
	resolver *cred.CachedResolver
	perUser  bool
}

// prune drops registry entries for servers no longer in the config, so a
// deleted server's resolver — and the credential material in its cache —
// doesn't outlive the server. Called after every successful config apply.
func (r *credRegistry) prune(servers map[string]config.Server) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name := range r.entries {
		if _, ok := servers[name]; !ok {
			delete(r.entries, name)
		}
	}
}

// lookup returns the server's resolver (nil for static credentials) and
// whether its credentials are per-user.
func (r *credRegistry) lookup(name string, s config.Server) (*cred.CachedResolver, bool) {
	if !s.Auth.Dynamic() {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[string]*credRegEntry)
	}
	e := r.entries[name]
	if e != nil && e.authPtr == s.Auth {
		return e.resolver, e.perUser // steady state: no marshal on the request path
	}
	authJSON, err := json.Marshal(s.Auth)
	if err != nil {
		r.log.Error("marshaling auth config", "server", name, "err", err)
		return nil, false
	}
	if e != nil && e.authJSON == string(authJSON) {
		// A reload swapped the pointer but not the config: keep the
		// resolver and its cache, re-anchor the fast path.
		e.authPtr = s.Auth
		return e.resolver, e.perUser
	}
	if r.live != nil {
		// Insert path only: build from the LIVE definition, so a caller
		// holding a stale config snapshot can neither resurrect a removed
		// server nor install a resolver for a superseded auth config.
		ls, ok := r.live(name)
		if !ok || !ls.Auth.Dynamic() {
			return nil, false
		}
		s = ls
		if authJSON, err = json.Marshal(s.Auth); err != nil {
			r.log.Error("marshaling auth config", "server", name, "err", err)
			return nil, false
		}
	}
	e = &credRegEntry{
		authPtr:  s.Auth,
		authJSON: string(authJSON),
		resolver: cred.Cached(buildResolver(s.Auth, r.nc, r.prefix), 0),
		perUser:  s.Auth.PerUser(),
	}
	r.entries[name] = e
	return e.resolver, e.perUser
}

// buildResolver maps a validated auth config to its built-in resolver. prefix
// is the wire's subject prefix ("" = wire.DefaultPrefix); only the `nats` mode
// reads it, to default its cred subject.
func buildResolver(a *config.Auth, nc *nats.Conn, prefix string) cred.Resolver {
	switch a.Mode {
	case config.AuthExec:
		return &cred.Exec{Command: a.Command, Args: a.Args, Env: a.Env}
	case config.AuthFile:
		ttl, _ := time.ParseDuration(a.TTL) // validated at config load
		return &cred.File{Path: a.Path, TTL: ttl}
	case config.AuthOAuthClientCredentials:
		return &cred.OAuthClientCredentials{
			TokenURL: a.TokenURL, ClientID: a.ClientID, ClientSecret: a.ClientSecret,
			Scope: a.Scope, Audience: a.Audience,
		}
	case config.AuthOAuthTokenExchange:
		return &cred.OAuthTokenExchange{
			TokenURL: a.TokenURL, ClientID: a.ClientID, ClientSecret: a.ClientSecret,
			Scope: a.Scope, Audience: a.Audience,
			SubjectTokenFile: a.SubjectTokenFile, SubjectTokenType: a.SubjectTokenType,
		}
	case config.AuthOAuthRefresh:
		return &cred.OAuthRefresh{
			TokenURL: a.TokenURL, ClientID: a.ClientID, ClientSecret: a.ClientSecret,
			Scope: a.Scope, Store: &cred.FileTokenStore{Path: a.RefreshTokenFile},
		}
	case config.AuthNATS:
		subject := a.Subject
		if subject == "" {
			// Derive from the configured wire prefix, not a hardcoded one: the
			// control plane grants cred-publish from ITS prefix, so a gateway
			// asking on "mcp.v1.cred.…" under a custom --subject-prefix would
			// be denied by its own JWT — and only at request time, with the pod
			// still reporting healthy.
			if prefix == "" {
				prefix = wire.DefaultPrefix
			}
			subject = prefix + ".cred"
		}
		return &cred.NATS{Conn: nc, SubjectPrefix: subject}
	}
	// Unreachable after config validation; a nil resolver would panic in
	// Cached, so fail closed with an erroring resolver instead.
	mode := a.Mode
	return cred.ResolveFunc(func(context.Context, string, string, string) (*cred.Credentials, error) {
		return nil, cred.Terminal(fmt.Errorf("cred: unknown auth mode %q", mode))
	})
}

// normalizeVersion maps build versions like "dev" onto a semver micro will
// accept.
func normalizeVersion(v string) string {
	if len(v) > 0 && v[0] >= '0' && v[0] <= '9' {
		return v
	}
	return "0.0.0"
}
