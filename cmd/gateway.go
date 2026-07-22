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

// bootParams are the boot-time settings that do NOT hot-reload: the gateway's
// own NATS connection and the wire's subject prefix / queue group / scoping.
type bootParams struct {
	url           string
	credsFile     string
	prefix        string
	queueGroup    string
	tenant        string
	user          string
	pool          config.Pool
	claimCheck    bool
	claimMaxAge   time.Duration
	claimMaxBytes int64
}

func runGateway(c *GatewayCmd, g *Globals, version string) error {
	log := NewLogger(g)

	if (c.Config == "") == (c.ConfigSubject == "") {
		return fmt.Errorf("exactly one of --config or --config-subject is required")
	}

	// Resolve boot params. File mode reads them from the file (one time);
	// fetch mode takes them from flags, since we must connect before we can
	// fetch the config.
	boot, err := c.bootParams(log)
	if err != nil {
		return err
	}

	opts := []nats.Option{nats.Name("natsmcp-gateway"), nats.MaxReconnects(-1)}
	if boot.credsFile != "" {
		opts = append(opts, nats.UserCredentials(boot.credsFile))
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

	// The reconciler holds the live config; the pool factory resolves each
	// server's definition through it, so an evicted server always respawns
	// from the newest config. The credential registry lives beside it so the
	// resolvers' caches survive requests but not an auth-config change.
	var rec *reconcile.Reconciler
	registry := &credRegistry{nc: nc, log: log}
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
	pool := backend.NewPool(backend.PoolConfig{
		MaxConcurrent:     boot.pool.MaxConcurrent,
		MaxProcsPerTenant: boot.pool.MaxProcsPerTenant,
	}, factory, log)
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

	// Build the config source.
	source, onSighup := c.buildSource(nc, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signals: SIGHUP reloads the file source; INT/TERM drain.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for s := range sig {
			if s == syscall.SIGHUP {
				if onSighup != nil {
					log.Info("SIGHUP: reloading config")
					onSighup()
				}
				continue
			}
			log.Info("draining", "signal", s.String())
			cancel()
			return
		}
	}()

	log.Info("gateway starting", "nats", boot.url, "source", c.sourceKind())

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
func (c *GatewayCmd) bootParams(_ *slog.Logger) (bootParams, error) {
	if c.Config != "" {
		cfg, err := config.Load(c.Config)
		if err != nil {
			return bootParams{}, err
		}
		url := cfg.NATS.URL
		if url == "" {
			url = nats.DefaultURL
		}
		boot := bootParams{
			url:        url,
			credsFile:  cfg.NATS.CredsFile,
			prefix:     cfg.NATS.SubjectPrefix,
			queueGroup: cfg.NATS.QueueGroup,
			tenant:     cfg.NATS.Tenant,
			user:       cfg.NATS.User,
			pool:       cfg.Pool,
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
	return bootParams{
		url:           c.NatsURL,
		credsFile:     c.NatsCreds,
		prefix:        c.SubjectPrefix,
		queueGroup:    c.QueueGroup,
		tenant:        c.ScopeTenant,
		user:          c.ScopeUser,
		claimCheck:    c.ClaimCheck,
		claimMaxAge:   c.ClaimMaxAge,
		claimMaxBytes: c.ClaimMaxBytes,
	}, nil
}

// buildSource constructs the config source and, for the file source, a SIGHUP
// reload hook.
func (c *GatewayCmd) buildSource(nc *nats.Conn, log *slog.Logger) (configsource.Source, func()) {
	if c.Config != "" {
		f := configsource.NewFile(c.Config, c.ReloadInterval)
		return f, f.Reload
	}
	return &configsource.NATS{
		Conn:           nc,
		RequestSubject: c.ConfigSubject,
		EventSubject:   c.ConfigEventsSubject,
		Refetch:        c.ConfigRefetch,
		Logger:         log,
	}, nil
}

func (c *GatewayCmd) sourceKind() string {
	if c.Config != "" {
		return "file:" + c.Config
	}
	return "nats:" + c.ConfigSubject
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
		out = &legacy.Backend{Inner: inner, DiscoverTTLMs: s.DiscoverTTLMs, Logger: blog}
	}
	if creds != nil && !creds.ExpiresAt.IsZero() {
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
	e = &credRegEntry{
		authPtr:  s.Auth,
		authJSON: string(authJSON),
		resolver: cred.Cached(buildResolver(s.Auth, r.nc), 0),
		perUser:  s.Auth.PerUser(),
	}
	r.entries[name] = e
	return e.resolver, e.perUser
}

// buildResolver maps a validated auth config to its built-in resolver.
func buildResolver(a *config.Auth, nc *nats.Conn) cred.Resolver {
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
		return &cred.NATS{Conn: nc, SubjectPrefix: a.Subject}
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
