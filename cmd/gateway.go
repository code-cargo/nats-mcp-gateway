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
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"regexp"
	"strings"
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
	// ignoredFlags are the file-source settings supplied by an env var the shim
	// and call commands share, which this source drops instead of refusing.
	// Dropping them silently is what left an unfenced inbox with nothing in the
	// log to explain it, so runGateway says so at boot.
	ignoredFlags []string
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
	log, err := NewLogger(g)
	if err != nil {
		return err
	}

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
	for _, ignored := range boot.ignoredFlags {
		log.Warn("file source ignores a setting supplied by a shared environment variable", "detail", ignored)
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
		return connectFailure(boot.url, err)
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
		Logger:     log,
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

	log.Info("gateway starting", "nats", redactNATSURL(boot.url), "source", kind.describe(c))

	// Run the config loop. It returns when ctx is cancelled (signal) or on a
	// fatal initial-config error.
	var perUserWarned string
	runErr := configsource.Run(ctx, log, source, func(cfg *config.Config) error {
		_, err := rec.Apply(cfg)
		if err == nil {
			registry.prune(cfg.Servers)
			perUserWarned = warnPerUserGrain(log, cfg, perUserWarned)
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

// warnPerUserGrain names the servers whose credentials are resolved per caller.
//
// That grain is the setting that turns an unattributed request into a -32014
// refusal, and nothing else in the gateway's output says which servers carry
// it: a deployment without per-user NATS auth sends the documented "_" token
// on every request, so the first symptom is every call to those servers
// failing at once, with a remedy the deployment may have no way to reach. It
// is a warning rather than an error because the fail-closed refusal is
// deliberate — this is the line an operator greps for after the first -32014.
//
// last is the set already reported, returned so the caller can suppress the
// repeat: the fetch source re-applies on every poll tick, and a warning that
// re-fires forever is one operators filter out.
func warnPerUserGrain(log *slog.Logger, cfg *config.Config, last string) string {
	if cfg == nil {
		return last
	}
	var names []string
	for _, name := range cfg.ServerNames() {
		if cfg.Servers[name].Auth.PerUser() {
			names = append(names, name)
		}
	}
	cur := strings.Join(names, ",")
	if cur == "" || cur == last {
		return cur
	}
	log.Warn("servers resolve credentials per caller; requests carrying the unattributed user token are refused with -32014",
		"servers", cur,
		"needs", "per-user NATS auth, so each caller publishes under its own {user} token",
		"opt_out", `"perUser": false (exec, nats) or a path without {user} (file, oauth-*)`)
	return cur
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
		ignored, err := c.checkFileSourceFlags(boot)
		if err != nil {
			return bootParams{}, err
		}
		boot.ignoredFlags = ignored
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
	// wire.Serve rejects this too — this is what names the flag, and what fails
	// before we open a connection. Nothing below the wire objects: micro
	// accepts a "*" in an endpoint subject and NATS binds it, so "mcp.*" would
	// subscribe this instance to every prefix in the account.
	if c.SubjectPrefix != "" {
		if err := wire.ValidateSubjectPrefix(c.SubjectPrefix); err != nil {
			return bootParams{}, fmt.Errorf("--subject-prefix: %w", err)
		}
	}
	if c.QueueGroup != "" {
		if err := wire.ValidateQueueGroup(c.QueueGroup); err != nil {
			return bootParams{}, fmt.Errorf("--queue-group: %w", err)
		}
	}
	// Validated here rather than left to nats.Connect: the option's own check
	// misses spaces, and its "invalid custom prefix" names neither the setting
	// nor the value.
	if c.InboxPrefix != "" {
		if err := wire.ValidateInboxPrefix(c.InboxPrefix); err != nil {
			return bootParams{}, fmt.Errorf("--inbox-prefix: %w", err)
		}
	}
	// The same rejection config.validate() makes for the document's durations,
	// on the path that is the fetch source's ONLY route to pool sizing. Every
	// consumer substitutes its default for anything <= 0, so "-30m" is not a
	// shorter TTL — it is the default, silently, with nothing logged for the
	// operator who asked for something else.
	for _, d := range []struct {
		flag string
		val  time.Duration
	}{
		{"--pool-idle-ttl", c.PoolIdleTTL},
		{"--pool-max-lifetime", c.PoolMaxLifetime},
		{"--claim-max-age", c.ClaimMaxAge},
	} {
		if d.val < 0 {
			return bootParams{}, fmt.Errorf("%s must not be negative (got %s)", d.flag, d.val)
		}
	}
	if c.ClaimMaxBytes < 0 {
		return bootParams{}, fmt.Errorf("--claim-max-bytes must not be negative (got %d)", c.ClaimMaxBytes)
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
		// loudly instead of leaving the gateway serving nothing.
		cfg, err := config.ParseInline([]byte(c.ConfigJSON))
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

// Defaults for the fetch/inline-only flags, mirrored from cli.go's `default:`
// tags. They are what the check below rests on, because kong cannot say whether
// a value came from the operator: Context.Reset() parses the tag through
// Value.Parse, which marks the value Set either way, so on a bare
// `gateway --config` every defaulted flag already reports Set. Only the VALUE
// separates them. TestFileSourceGuardAgainstKongDefaults pins these to the
// tags, so a changed default fails a test instead of every file-source boot —
// which is why that test also parses a document that ENABLES claim-check. The
// sizing comparison is skipped when neither side asks for claim-check, so a
// defaults-only document would leave wire's two claim limits unpinned.
//
// Each of these names the value the process substitutes for the unset setting,
// not a second copy of it: defaulting belongs to the package that acts on the
// setting (nats.go, wire, backend), so every config source inherits it.
const (
	defaultNatsURL       = nats.DefaultURL
	defaultSubjectPrefix = wire.DefaultPrefix
)

// checkFileSourceFlags rejects a fetch/inline-only setting that the file source
// is about to drop, unless the document already carries that same value.
//
// This is the inline `nats` block rejection arriving from the other direction.
// The per-user pod recipe injects scope as NATSMCP_SCOPE_TENANT and
// NATSMCP_SCOPE_USER, and env in a pod spec never reads the --help string that
// says "(fetch + inline sources)": mount a file whose nats block omits the
// scope and the gateway boots UNSCOPED — it binds {prefix}.req.*.*.{server}.>
// and serves every tenant using this pod's per-user credentials. A dropped
// NATSMCP_NATS_URL falls back to loopback and a dropped --inbox-prefix leaves
// the identity fencing off, both without a word in the log. A deployment that
// asks to be scoped can never silently serve everyone.
//
// Only a CONFLICT fails. A value the document already carries drops nothing,
// and NATSMCP_NATS_URL, NATSMCP_SUBJECT_PREFIX and NATSMCP_INBOX_PREFIX are
// shared with the shim and call commands, so one exported value per deployment
// is ordinary; refusing to boot over it would break working fleets to correct a
// no-op — the trade config.validate() already declined to make for
// discoverTtlMs.
//
// --config-events-subject and --config-refetch are deliberately absent: they
// steer the fetch source's own polling, the job --reload-interval does for this
// source, and nothing about the connection, the scope or the served subjects
// rides on them.
func (c *GatewayCmd) checkFileSourceFlags(boot bootParams) ([]string, error) {
	var dropped, ignored []string
	// flag and env are how the operator supplied the value; field is the
	// document key that governs instead. inDoc is what that key resolved to,
	// which is the whole point of the message: it names the value in force.
	check := func(flag, env, field string, conflict bool, got, inDoc any) {
		if !conflict {
			return
		}
		dropped = append(dropped, fmt.Sprintf("--%s=%s (%s) but %s=%s",
			flag, settingValue(got), env, field, settingValue(inDoc)))
	}
	// checkShared is check for a setting whose env var the shim and call
	// commands read too. One value exported once for a host is the ordinary
	// deployment — the README's own layout runs a shim beside a file-source
	// gateway — so a value that arrived that way is reported and dropped
	// rather than refused: refusing breaks a working fleet to correct a
	// setting this source was never going to read, which is the trade the
	// comment above already declines to make. An explicit flag still fails,
	// because that one was aimed at this process.
	checkShared := func(flag, env, field string, conflict bool, got, inDoc string) {
		if !conflict {
			return
		}
		if suppliedByEnv(env, got) {
			ignored = append(ignored, fmt.Sprintf("--%s=%s (%s) is ignored by the file source; %s=%s is in force",
				flag, settingValue(got), env, field, settingValue(inDoc)))
			return
		}
		check(flag, env, field, conflict, got, inDoc)
	}
	checkShared("nats-url", "NATSMCP_NATS_URL", "nats.url",
		c.NatsURL != "" && c.NatsURL != defaultNatsURL && c.NatsURL != boot.url, c.NatsURL, boot.url)
	// Either name may have supplied this: the pair are aliases of each other on
	// every subcommand, so the message has to name the one the operator
	// actually exported. Naming the other sends them to unset a variable they
	// never set, which does not clear the error.
	checkShared("nats-creds", credsEnvName(c.NatsCreds), "nats.credsFile",
		c.NatsCreds != "" && c.NatsCreds != boot.credsFile, c.NatsCreds, boot.credsFile)
	// Against the prefix the wire will actually bind, for the same reason the
	// queue group and the pool are: wire.Serve owns this default, so a document
	// that omits the field is not asking for the empty prefix.
	effPrefix := boot.prefix
	if effPrefix == "" {
		effPrefix = wire.DefaultPrefix
	}
	checkShared("subject-prefix", "NATSMCP_SUBJECT_PREFIX", "nats.subjectPrefix",
		c.SubjectPrefix != "" && c.SubjectPrefix != defaultSubjectPrefix && c.SubjectPrefix != effPrefix,
		c.SubjectPrefix, effPrefix)
	checkShared("inbox-prefix", "NATSMCP_INBOX_PREFIX", "nats.inboxPrefix",
		c.InboxPrefix != "" && c.InboxPrefix != boot.inboxPrefix, c.InboxPrefix, boot.inboxPrefix)
	// Compared against the group the wire will actually join, not against the
	// document's silence: wire.DefaultQueueGroup owns this default precisely so
	// every source gets it, so pinning a flag to the value it already computes
	// drops nothing and must not refuse the boot.
	effQueue := boot.queueGroup
	if effQueue == "" {
		effQueue = wire.DefaultQueueGroup(boot.tenant, boot.user)
	}
	check("queue-group", "NATSMCP_QUEUE_GROUP", "nats.queueGroup",
		c.QueueGroup != "" && c.QueueGroup != effQueue, c.QueueGroup, effQueue)
	check("scope-tenant", "NATSMCP_SCOPE_TENANT", "nats.tenant",
		c.ScopeTenant != "" && c.ScopeTenant != boot.tenant, c.ScopeTenant, boot.tenant)
	check("scope-user", "NATSMCP_SCOPE_USER", "nats.user",
		c.ScopeUser != "" && c.ScopeUser != boot.user, c.ScopeUser, boot.user)
	// Same for the pool: PoolConfig.fill substitutes a default for anything
	// <= 0, so a document that omits these is not asking for zero — it is
	// asking for 32/16/5m/1h, which is what cli.go's own help text advertises.
	// A chart that materializes those numbers into env would otherwise be
	// refused for agreeing with the gateway.
	effPool := boot.pool.Filled()
	check("pool-max-concurrent", "NATSMCP_POOL_MAX_CONCURRENT", "pool.maxConcurrent",
		c.PoolMaxConcurrent > 0 && c.PoolMaxConcurrent != effPool.MaxConcurrent,
		c.PoolMaxConcurrent, effPool.MaxConcurrent)
	check("pool-max-procs-per-tenant", "NATSMCP_POOL_MAX_PROCS_PER_TENANT", "pool.maxProcsPerTenant",
		c.PoolMaxProcsPerTenant > 0 && c.PoolMaxProcsPerTenant != effPool.MaxProcsPerTenant,
		c.PoolMaxProcsPerTenant, effPool.MaxProcsPerTenant)
	check("pool-idle-ttl", "NATSMCP_POOL_IDLE_TTL", "pool.idleTtl",
		c.PoolIdleTTL > 0 && c.PoolIdleTTL != effPool.IdleTTL, c.PoolIdleTTL, effPool.IdleTTL)
	check("pool-max-lifetime", "NATSMCP_POOL_MAX_LIFETIME", "pool.maxLifetime",
		c.PoolMaxLifetime > 0 && c.PoolMaxLifetime != effPool.MaxLifetime, c.PoolMaxLifetime, effPool.MaxLifetime)
	check("claim-check", "NATSMCP_CLAIM_CHECK", "claimCheck",
		c.ClaimCheck && !boot.claimCheck, c.ClaimCheck, boot.claimCheck)
	// The sizing flags are read only when claim-check is on (see runGateway),
	// so with neither side asking for it they are inert and dropping them
	// costs nothing. When the FLAG asks for it the request is being dropped
	// whole, and naming only --claim-check would under-report what the
	// operator asked for and is not getting.
	if boot.claimCheck || c.ClaimCheck {
		// Against the limits the bucket will actually get, the same way the
		// pool is: wire.FilledClaimLimits substitutes a default for anything
		// <= 0, so a document enabling claimCheck without sizing it is asking
		// for 5m/1GiB, not for zero. When the DOCUMENT does not enable
		// claim-check the sizing is compared and reported raw instead: there
		// is no bucket, so nothing is in force to name, and the zero pairs
		// with the claimCheck=false line already reporting that the whole
		// feature is being dropped.
		inDocAge, inDocBytes := boot.claimMaxAge, boot.claimMaxBytes
		if boot.claimCheck {
			inDocAge, inDocBytes = wire.FilledClaimLimits(boot.claimMaxAge, boot.claimMaxBytes)
		}
		check("claim-max-age", "NATSMCP_CLAIM_MAX_AGE", "claimCheck.maxAge",
			c.ClaimMaxAge > 0 && c.ClaimMaxAge != wire.DefaultClaimMaxAge && c.ClaimMaxAge != inDocAge,
			c.ClaimMaxAge, inDocAge)
		check("claim-max-bytes", "NATSMCP_CLAIM_MAX_BYTES", "claimCheck.maxBytes",
			c.ClaimMaxBytes > 0 && c.ClaimMaxBytes != wire.DefaultClaimMaxBytes && c.ClaimMaxBytes != inDocBytes,
			c.ClaimMaxBytes, inDocBytes)
	}

	if len(dropped) == 0 {
		return ignored, nil
	}
	return ignored, fmt.Errorf("config: %s: %s — the file source takes its connection, scope, pool and claim-check settings from this document; set them there, or drop the flag and its env var (only --config-subject and --config-json read them)",
		c.Config, strings.Join(dropped, "; "))
}

// suppliedByEnv reports whether name is what gave this setting its value, so a
// variable exported once for a whole host can be told apart from a flag aimed
// at this process. Kong has already collapsed the two into one field by the
// time anything here runs, and an operator who passes the flag AND exports the
// same value gets the gentler of the two readings, which is the right way for
// this to be wrong.
func suppliedByEnv(name, got string) bool {
	return got != "" && os.Getenv(name) == got
}

// credsEnvName is the credentials variable that actually supplied the value,
// preferring the gateway's own documented name when both are set to it.
func credsEnvName(got string) string {
	for _, name := range gatewayCredsEnv {
		if suppliedByEnv(name, got) {
			return name
		}
	}
	return gatewayCredsEnv[0]
}

// settingValue renders a setting for that error, quoting strings so an empty
// document field reads as "" rather than as nothing at all.
func settingValue(v any) string {
	if s, ok := v.(string); ok {
		// Redacted because two of the settings rendered here are NATS URLs and
		// a NATS URL carries its credential inline — the README's own canonical
		// form is nats://gw:pw@nats:4222. This text is built on both paths: the
		// fatal one, which a person reads once, and the ignored one, which goes
		// to the log. Non-URL values are unaffected; redactNATSURL only rewrites
		// an authority that has userinfo in it.
		return fmt.Sprintf("%q", redactNATSURL(s))
	}
	return fmt.Sprint(v)
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
	if credUser == "" || credUser == wire.UserUnattributed {
		// The pool key was minted without a per-user identity, and mapping it
		// to the unattributed placeholder is only right when the server does
		// not ask for one. The proxy refuses that combination on the request
		// path, but this factory runs later and re-reads the config, so a
		// reload flipping a server shared -> per-user mid-request arrives here
		// with a key the new mode cannot satisfy.
		//
		// Both spellings of "no identity" are caught. Only the empty CredSet
		// is reachable today (the proxy leaves it unset for a shared server),
		// but the placeholder means the same thing everywhere else in the
		// gateway, and a credential-identity boundary should not rest on which
		// of the two an unrelated caller happened to construct.
		//
		// Checked against the grain rather than left to the generation
		// comparison below. That comparison does reject it today — a
		// resolver-less key carries CredVersion 0 and globalGen starts at 1 —
		// but that is an accident of numbering. Refusing here also means the
		// resolver is never asked for "_", so an exec helper with side effects
		// does not run under an identity nothing will accept.
		if s.Auth.PerUser() {
			return nil, fmt.Errorf(
				"server %q resolves credentials per user and this backend was keyed without one; "+
					"the request predates a reload that made it per-user, and re-issuing will use the new mode: %w",
				key.Server, cred.ErrIdentityRequired,
			)
		}
		credUser = wire.UserUnattributed
	}
	var creds *cred.Credentials
	if resolver != nil {
		// Steady-state this is a cache hit: the proxy resolved the same key
		// to build the pool key just before spawning us.
		c, gen, err := resolver.ResolveGen(context.Background(), key.Tenant, credUser, key.Server)
		if err != nil {
			// The factory's error is the pool's error, which the proxy hands
			// the caller: the same detail suppression the request path applies
			// is owed here, and through the same function so the two cannot
			// drift.
			//
			// Returned as proxy.CredentialError so it also arrives under the
			// same wire code. A bare error from here reaches the caller as
			// -32010, "the stream broke, re-issue" — and CallerMessage's "do
			// not retry" would then sit inside the one code that promises
			// retrying helps, so a client switching on the code (the reason
			// codes exist) retry-loops a permanent refusal. Reachable whenever
			// the proxy's resolve hits the cache and this one misses it: a TTL
			// boundary, or a concurrent 401 Invalidate.
			ref := cred.FailureRef()
			blog.Warn("credential resolution failed while spawning backend", "err", err, "ref", ref)
			return nil, &proxy.CredentialError{
				Message: fmt.Sprintf("%s/%s: %s", key.Tenant, key.Server, cred.CallerMessage(err, ref)),
			}
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
				resolver: resolver, log: blog,
				tenant: key.Tenant, user: credUser, server: key.Server,
			}
		}
		inner = hb
	} else {
		env := s.Env
		if creds != nil && len(creds.Env) > 0 {
			// Merge, not replace: config env keeps non-secrets (AWS_REGION),
			// resolved credentials override — but only the ones a resolver is
			// entitled to set (see safeCredEnv).
			resolved := safeCredEnv(creds.Env, blog)
			env = make(map[string]string, len(s.Env)+len(resolved))
			maps.Copy(env, s.Env)
			maps.Copy(env, resolved)
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

// A credential resolver is trusted to supply CREDENTIALS, not code. Its reply's
// env is merged over the config's and handed to StdioBackend as the child's
// entire environment, so a variable the dynamic loader or a language runtime
// reads at startup would promote the resolver from "supplies credentials" to
// "runs code in the gateway pod, at the gateway's uid": for mode "nats" that is
// a compromised controller, for mode "file" whatever writes the mounted file —
// neither of which is the party that authored the config. (mode "exec" is
// already local execution and gains nothing here, but this merge point should
// not have to know the mode to be safe.)
//
// The config's own env block is deliberately NOT filtered. A document that
// names the command and its args already chooses what runs, so filtering its
// env would buy nothing and would break the operator who set
// NODE_OPTIONS=--max-old-space-size on a server they wrote themselves.
//
// What this covers is the loader plus the startup hooks of the runtimes a
// stdio backend is actually built on (node, python, shells, and the
// interpreters behind an npx/uvx server). It is deliberately not a list of
// every program's exec hook — a backend that shells out to git still honors
// GIT_SSH_COMMAND, and nothing here can know that it does. The boundary it
// draws is that a credential reply cannot redirect what the child itself loads
// and runs before its first line of MCP code.
var (
	credEnvBlockedPrefixes = []string{
		"LD_",        // glibc/musl loader: LD_PRELOAD, LD_AUDIT, LD_LIBRARY_PATH
		"DYLD_",      // macOS loader: DYLD_INSERT_LIBRARIES and friends
		"BASH_FUNC_", // exported shell functions, defined before the script runs
	}
	credEnvBlocked = map[string]struct{}{
		// Which binary "command" resolves to, and where every runtime looks
		// for the rc/config files it reads at startup. StdioBackend sets both
		// deliberately (the gateway's PATH, a private empty HOME), so these
		// are also the two the reply would be overriding rather than adding.
		"PATH": {}, "HOME": {},
		"GCONV_PATH": {}, // glibc loads charset modules from it
		// node
		"NODE_OPTIONS": {}, "NODE_PATH": {}, "NODE_REPL_EXTERNAL_MODULE": {},
		"NODE_EXTRA_CA_CERTS": {}, // TLS trust is a control, not a credential
		// python
		"PYTHONPATH": {}, "PYTHONHOME": {}, "PYTHONSTARTUP": {},
		"PYTHONEXECUTABLE": {}, "PYTHONBREAKPOINT": {},
		// sh/bash
		"BASH_ENV": {}, "ENV": {}, "SHELLOPTS": {},
		// perl, ruby
		"PERL5OPT": {}, "PERL5LIB": {}, "PERLLIB": {}, "PERL5DB": {},
		"RUBYOPT": {}, "RUBYLIB": {},
		// jvm, .net
		"JAVA_TOOL_OPTIONS": {}, "_JAVA_OPTIONS": {}, "JDK_JAVA_OPTIONS": {}, "CLASSPATH": {},
		"DOTNET_STARTUP_HOOKS": {}, "CORECLR_ENABLE_PROFILING": {},
		"CORECLR_PROFILER": {}, "CORECLR_PROFILER_PATH": {},
	}
)

// safeCredEnv is the resolver's env with those keys removed, one WARN per
// refusal. The key is named and the value never is: a refused value is exactly
// as secret as an accepted one, and the operator needs the key to find the
// resolver that sent it.
func safeCredEnv(env map[string]string, log *slog.Logger) map[string]string {
	out := make(map[string]string, len(env))
	for k, v := range env {
		if credEnvRefused(k) {
			log.Warn("dropping a credential-supplied environment variable: a resolver may not redirect the backend's code execution",
				"key", k)
			continue
		}
		out[k] = v
	}
	return out
}

func credEnvRefused(key string) bool {
	// A key holding "=" sets a variable other than the one it names, because
	// StdioBackend joins them as k+"="+v — which would make both this filter
	// and the warning above a statement about the wrong variable. A NUL fails
	// the spawn outright, and an empty key names nothing.
	if key == "" || strings.ContainsAny(key, "=\x00") {
		return true
	}
	if _, refused := credEnvBlocked[key]; refused {
		return true
	}
	for _, p := range credEnvBlockedPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
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
	log                  *slog.Logger
	tenant, user, server string
}

func (t *credTokenSource) Headers(ctx context.Context) (map[string]string, error) {
	c, _, err := t.resolver.ResolveGen(ctx, t.tenant, t.user, t.server)
	if err != nil {
		// The third resolve site, and the only one on the REQUEST path: an
		// HTTP backend answering 401 makes doWithAuthRetry invalidate and
		// resolve again, so this runs exactly when the credential source is
		// most likely to be failing and most likely to say why. What it says
		// travels — newRequest wraps it, roundTripOnce turns it into a -32603,
		// and the proxy forwards a backend's JSON-RPC error verbatim by
		// design. The same suppression the other two sites apply is owed here,
		// or the helper's stderr reaches the tenant user through the one path
		// that runs on every request.
		ref := cred.FailureRef()
		t.log.Warn("credential resolution failed for a backend request",
			"err", err, "ref", ref, "server", t.server)
		return nil, errors.New(cred.CallerMessage(err, ref))
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

// semVer is micro's own acceptance rule — semver.org's suggested regexp, the
// one micro applies to ServerConfig.Version. Duplicated rather than inferred
// because the fallback below is only safe if it catches EVERYTHING micro would
// reject: a looser local test (say, "starts with a digit") passes strings like
// "1.2" or "1.02.3" straight through to a rejection we exist to prevent.
var semVer = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)

// normalizeVersion maps a build version onto a semver micro will accept —
// micro rejects a service whose Version is not semver, and that rejection
// fails wire.Serve and with it the boot.
//
// Release tags are "vX.Y.Z" and the leading "v" is not part of semver, so it
// is stripped rather than sent to the fallback: mapping every release to 0.0.0
// left `nats micro list` reporting one version for the entire fleet, unable to
// show which instances had taken a rollout.
//
// Anything that is not semver after that — the Makefile's "develop", a build
// off an untagged tree, a release tag typed as "v1.2" — becomes 0.0.0, because
// a boot failure is a worse answer than an unhelpful version. That matters
// most for the typo'd tag: release.yml takes its tag as free text, so the
// mistake is not caught until the shipped binary fails to serve.
func normalizeVersion(v string) string {
	if s := strings.TrimPrefix(v, "v"); semVer.MatchString(s) {
		return s
	}
	return "0.0.0"
}
