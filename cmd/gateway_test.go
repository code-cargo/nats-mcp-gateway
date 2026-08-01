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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend/cred"
	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
	"github.com/code-cargo/nats-mcp-gateway/pkg/configsource"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/proxy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/reconcile"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakemcp.EnvFlag) == "1" {
		fakemcp.Main()
		return
	}
	os.Exit(m.Run())
}

// testLogger discards output (tests assert on behavior, not logs).
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// assembled is the gateway core wired exactly as runGateway does, minus the
// process-level signal handling — the shape an embedder would use, and what
// makes the fetch-mode reload path testable end to end.
type assembled struct {
	nc       *nats.Conn // gateway/responder connection (micro services live here)
	clientNC *nats.Conn // separate connection for wire client calls
	rec      *reconcile.Reconciler
}

func assemble(t *testing.T, nc *nats.Conn, url string) *assembled {
	t.Helper()
	return assembleScoped(t, nc, url, "", "")
}

// assembleScoped is assemble with the wire bound to a (tenant, user) scope —
// "" for either token leaves it unscoped/wildcarded, matching wire.Serve.
func assembleScoped(t *testing.T, nc *nats.Conn, url, tenant, user string) *assembled {
	t.Helper()
	// The wire client mutates/reads the connection's async error handler
	// (permission-violation fast-fail), which micro's per-service Add/Stop
	// also touches. In production the gateway and the shim are separate
	// processes with separate connections; mirror that here with a dedicated
	// client connection so the two don't race on one conn's callbacks.
	clientNC, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(clientNC.Close)

	a := &assembled{nc: nc, clientNC: clientNC}
	lookupServer := func(name string) (config.Server, bool) {
		cur := a.rec.Current()
		if cur == nil {
			return config.Server{}, false
		}
		s, ok := cur.Servers[name]
		return s, ok
	}
	registry := &credRegistry{nc: nc, log: testLogger(), live: lookupServer}
	pool := backend.NewPool(backend.PoolConfig{}, func(key backend.Key) (backend.Backend, error) {
		s, ok := lookupServer(key.Server)
		if !ok {
			return nil, fmt.Errorf("unknown server %q", key.Server)
		}
		resolver, _ := registry.lookup(key.Server, s)
		return buildBackend(key, s, resolver, testLogger())
	}, testLogger())
	t.Cleanup(pool.Shutdown)

	px := proxy.New(pool, testLogger())
	px.Creds = func(server string) (*cred.CachedResolver, bool) {
		s, ok := lookupServer(server)
		if !ok {
			return nil, false
		}
		return registry.lookup(server, s)
	}
	ws, err := wire.Serve(nc, wire.ServerConfig{KeepAlive: 50 * time.Millisecond, Tenant: tenant, User: user}, px.Handler())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})
	a.rec = reconcile.New(ws, pool, testLogger())
	return a
}

func (a *assembled) call(t *testing.T, serverName, tool string) wire.Frame {
	t.Helper()
	return a.callAs(t, "", serverName, tool, nil)
}

func mustPID(t *testing.T, f wire.Frame) float64 {
	t.Helper()
	require.Equal(t, wire.FrameEnd, f.Kind, "expected a result, got %+v", f)
	m, err := jsonrpc.Decode(f.Body)
	require.NoError(t, err)
	var r struct {
		PID float64 `json:"pid"`
	}
	require.NoError(t, json.Unmarshal(m.Result, &r))
	return r.PID
}

func fetchNATS(t *testing.T) (*nats.Conn, string) {
	t.Helper()
	return natstest.Run(t, nil)
}

// mutableConfig serves a config over request/reply and publishes change events.
type mutableConfig struct {
	nc  *nats.Conn
	mu  sync.Mutex
	raw []byte
}

func fakeServerJSON(name string, env map[string]string) string {
	e, _ := json.Marshal(env)
	return fmt.Sprintf(`%q:{"protocol":%q,"transport":"stdio","command":%q,"env":%s}`,
		name, mcpspec.ProtocolVersion, os.Args[0], e)
}

func (mc *mutableConfig) set(entries ...string) {
	body := `{"servers":{`
	for i, e := range entries {
		if i > 0 {
			body += ","
		}
		body += e
	}
	body += `}}`
	mc.mu.Lock()
	mc.raw = []byte(body)
	mc.mu.Unlock()
}

func (mc *mutableConfig) serve(t *testing.T) {
	t.Helper()
	sub, err := mc.nc.Subscribe("mcp.cfg.request", func(m *nats.Msg) {
		mc.mu.Lock()
		defer mc.mu.Unlock()
		_ = m.Respond(mc.raw)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

func (mc *mutableConfig) publishChange(t *testing.T) {
	t.Helper()
	require.NoError(t, mc.nc.Publish("mcp.cfg.changed", nil))
}

// env with the fakemcp re-exec flag plus optional extras.
func fakeEnv(extra map[string]string) map[string]string {
	m := map[string]string{fakemcp.EnvFlag: "1"}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func TestGatewayFetchModeHotReload(t *testing.T) {
	nc, url := fetchNATS(t)
	a := assemble(t, nc, url)

	mc := &mutableConfig{nc: nc}
	mc.set(fakeServerJSON("a", fakeEnv(nil)))
	mc.serve(t)

	src := &configsource.NATS{
		Conn:           nc,
		RequestSubject: "mcp.cfg.request",
		EventSubject:   "mcp.cfg.changed",
		Refetch:        time.Hour, // isolate the event-driven path
		Logger:         testLogger(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = configsource.Run(ctx, testLogger(), src, func(cfg *config.Config) error {
			_, err := a.rec.Apply(cfg)
			return err
		})
	}()

	// Wait for the initial config to be applied, then call server a.
	waitServing(t, a, "a")
	pidA := mustPID(t, a.call(t, "a", "echo"))

	// Reload: change a's env (forces respawn) and add b; drop nothing yet.
	mc.set(
		fakeServerJSON("a", fakeEnv(map[string]string{"EXTRA": "1"})),
		fakeServerJSON("b", fakeEnv(nil)),
	)
	mc.publishChange(t)

	// b appears without a restart, and a respawns with a new pid.
	waitServing(t, a, "b")
	assert.NotEqual(t, pidA, mustPID(t, a.call(t, "a", "echo")), "changed server must respawn")

	// Reload: drop a entirely.
	mc.set(fakeServerJSON("b", fakeEnv(nil)))
	mc.publishChange(t)

	require.Eventually(t, func() bool {
		f := a.call(t, "a", "echo")
		return f.Err != nil && f.Err.Code == wire.ErrCodeNoGateway
	}, 5*time.Second, 50*time.Millisecond, "dropped server must stop serving")

	// b is unaffected throughout.
	assert.Equal(t, wire.FrameEnd, a.call(t, "b", "echo").Kind)
}

func TestGatewayFetchModeBootRetry(t *testing.T) {
	nc, url := fetchNATS(t)
	a := assemble(t, nc, url)

	src := &configsource.NATS{
		Conn:           nc,
		RequestSubject: "mcp.cfg.request",
		RequestTimeout: 200 * time.Millisecond,
		BootTimeout:    10 * time.Second,
		Logger:         testLogger(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = configsource.Run(ctx, testLogger(), src, func(cfg *config.Config) error {
			_, err := a.rec.Apply(cfg)
			return err
		})
	}()

	// Responder is down; the gateway should be retrying, serving nothing.
	f := a.call(t, "a", "echo")
	require.NotNil(t, f.Err)

	// Bring the responder up; the gateway converges without a restart.
	time.Sleep(400 * time.Millisecond)
	mc := &mutableConfig{nc: nc}
	mc.set(fakeServerJSON("a", fakeEnv(nil)))
	mc.serve(t)

	waitServing(t, a, "a")
}

// Zero or many config sources is a boot error, caught before any connection is
// attempted (kong's xor guards the parsed CLI; this guards every entry point).
func TestGatewayRejectsWrongSourceCount(t *testing.T) {
	g := &Globals{LogLevel: "error", LogFormat: "text"}
	for _, c := range []*GatewayCmd{
		{},                                     // zero
		{Config: "/x", ConfigSubject: "s"},     // two
		{Config: "/x", ConfigJSON: "{}"},       // two
		{ConfigSubject: "s", ConfigJSON: "{}"}, // two
		{Config: "/x", ConfigSubject: "s", ConfigJSON: "{}"}, // three
	} {
		err := runGateway(c, g, "0.0.0")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one of")
	}
}

// selectSource is the single place that maps flags to a source, so everything
// downstream (boot params, the source itself, the boot log, the SIGHUP reply)
// agrees by construction. This pins the mapping and the reload semantics that
// hang off it — the boot log previously reported an inline gateway as a NATS
// fetch with an empty subject, because the kind was re-derived per call site.
func TestSelectSourceMapsFlagsToKind(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cmd        *GatewayCmd
		want       sourceKind
		wantDesc   string
		wantSighup bool
	}{
		{"file", &GatewayCmd{Config: "/etc/gw.json"}, sourceFile, "file:/etc/gw.json", true},
		{"inline", &GatewayCmd{ConfigJSON: `{"servers":{}}`}, sourceInline, "inline", false},
		{"fetch", &GatewayCmd{ConfigSubject: "mcp.v1.cfg.gateway"}, sourceFetch, "nats:mcp.v1.cfg.gateway", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, err := tc.cmd.selectSource()
			require.NoError(t, err)
			assert.Equal(t, tc.want, kind)
			assert.Equal(t, tc.wantDesc, kind.describe(tc.cmd), "boot log must name the real source")
			assert.Equal(t, tc.wantSighup, kind.reloadsOnSighup())
		})
	}
}

// Only the file source re-reads on SIGHUP. The fetch source reloads on its own
// (change events + --config-refetch), so it must never be described as
// non-reloading — an operator HUPing it to force a refresh would otherwise be
// told its config is frozen, which is the opposite of true.
func TestOnlyFileSourceReloadsOnSighup(t *testing.T) {
	assert.True(t, sourceFile.reloadsOnSighup())
	assert.False(t, sourceFetch.reloadsOnSighup())
	assert.False(t, sourceInline.reloadsOnSighup())
}

// Pool limits are boot-fixed, so the fetch source can only get them from flags
// — a fetched config arrives after the pool is built. An inline document
// overrides them, and omitting its pool block falls back to the flags.
func TestPoolLimitsFromFlagsAndInlineDocument(t *testing.T) {
	flags := &GatewayCmd{
		ConfigSubject:         "cfg",
		PoolMaxConcurrent:     7,
		PoolMaxProcsPerTenant: 64,
		PoolIdleTTL:           30 * time.Minute,
		PoolMaxLifetime:       24 * time.Hour,
	}
	boot, err := flags.bootParams(sourceFetch)
	require.NoError(t, err)
	assert.Equal(t, 7, boot.pool.MaxConcurrent)
	assert.Equal(t, 64, boot.pool.MaxProcsPerTenant)
	assert.Equal(t, 30*time.Minute, boot.pool.IdleTTL)
	assert.Equal(t, 24*time.Hour, boot.pool.MaxLifetime)

	// An inline pool block wins over the flags.
	doc := fmt.Sprintf(`{"pool":{"maxProcsPerTenant":9,"idleTtl":"1m"},"servers":{%s}}`,
		fakeServerJSON("a", fakeEnv(nil)))
	boot, err = (&GatewayCmd{ConfigJSON: doc, PoolMaxProcsPerTenant: 64}).bootParams(sourceInline)
	require.NoError(t, err)
	assert.Equal(t, 9, boot.pool.MaxProcsPerTenant)
	assert.Equal(t, time.Minute, boot.pool.IdleTTL)

	// No inline pool block: the flags still apply.
	doc = fmt.Sprintf(`{"servers":{%s}}`, fakeServerJSON("a", fakeEnv(nil)))
	boot, err = (&GatewayCmd{ConfigJSON: doc, PoolMaxProcsPerTenant: 64}).bootParams(sourceInline)
	require.NoError(t, err)
	assert.Equal(t, 64, boot.pool.MaxProcsPerTenant)
}

// inlineSource resolves boot params and the source the way runGateway does:
// the source is selected once, the inline document is parsed in bootParams,
// and buildSource reuses that parse.
func inlineSource(t *testing.T, c *GatewayCmd) (configsource.Source, func()) {
	t.Helper()
	kind, err := c.selectSource()
	require.NoError(t, err)
	require.Equal(t, sourceInline, kind)
	boot, err := c.bootParams(kind)
	require.NoError(t, err)
	src, reload := c.buildSource(kind, boot, nil, testLogger())
	return src, reload
}

// buildSource turns --config-json into a source that emits the parsed document
// once and never reloads.
func TestBuildSourceInline(t *testing.T) {
	inline := fmt.Sprintf(`{"servers":{%s}}`, fakeServerJSON("a", fakeEnv(nil)))
	src, reload := inlineSource(t, &GatewayCmd{ConfigJSON: inline})
	require.NotNil(t, src)
	assert.Nil(t, reload, "inline config never reloads")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := <-src.Watch(ctx)
	require.NoError(t, u.Err)
	assert.Equal(t, []string{"a"}, u.Config.ServerNames())
}

// A malformed inline document fails the boot rather than leaving the gateway
// up and serving nothing.
func TestBuildSourceInlineInvalidJSON(t *testing.T) {
	_, err := (&GatewayCmd{ConfigJSON: `{"servers":{"a":{"transport":"grpc"}}}`}).bootParams(sourceInline)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config-json")

	_, err = (&GatewayCmd{ConfigJSON: `not json`}).bootParams(sourceInline)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config-json")
}

// The inline document is plain-only: connection and SCOPE come from flags, so
// a `nats` block is rejected outright. Ignoring it silently would let a
// document that asked to be scoped to one tenant serve every tenant.
func TestBuildSourceInlineRejectsNatsBlock(t *testing.T) {
	for _, block := range []string{
		`"nats":{"tenant":"acme","user":"u1"}`,
		`"nats":{"url":"nats://elsewhere:4222"}`,
		`"nats":{"queueGroup":"other"}`,
	} {
		inline := fmt.Sprintf(`{%s,"servers":{%s}}`, block, fakeServerJSON("a", fakeEnv(nil)))
		_, err := (&GatewayCmd{ConfigJSON: inline}).bootParams(sourceInline)
		require.Error(t, err, block)
		assert.Contains(t, err.Error(), `"nats" block is not honored inline`)
	}
}

// pool and claimCheck carry no credentials, so the inline document does own
// them — a per-org deployment has to be able to raise maxProcsPerTenant.
func TestBuildSourceInlineHonorsPoolAndClaimCheck(t *testing.T) {
	inline := fmt.Sprintf(
		`{"pool":{"maxProcsPerTenant":64,"idleTtl":"30m","maxLifetime":"24h"},"claimCheck":{"maxAge":"9m","maxBytes":5},"servers":{%s}}`,
		fakeServerJSON("a", fakeEnv(nil)),
	)
	boot, err := (&GatewayCmd{ConfigJSON: inline}).bootParams(sourceInline)
	require.NoError(t, err)
	assert.Equal(t, 64, boot.pool.MaxProcsPerTenant)
	// Parsed on the way in — bootParams hands NewPool the pool's own shape.
	assert.Equal(t, 30*time.Minute, boot.pool.IdleTTL)
	assert.Equal(t, 24*time.Hour, boot.pool.MaxLifetime)
	assert.True(t, boot.claimCheck)
	assert.Equal(t, 9*time.Minute, boot.claimMaxAge)
	assert.Equal(t, int64(5), boot.claimMaxBytes)
}

// Scope flags are token-validated at boot. Without this the process connects,
// binds a queue group built from the bad token, and only dies when the first
// config arrives (an empty server set never reaches EndpointSubject).
func TestBootParamsRejectsUnsafeScopeFlags(t *testing.T) {
	_, err := (&GatewayCmd{ConfigSubject: "cfg", ScopeTenant: "bad tenant"}).bootParams(sourceFetch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not subject-token safe")

	_, err = (&GatewayCmd{ConfigSubject: "cfg", ScopeTenant: "acme", ScopeUser: "bad user"}).bootParams(sourceFetch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not subject-token safe")

	_, err = (&GatewayCmd{ConfigSubject: "cfg", ScopeUser: "u1"}).bootParams(sourceFetch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires --scope-tenant")
}

// The inbox prefix is validated at boot rather than left to nats.Connect: the
// option's own check misses spaces, and it reports neither the setting nor the
// value.
func TestBootParamsValidatesInboxPrefix(t *testing.T) {
	for _, bad := range []string{
		"_INBOX acme",   // space
		"_INBOX.*",      // wildcard token
		"_INBOX.>",      // wildcard token
		"_INBOX.acme.",  // trailing dot
		".acme",         // leading dot
		"_INBOX..acme",  // doubled dot
		"_INBOX.acme/1", // not token-safe
	} {
		_, err := (&GatewayCmd{ConfigSubject: "cfg", InboxPrefix: bad}).bootParams(sourceFetch)
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "--inbox-prefix", bad)
	}

	boot, err := (&GatewayCmd{ConfigSubject: "cfg", InboxPrefix: "_INBOX_acme.u_9f3a"}).bootParams(sourceFetch)
	require.NoError(t, err)
	assert.Equal(t, "_INBOX_acme.u_9f3a", boot.inboxPrefix)

	// Unset stays unset — the nats.go default inbox, exactly as before.
	boot, err = (&GatewayCmd{ConfigSubject: "cfg"}).bootParams(sourceFetch)
	require.NoError(t, err)
	assert.Empty(t, boot.inboxPrefix)
}

// A custom inbox prefix must cover EVERY request/reply this process issues —
// the config fetch AND the `nats` cred resolver — because that is the whole
// point: it lets a scoped pod's identity be granted a narrow inbox instead of
// `_INBOX.>`, the account's entire reply namespace. Anything that built a
// reply subject by hand would silently escape it.
func TestInboxPrefixCoversConfigAndCredRequests(t *testing.T) {
	_, url := fetchNATS(t)
	const prefix = "_INBOX_acme.u_9f3a"

	nc, err := nats.Connect(url, nats.CustomInboxPrefix(prefix))
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	// Responders record the reply subject the gateway asked them to answer on.
	replies := make(chan string, 2)
	record := func(subject string, body string) {
		sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
			replies <- m.Reply
			_ = m.Respond([]byte(body))
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	record("mcp.cfg.request", `{"servers":{}}`)
	record("mcp.v1.cred.>", `{"env":{"TOKEN":"t"}}`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := &configsource.NATS{Conn: nc, RequestSubject: "mcp.cfg.request", Logger: testLogger()}
	u := <-src.Watch(ctx)
	require.NoError(t, u.Err)

	_, err = buildResolver(&config.Auth{Mode: config.AuthNATS}, nc, "").
		Resolve(ctx, "acme", "u1", "gh")
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		reply := <-replies
		assert.True(t, strings.HasPrefix(reply, prefix+"."),
			"reply inbox %q must live under the custom prefix %q", reply, prefix)
	}
}

// The POINT of the inbox prefix, against a real NATS identity that is actually
// restricted: a gateway granted subscribe on ONLY its own inbox works, and the
// same identity without the prefix is denied.
//
// This is what the feature exists for. Without the prefix, an identity needs
// subscribe on `_INBOX.>` — the account's entire reply namespace — so a pod
// running third-party MCP server code beside the gateway could lift the
// connection's credential and read every other tenant's credential replies.
// Asserting that reply subjects merely *start with* the prefix does not prove
// a narrowed grant is survivable; only the server refusing the wider form does.
func TestInboxPrefixWorksUnderARestrictedIdentity(t *testing.T) {
	const prefix = "_INBOX_mcpgw.acme"

	// The gateway may publish its two request subjects and subscribe to
	// NOTHING but its own prefixed inbox. No `_INBOX.>`. NoAuthUser binds the
	// unauthenticated connection natstest opens to the unrestricted responder,
	// so that connection serves the replies.
	rConn, url := natstest.Run(t, &server.Options{
		NoAuthUser: "responder",
		Users: []*server.User{
			{
				Username: "gateway", Password: "gw",
				Permissions: &server.Permissions{
					Publish:   &server.SubjectPermission{Allow: []string{"mcp.cfg.request", "mcp.v1.cred.>"}},
					Subscribe: &server.SubjectPermission{Allow: []string{prefix + ".>"}},
				},
			},
			{Username: "responder", Password: "r"}, // unrestricted; serves the replies
		},
	})

	for subject, body := range map[string]string{
		"mcp.cfg.request": `{"servers":{}}`,
		"mcp.v1.cred.>":   `{"env":{"TOKEN":"t"}}`,
	} {
		sub, err := rConn.Subscribe(subject, func(m *nats.Msg) { _ = m.Respond([]byte(body)) })
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	require.NoError(t, rConn.Flush())

	// connect returns a gateway connection plus a channel of async errors, so
	// the denied case is observed directly rather than inferred from a timeout.
	connect := func(inboxPrefix string) (*nats.Conn, chan error) {
		errs := make(chan error, 8)
		opts := []nats.Option{
			nats.UserInfo("gateway", "gw"),
			nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
				select {
				case errs <- err:
				default:
				}
			}),
		}
		if inboxPrefix != "" {
			opts = append(opts, nats.CustomInboxPrefix(inboxPrefix))
		}
		nc, err := nats.Connect(url, opts...)
		require.NoError(t, err)
		t.Cleanup(nc.Close)
		return nc, errs
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("with the prefix the narrow grant is enough", func(t *testing.T) {
		nc, errs := connect(prefix)

		u := <-(&configsource.NATS{Conn: nc, RequestSubject: "mcp.cfg.request", Logger: testLogger()}).Watch(ctx)
		require.NoError(t, u.Err, "config fetch must succeed under the narrow grant")

		creds, err := buildResolver(&config.Auth{Mode: config.AuthNATS}, nc, "").Resolve(ctx, "acme", "u1", "gh")
		require.NoError(t, err, "cred fetch must succeed under the narrow grant")
		assert.Equal(t, "t", creds.Env["TOKEN"])

		select {
		case err := <-errs:
			t.Fatalf("no permission error expected, got %v", err)
		default:
		}
	})

	t.Run("without the prefix the same identity is denied", func(t *testing.T) {
		nc, errs := connect("") // default _INBOX.<nuid>, which the grant excludes

		_, err := buildResolver(&config.Auth{Mode: config.AuthNATS}, nc, "").Resolve(ctx, "acme", "u1", "gh")
		require.Error(t, err, "the default inbox is outside the grant, so the reply can never arrive")

		select {
		case err := <-errs:
			assert.ErrorIs(t, err, nats.ErrPermissionViolation)
		case <-time.After(2 * time.Second):
			t.Fatal("expected a subscribe permission violation on the default inbox")
		}
	})
}

// The `nats` cred mode's default subject derives from the CONFIGURED wire
// prefix. A hardcoded default agrees with the control plane only while the
// prefix is the default one; past that the pod publishes cred requests to a
// subject its own JWT denies, and every tool call fails while it still reports
// healthy.
func TestCredSubjectDerivesFromSubjectPrefix(t *testing.T) {
	nc, _ := fetchNATS(t)

	for _, tc := range []struct {
		name       string
		wirePrefix string
		auth       *config.Auth
		want       string
	}{
		{"default prefix", "", &config.Auth{Mode: config.AuthNATS}, "mcp.v1.cred.acme.u1.gh"},
		{"custom prefix", "acme.mcp", &config.Auth{Mode: config.AuthNATS}, "acme.mcp.cred.acme.u1.gh"},
		{"explicit subject wins", "acme.mcp", &config.Auth{Mode: config.AuthNATS, Subject: "ctl.creds"}, "ctl.creds.acme.u1.gh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan string, 1)
			sub, err := nc.Subscribe(tc.want, func(m *nats.Msg) {
				got <- m.Subject
				_ = m.Respond([]byte(`{"env":{"TOKEN":"t"}}`))
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = sub.Unsubscribe() })

			creds, err := buildResolver(tc.auth, nc, tc.wirePrefix).
				Resolve(context.Background(), "acme", "u1", "gh")
			require.NoError(t, err)
			assert.Equal(t, "t", creds.Env["TOKEN"])
			assert.Equal(t, tc.want, <-got)
		})
	}
}

// ...and the registry threads the boot prefix into every resolver it builds,
// which is the whole path a --subject-prefix takes to a cred request.
func TestCredRegistryThreadsSubjectPrefix(t *testing.T) {
	nc, _ := fetchNATS(t)

	got := make(chan string, 1)
	sub, err := nc.Subscribe("acme.mcp.cred.>", func(m *nats.Msg) {
		got <- m.Subject
		_ = m.Respond([]byte(`{"env":{"TOKEN":"t"}}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	r := &credRegistry{nc: nc, log: testLogger(), prefix: "acme.mcp"}
	resolver, perUser := r.lookup("gh", config.Server{
		Command: "x", Auth: &config.Auth{Mode: config.AuthNATS},
	})
	require.NotNil(t, resolver)
	assert.True(t, perUser)

	_, err = resolver.Resolve(context.Background(), "acme", "u1", "gh")
	require.NoError(t, err)
	assert.Equal(t, "acme.mcp.cred.acme.u1.gh", <-got)
}

// The expiry clamp bounds how long a leaked credential stays usable, so it
// applies only when the reply carried something leakable. A reply with an
// expiry and no material must not recycle the process on that cadence —
// npx/uvx cold starts would surface as periodic latency spikes on a server
// that has no credentials at all.
func TestBuildBackendExpiryRequiresCredentialMaterial(t *testing.T) {
	exp := time.Now().Add(time.Hour).UTC()
	build := func(t *testing.T, s config.Server, c *cred.Credentials) backend.Backend {
		t.Helper()
		r := cred.Cached(cred.ResolveFunc(
			func(context.Context, string, string, string) (*cred.Credentials, error) { return c, nil },
		), 0)
		_, gen, err := r.ResolveGen(context.Background(), "acme", "u1", "s")
		require.NoError(t, err)
		b, err := buildBackend(
			backend.Key{Server: "s", Tenant: "acme", CredSet: "u1", CredVersion: gen},
			s, r, testLogger(),
		)
		require.NoError(t, err)
		return b
	}
	stdio := config.Server{Command: "true"}
	http := config.Server{Transport: "http", URL: "https://example.invalid/mcp"}

	// Material present: the clamp holds (the property that bounds reuse of a
	// leaked credential).
	b := build(t, stdio, &cred.Credentials{Env: map[string]string{"TOKEN": "t"}, ExpiresAt: exp})
	e, ok := b.(backend.Expiring)
	require.True(t, ok, "env material must bound the backend's life")
	assert.Equal(t, exp, e.CredExpiresAt())

	b = build(t, http, &cred.Credentials{Headers: map[string]string{"Authorization": "Bearer t"}, ExpiresAt: exp})
	e, ok = b.(backend.Expiring)
	require.True(t, ok, "header material must bound the backend's life")
	assert.Equal(t, exp, e.CredExpiresAt())

	// Expiry with no material: nothing to leak, so no bounded lifetime.
	b = build(t, stdio, &cred.Credentials{ExpiresAt: exp})
	_, ok = b.(backend.Expiring)
	assert.False(t, ok, "a materially-empty reply must not bound the backend's life")

	b = build(t, http, &cred.Credentials{Headers: map[string]string{}, Env: map[string]string{}, ExpiresAt: exp})
	_, ok = b.(backend.Expiring)
	assert.False(t, ok, "empty maps are not credential material")
}

// End to end: an inline --config-json document, a tenant-scoped wire, and a
// real stdio backend — every user of the scoped tenant is served, another
// tenant is not. This is the scoped stdio-pod shape.
func TestGatewayInlineConfigServesTenantScoped(t *testing.T) {
	nc, url := fetchNATS(t)
	a := assembleScoped(t, nc, url, "demo", "") // tenant-only scope

	inline := fmt.Sprintf(`{"servers":{%s}}`, fakeServerJSON("a", fakeEnv(nil)))
	src, _ := inlineSource(t, &GatewayCmd{ConfigJSON: inline})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = configsource.Run(ctx, testLogger(), src, func(cfg *config.Config) error {
			_, err := a.rec.Apply(cfg)
			return err
		})
	}()

	// The inline server comes up and serves any user of the scoped tenant.
	waitServing(t, a, "a") // user "_"
	for _, user := range []string{"u1", "u2"} {
		assert.Equal(t, wire.FrameEnd, a.callAs(t, user, "a", "echo", nil).Kind,
			"user %q of the scoped tenant must be served", user)
	}

	// Another tenant's request never reaches the scoped pod.
	other, err := wire.NewClient(a.clientNC, wire.ClientConfig{Tenant: "other", User: "u1", Inactivity: 3 * time.Second})
	require.NoError(t, err)
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "1", "method": "tools/call", "params": map[string]any{}})
	s, err := other.Do(context.Background(), &wire.Request{
		Server: "a", Method: "tools/call", ProtocolVersion: mcpspec.ProtocolVersion, Body: body,
	})
	require.NoError(t, err)
	var last wire.Frame
	for f := range s.C {
		last = f
	}
	require.NotNil(t, last.Err, "another tenant must not reach the scoped pod")
	assert.Equal(t, wire.ErrCodeNoGateway, last.Err.Code)
}

func waitServing(t *testing.T, a *assembled, serverName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return a.call(t, serverName, "echo").Kind == wire.FrameEnd
	}, 8*time.Second, 100*time.Millisecond, "server %q never began serving", serverName)
}

// callAs is call with an explicit user identity and tool arguments.
func (a *assembled) callAs(t *testing.T, user, serverName, tool string, args map[string]any) wire.Frame {
	t.Helper()
	c, err := wire.NewClient(a.clientNC, wire.ClientConfig{Tenant: "demo", User: user, Inactivity: 3 * time.Second})
	require.NoError(t, err)
	if args == nil {
		args = map[string]any{}
	}
	params, _ := json.Marshal(map[string]any{
		"name": tool, "arguments": args,
		"_meta": map[string]any{mcpspec.MetaProtocolVersion: mcpspec.ProtocolVersion},
	})
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": "1", "method": "tools/call",
		"params": json.RawMessage(params),
	})
	s, err := c.Do(context.Background(), &wire.Request{
		Server: serverName, Method: "tools/call", Name: tool,
		ProtocolVersion: mcpspec.ProtocolVersion, Body: body,
	})
	require.NoError(t, err)
	var last wire.Frame
	for f := range s.C {
		last = f
	}
	return last
}

// The full config-driven per-user path: an auth.mode=exec server resolves a
// distinct credential per caller (credRegistry -> Cached(Exec) -> proxy pool
// key -> buildBackend env merge), while config env non-secrets survive.
func TestGatewayPerUserExecCredentials(t *testing.T) {
	nc, url := fetchNATS(t)
	a := assemble(t, nc, url)

	script := filepath.Join(t.TempDir(), "helper.sh")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
echo "{\"env\":{\"TOKEN\":\"tok-$NATSMCP_CRED_USER\"},\"expiresAt\":\"2100-01-01T00:00:00Z\"}"
`), 0o755))

	serverJSON := fmt.Sprintf(`"a":{"protocol":%q,"transport":"stdio","command":%q,"env":{%q:"1","REGION":"eu-1"},"auth":{"mode":"exec","command":%q}}`,
		mcpspec.ProtocolVersion, os.Args[0], fakemcp.EnvFlag, script)

	mc := &mutableConfig{nc: nc}
	mc.set(serverJSON)
	mc.serve(t)

	src := &configsource.NATS{
		Conn:           nc,
		RequestSubject: "mcp.cfg.request",
		Refetch:        time.Hour,
		Logger:         testLogger(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = configsource.Run(ctx, testLogger(), src, func(cfg *config.Config) error {
			_, err := a.rec.Apply(cfg)
			return err
		})
	}()
	waitServing(t, a, "a")

	envOf := func(user, name string) (float64, string) {
		f := a.callAs(t, user, "a", "env", map[string]any{"name": name})
		require.Equal(t, wire.FrameEnd, f.Kind, "expected result, got %+v", f)
		m, err := jsonrpc.Decode(f.Body)
		require.NoError(t, err)
		require.Nil(t, m.Error)
		var r struct {
			PID   float64 `json:"pid"`
			Value string  `json:"value"`
		}
		require.NoError(t, json.Unmarshal(m.Result, &r))
		return r.PID, r.Value
	}

	pidAlice, tokAlice := envOf("alice", "TOKEN")
	pidBob, tokBob := envOf("bob", "TOKEN")
	_, region := envOf("alice", "REGION")

	assert.Equal(t, "tok-alice", tokAlice)
	assert.Equal(t, "tok-bob", tokBob)
	assert.NotEqual(t, pidAlice, pidBob, "per-user servers must not share a process across users")
	assert.Equal(t, "eu-1", region, "config env non-secrets must survive the credential merge")
}

// buildBackend refuses to build when the credentials rotated between the
// proxy's resolve (which stamped the pool key) and its own — a mislabeled
// entry would alias the pool. The caller retries and keys the new generation.
func TestBuildBackendRejectsGenerationMismatch(t *testing.T) {
	calls := 0
	resolver := cred.Cached(cred.ResolveFunc(
		func(context.Context, string, string, string) (*cred.Credentials, error) {
			calls++
			return &cred.Credentials{Env: map[string]string{"TOKEN": fmt.Sprintf("t%d", calls)}}, nil
		},
	), 0)

	_, gen, err := resolver.ResolveGen(context.Background(), "acme", "u1", "s")
	require.NoError(t, err)

	// Simulate a rotation between the proxy's resolve and the factory's.
	resolver.Invalidate("acme", "u1", "s")

	key := backend.Key{Server: "s", Tenant: "acme", CredSet: "u1", CredVersion: gen}
	_, err = buildBackend(key, config.Server{Command: "true"}, resolver, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rotated during spawn")

	// With a consistent key the build succeeds.
	_, gen2, err := resolver.ResolveGen(context.Background(), "acme", "u1", "s")
	require.NoError(t, err)
	key.CredVersion = gen2
	_, err = buildBackend(key, config.Server{Command: "true"}, resolver, testLogger())
	require.NoError(t, err)
}

// The factory's own resolve is a second conduit for the same material: its
// error becomes the pool's error, which the proxy hands the caller as -32010
// with the text intact.
func TestBuildBackendDoesNotEchoTheResolverDetail(t *testing.T) {
	const detail = "helper stderr: no grant for /var/run/secrets/acme/u1.jwt (token AKIAWOULDBEBAD)"
	resolver := cred.Cached(cred.ResolveFunc(
		func(context.Context, string, string, string) (*cred.Credentials, error) {
			return nil, cred.Terminal(errors.New(detail))
		},
	), 0)

	key := backend.Key{Server: "s", Tenant: "acme", CredSet: "u1", CredVersion: 7}
	_, err := buildBackend(key, config.Server{Command: "true"}, resolver, testLogger())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "u1.jwt", "the resolver's error text must not travel to the caller")
	assert.NotContains(t, err.Error(), "AKIAWOULDBEBAD")
	assert.Contains(t, err.Error(), "gateway ref ")
}

// A server removed from config must not leave its resolver (and cached
// credential material) behind in the registry.
func TestCredRegistryPrunesRemovedServers(t *testing.T) {
	r := &credRegistry{log: testLogger()}
	srv := config.Server{Command: "x", Auth: &config.Auth{Mode: config.AuthExec, Command: "helper"}}

	resolver, perUser := r.lookup("gone", srv)
	require.NotNil(t, resolver)
	require.True(t, perUser)
	require.Len(t, r.entries, 1)

	r.prune(map[string]config.Server{"kept": srv})
	assert.Empty(t, r.entries, "a removed server's resolver must be pruned")

	// Present servers survive a prune.
	r.lookup("kept", srv)
	r.prune(map[string]config.Server{"kept": srv})
	assert.Len(t, r.entries, 1)
}

// A lookup racing a prune with a STALE config snapshot must not resurrect a
// removed server's resolver: the insert path consults the live config.
func TestCredRegistryLookupCannotResurrectPrunedServer(t *testing.T) {
	liveServers := map[string]config.Server{}
	r := &credRegistry{log: testLogger(), live: func(name string) (config.Server, bool) {
		s, ok := liveServers[name]
		return s, ok
	}}
	oldDef := config.Server{Command: "x", Auth: &config.Auth{Mode: config.AuthExec, Command: "helper"}}

	// Server present: lookup populates normally.
	liveServers["s"] = oldDef
	resolver, _ := r.lookup("s", oldDef)
	require.NotNil(t, resolver)

	// Server removed + pruned; a caller holding the OLD snapshot races in.
	delete(liveServers, "s")
	r.prune(liveServers)
	resolver, perUser := r.lookup("s", oldDef) // stale snapshot's definition
	assert.Nil(t, resolver, "a stale snapshot must not resurrect a pruned server's resolver")
	assert.False(t, perUser)
	assert.Empty(t, r.entries)
}
