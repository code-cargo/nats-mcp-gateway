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
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/kong"
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

// The connect failure is the likeliest place a NATS URL is ever read by a
// human, and until it was redacted it was also the likeliest place the
// password leaked: a gateway that cannot reach NATS crash-loops, so the error
// lands in a pod's event stream and in whatever the operator pastes into a
// ticket.
func TestConnectErrorRedactsPassword(t *testing.T) {
	port := closedPort(t)
	err := connectErr(t, "nats://gw:s3cr3t@127.0.0.1:"+port)
	assert.NotContains(t, err.Error(), "s3cr3t")
	assert.Contains(t, err.Error(), "nats://gw:xxxxx@127.0.0.1:"+port,
		"the host and identity must survive: they are what the operator needs")
}

// The wire prefix and queue group get the same boot check as the inbox prefix,
// and for a sharper reason: nothing downstream rejects a wildcard prefix.
// "mcp.*" binds the endpoint subject "mcp.*.req.*.*.{server}.>" — micro accepts
// it, NATS binds it, and this gateway then receives traffic addressed to every
// other prefix in the account. The empty-token forms and a spaced queue group
// do fail, but only at the first config apply, from nats.go and micro, naming
// neither the setting nor the value.
func TestBootParamsValidatesSubjectPrefixAndQueueGroup(t *testing.T) {
	for _, bad := range []string{"mcp.*", "mcp.>", "mcp v1", "mcp.v1.", ".mcp.v1", "mcp..v1"} {
		_, err := (&GatewayCmd{ConfigSubject: "cfg", SubjectPrefix: bad}).bootParams(sourceFetch)
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "--subject-prefix", bad)
	}
	for _, bad := range []string{"mcpgw.*", "mcpgw.>", "mcp gw", "mcpgw.", "mcpgw..acme"} {
		_, err := (&GatewayCmd{ConfigSubject: "cfg", QueueGroup: bad}).bootParams(sourceFetch)
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "--queue-group", bad)
	}

	boot, err := (&GatewayCmd{ConfigSubject: "cfg", SubjectPrefix: "acme.mcp", QueueGroup: "mcpgw.acme"}).
		bootParams(sourceFetch)
	require.NoError(t, err)
	assert.Equal(t, "acme.mcp", boot.prefix)
	assert.Equal(t, "mcpgw.acme", boot.queueGroup)

	// Unset stays unset, so wire.Serve still applies its scope-aware queue
	// group default.
	boot, err = (&GatewayCmd{ConfigSubject: "cfg"}).bootParams(sourceFetch)
	require.NoError(t, err)
	assert.Empty(t, boot.prefix)
	assert.Empty(t, boot.queueGroup)
}

func writeConfig(t *testing.T, doc string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.json")
	require.NoError(t, os.WriteFile(path, []byte(doc), 0o600))
	return path
}

// The fetch/inline-only flags never reach the file source — it reads its
// connection, scope, pool and claim-check settings from the document — so a
// supplied one that the document contradicts must fail the boot. This is the
// inline `nats` block hazard arriving from the other direction: the per-user
// pod recipe injects NATSMCP_SCOPE_TENANT/NATSMCP_SCOPE_USER as env, and with a
// mounted file that carries no matching nats block the pod would boot UNSCOPED,
// bind {prefix}.req.*.*.{server}.>, and serve every tenant using its own
// per-user credentials.
func TestBootParamsFileSourceRejectsIgnoredFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
		cmd  GatewayCmd
		want []string
	}{
		{
			name: "scope injected as env, document unscoped",
			doc:  `{"servers":{}}`,
			cmd:  GatewayCmd{ScopeTenant: "acme", ScopeUser: "u_9f3a"},
			want: []string{
				`--scope-tenant="acme"`, "NATSMCP_SCOPE_TENANT", `nats.tenant=""`,
				`--scope-user="u_9f3a"`, "NATSMCP_SCOPE_USER", `nats.user=""`,
			},
		},
		{
			name: "scope disagreeing with the document",
			doc:  `{"nats":{"tenant":"other"},"servers":{}}`,
			cmd:  GatewayCmd{ScopeTenant: "acme"},
			want: []string{`--scope-tenant="acme"`, `nats.tenant="other"`},
		},
		{
			name: "url dropped for the loopback fallback",
			doc:  `{"servers":{}}`,
			cmd:  GatewayCmd{NatsURL: "nats://prod:4222", NatsCreds: "/run/gw.creds"},
			want: []string{
				`--nats-url="nats://prod:4222"`, `nats.url="nats://127.0.0.1:4222"`,
				`--nats-creds="/run/gw.creds"`, `nats.credsFile=""`,
			},
		},
		{
			name: "identity fencing quietly not applied",
			doc:  `{"servers":{}}`,
			cmd:  GatewayCmd{InboxPrefix: "_INBOX_acme.u_9f3a", QueueGroup: "mcpgw.acme"},
			want: []string{
				`--inbox-prefix="_INBOX_acme.u_9f3a"`, `nats.inboxPrefix=""`,
				`--queue-group="mcpgw.acme"`, `nats.queueGroup="mcpgw"`,
			},
		},
		{
			name: "wire prefix disagreeing with the document",
			doc:  `{"nats":{"subjectPrefix":"acme.mcp"},"servers":{}}`,
			cmd:  GatewayCmd{SubjectPrefix: "other.mcp"},
			want: []string{`--subject-prefix="other.mcp"`, `nats.subjectPrefix="acme.mcp"`},
		},
		{
			// A silent document is not asking for the empty prefix, so the
			// message names what the wire will bind rather than the blank the
			// field holds — the same courtesy the queue group and the pool get.
			name: "wire prefix dropped for the wire default",
			doc:  `{"servers":{}}`,
			cmd:  GatewayCmd{SubjectPrefix: "acme.mcp"},
			want: []string{`--subject-prefix="acme.mcp"`, `nats.subjectPrefix="mcp.v1"`},
		},
		{
			name: "pool limits",
			doc:  `{"pool":{"maxProcsPerTenant":16},"servers":{}}`,
			cmd: GatewayCmd{
				PoolMaxConcurrent: 7, PoolMaxProcsPerTenant: 64,
				PoolIdleTTL: 30 * time.Minute, PoolMaxLifetime: 24 * time.Hour,
			},
			want: []string{
				"--pool-max-concurrent=7", "pool.maxConcurrent=32",
				"--pool-max-procs-per-tenant=64", "pool.maxProcsPerTenant=16",
				"--pool-idle-ttl=30m0s", "--pool-max-lifetime=24h0m0s",
			},
		},
		{
			// The whole feature is being dropped, so the sizing is reported
			// raw: there is no bucket, and naming wire's 5m/1GiB here would
			// describe limits nothing is going to apply.
			name: "claim-check enabled by flag, absent from the document",
			doc:  `{"servers":{}}`,
			cmd:  GatewayCmd{ClaimCheck: true, ClaimMaxAge: 9 * time.Minute, ClaimMaxBytes: 5},
			want: []string{
				"--claim-check=true", "claimCheck=false",
				"--claim-max-age=9m0s", "claimCheck.maxAge=0s",
				"--claim-max-bytes=5", "claimCheck.maxBytes=0",
			},
		},
		{
			// Here the document DOES enable it, so an unsized block is asking
			// for wire's substituted limits and the message names those — the
			// zero it literally holds is not what the bucket is about to get.
			name: "claim sizing disagreeing with an unsized document block",
			doc:  `{"claimCheck":{},"servers":{}}`,
			cmd:  GatewayCmd{ClaimMaxAge: 10 * time.Minute, ClaimMaxBytes: 7},
			want: []string{
				"--claim-max-age=10m0s", "claimCheck.maxAge=5m0s",
				"--claim-max-bytes=7", "claimCheck.maxBytes=1073741824",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.cmd
			c.Config = writeConfig(t, tc.doc)
			_, err := c.bootParams(sourceFile)
			require.Error(t, err)
			for _, want := range tc.want {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

// Only a CONFLICT fails. A value the document already carries drops nothing,
// and NATSMCP_NATS_URL, NATSMCP_SUBJECT_PREFIX and NATSMCP_INBOX_PREFIX are
// shared with the shim and call commands — one exported value per deployment is
// ordinary, and refusing to boot over it would break working fleets to correct
// a no-op.
func TestBootParamsFileSourceAcceptsAgreeingFlags(t *testing.T) {
	path := writeConfig(t, `{
		"nats":{"url":"nats://prod:4222","credsFile":"/run/gw.creds","subjectPrefix":"acme.mcp",
		        "inboxPrefix":"_INBOX_acme.u_9f3a","queueGroup":"mcpgw.acme","tenant":"acme","user":"u_9f3a"},
		"pool":{"maxProcsPerTenant":64},
		"claimCheck":{"maxAge":"9m","maxBytes":5},
		"servers":{}}`)

	boot, err := (&GatewayCmd{
		Config:  path,
		NatsURL: "nats://prod:4222", NatsCreds: "/run/gw.creds", SubjectPrefix: "acme.mcp",
		InboxPrefix: "_INBOX_acme.u_9f3a", QueueGroup: "mcpgw.acme",
		ScopeTenant: "acme", ScopeUser: "u_9f3a", PoolMaxProcsPerTenant: 64,
		ClaimCheck: true, ClaimMaxAge: 9 * time.Minute, ClaimMaxBytes: 5,
	}).bootParams(sourceFile)
	require.NoError(t, err)
	assert.Equal(t, "acme", boot.tenant)
	assert.Equal(t, "u_9f3a", boot.user)

	// Nothing supplied: the document governs, exactly as it always has.
	boot, err = (&GatewayCmd{Config: path}).bootParams(sourceFile)
	require.NoError(t, err)
	assert.Equal(t, "acme", boot.tenant)
	assert.Equal(t, "nats://prod:4222", boot.url)
	assert.Equal(t, "_INBOX_acme.u_9f3a", boot.inboxPrefix)
}

// clearNATSMCPEnv unsets every NATSMCP_* variable for the duration of the test,
// so the developer's shell cannot decide whether a defaults-only parse passes.
func clearNATSMCPEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		name, value, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, "NATSMCP_") {
			continue
		}
		require.NoError(t, os.Unsetenv(name))
		t.Cleanup(func() { _ = os.Setenv(name, value) })
	}
}

// Against the REAL grammar, because the guard has to tell an operator's value
// from kong's own default and kong itself cannot: Context.Reset() parses the
// `default:` tag through Value.Parse, which marks the value Set whether or not
// anything was supplied, so every defaulted flag reports Set on a bare
// `gateway --config` and only the default VALUE distinguishes them. This pins
// that comparison to the tags — a changed default fails here rather than
// erroring every file-source boot — and drives the guard down the path the
// hazard actually takes: scope arriving as pod env, never as an argv the
// operator could see was ignored.
func TestFileSourceGuardAgainstKongDefaults(t *testing.T) {
	clearNATSMCPEnv(t)
	path := writeConfig(t, `{"servers":{}}`)

	parse := func(t *testing.T, cfgPath string) *GatewayCmd {
		t.Helper()
		var cli CLI
		parser, err := kong.New(&cli, kong.Name("natsmcp"), kong.Exit(func(int) {}))
		require.NoError(t, err)
		_, err = parser.Parse([]string{"gateway", "--config", cfgPath})
		require.NoError(t, err)
		return &cli.Gateway
	}

	// Defaults only: the ordinary file-source boot, which must survive.
	boot, err := parse(t, path).bootParams(sourceFile)
	require.NoError(t, err)
	assert.Equal(t, nats.DefaultURL, boot.url)
	assert.Empty(t, boot.tenant)

	// And again against a document that ENABLES claim-check, because the sizing
	// comparison is skipped entirely when neither side asks for it — a
	// defaults-only document alone would leave --claim-max-age and
	// --claim-max-bytes free to drift from wire's substituted limits, and the
	// first operator to mount a claimCheck block would be refused a boot they
	// had asked nothing of. The block is left unsized deliberately: that is what
	// makes the flag defaults meet wire.FilledClaimLimits head-on.
	_, err = parse(t, writeConfig(t, `{"claimCheck":{},"servers":{}}`)).bootParams(sourceFile)
	require.NoError(t, err, "a claimCheck document must not be refused over untouched sizing flags")

	// The reported failure: scope injected as env, mounted file with no nats
	// block. Before the guard this booted unscoped, serving every tenant.
	t.Setenv("NATSMCP_SCOPE_TENANT", "acme")
	t.Setenv("NATSMCP_SCOPE_USER", "u_9f3a")
	_, err = parse(t, path).bootParams(sourceFile)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NATSMCP_SCOPE_TENANT")
	assert.Contains(t, err.Error(), "nats.tenant")
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

// Release tags are "vX.Y.Z", and the leading v sent every one of them to the
// 0.0.0 fallback — so `nats micro list` reported 0.0.0 for the whole fleet and
// could not tell a rolled-out build from the one it replaced, which is most of
// what that command is for during a rollout.
func TestNormalizeVersionKeepsReleaseTags(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"v1.2.3", "1.2.3"},
		{"v1.2.3-rc.1", "1.2.3-rc.1"},
		{"1.2.3", "1.2.3"},
		{"v1.2.3+build.7", "1.2.3+build.7"},
		// No semver reading: the Makefile's default, and a build off an
		// untagged tree.
		{"develop", "0.0.0"},
		{"dev", "0.0.0"},
		{"", "0.0.0"},
		{"v", "0.0.0"},
		// Stripping the "v" must not walk a MALFORMED tag past the fallback.
		// release.yml takes its tag as free text, so each of these is one
		// keystroke away from a real release — and each is rejected by micro,
		// which would fail wire.Serve and with it the boot of the binary that
		// release just shipped. The fallback is the whole point: an unhelpful
		// version beats a gateway that will not start.
		{"v1.2", "0.0.0"},
		{"v2", "0.0.0"},
		{"v1.02.3", "0.0.0"},  // semver forbids the leading zero
		{"v1.2.3.4", "0.0.0"}, // a fourth component is not semver
		{"v1.2.3_rc1", "0.0.0"},
	} {
		assert.Equal(t, tc.want, normalizeVersion(tc.in), tc.in)
	}
}

// ...and the output still has to satisfy micro, which rejects a non-semver
// Version outright. That check is why normalizeVersion exists, so stripping
// the "v" must not walk past it: a rejected version fails wire.Serve, which
// fails the boot.
func TestNormalizeVersionSatisfiesMicro(t *testing.T) {
	nc, _ := fetchNATS(t)
	for _, v := range []string{
		"v1.2.3", "v1.2.3-rc.1", "v1.2.3+build.7", "develop",
		// The malformed tags go through micro too: the fallback is only worth
		// anything if what it catches is exactly what micro would reject.
		"v1.2", "v2", "v1.02.3", "v1.2.3.4", "v1.2.3_rc1",
	} {
		ws, err := wire.Serve(nc, wire.ServerConfig{
			Version: normalizeVersion(v),
			Servers: []string{"a"},
		}, nil)
		require.NoError(t, err, v)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		require.NoError(t, ws.Shutdown(ctx))
		cancel()
	}
}

func waitServing(t *testing.T, a *assembled, serverName string) {
	t.Helper()
	waitServingAs(t, a, "", serverName)
}

// waitServingAs is waitServing with a caller identity, which a server whose
// credentials are per-user requires: the gateway refuses the unattributed "_"
// token there rather than resolve a credential for it, so an anonymous probe
// would wait out the timeout against a server that is serving perfectly well.
func waitServingAs(t *testing.T, a *assembled, user, serverName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return a.callAs(t, user, serverName, "echo", nil).Kind == wire.FrameEnd
	}, 8*time.Second, 100*time.Millisecond, "server %q never began serving", serverName)
}

// callAs is call with an explicit user identity and tool arguments.
func (a *assembled) callAs(t *testing.T, user, serverName, tool string, args map[string]any) wire.Frame {
	t.Helper()
	return wireCall(t, a.clientNC, "demo", user, serverName, tool, args)
}

// wireCall issues one tools/call over the wire and returns the terminal frame.
func wireCall(t *testing.T, nc *nats.Conn, tenant, user, serverName, tool string, args map[string]any) wire.Frame {
	t.Helper()
	c, err := wire.NewClient(nc, wire.ClientConfig{Tenant: tenant, User: user, Inactivity: 3 * time.Second})
	require.NoError(t, err)
	f, err := clientCall(c, serverName, tool, args)
	require.NoError(t, err)
	return f
}

// clientCall is wireCall's fallible half, on a client the caller owns. Use it
// where failing the test in place is wrong — require.Eventually runs its
// condition on a goroutine of its own, and t.FailNow there does not stop the
// test — or where one client should serve many calls: NewClient chains onto the
// connection's async error handler, so building one per poll tick grows that
// chain for the life of the connection.
func clientCall(c *wire.Client, serverName, tool string, args map[string]any) (wire.Frame, error) {
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
	if err != nil {
		return wire.Frame{}, err
	}
	var last wire.Frame
	for f := range s.C {
		last = f
	}
	return last, nil
}

// SIGHUP end to end through the real gateway: signal handler -> the file
// source's reload hook -> re-read -> apply -> a new server answering on the
// wire. Every link in that chain is process-level, so nothing below cmd can
// cover it — and the one link an operator can actually observe (a HUP that
// does nothing) is the one worth pinning.
func TestGatewaySighupReloadsFileConfig(t *testing.T) {
	_, url := fetchNATS(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "gw.json")
	writeCfg := func(entries ...string) {
		body := fmt.Sprintf(`{"nats":{"url":%q},"servers":{%s}}`, url, strings.Join(entries, ","))
		tmp := path + ".tmp"
		require.NoError(t, os.WriteFile(tmp, []byte(body), 0o600))
		require.NoError(t, os.Rename(tmp, path))
	}
	writeCfg(fakeServerJSON("a", fakeEnv(nil)))

	clientNC, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(clientNC.Close)
	c, err := wire.NewClient(clientNC, wire.ClientConfig{Tenant: "demo", Inactivity: 3 * time.Second})
	require.NoError(t, err)
	// Polled from require.Eventually's goroutine, so it reports rather than
	// fails: a not-yet-bound server is the expected answer here, not an error.
	serving := func(server string) bool {
		f, err := clientCall(c, server, "echo", nil)
		return err == nil && f.Kind == wire.FrameEnd
	}

	// runGateway installs process-wide signal handlers and never removes them;
	// drop them with the test so `go test` stays interruptible afterwards.
	t.Cleanup(func() { signal.Reset(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP) })

	done := make(chan error, 1)
	go func() {
		// ReloadInterval stays 0 so polling is off and SIGHUP is the only
		// thing that can produce the second server.
		done <- runGateway(&GatewayCmd{Config: path}, &Globals{LogLevel: "error", LogFormat: "text"}, "0.0.0")
	}()

	// Serving proves the handlers are installed, so the signals below cannot
	// reach the default disposition and kill the test binary.
	require.Eventually(t, func() bool { return serving("a") }, 15*time.Second, 100*time.Millisecond,
		"the gateway never began serving its initial config")

	writeCfg(fakeServerJSON("a", fakeEnv(nil)), fakeServerJSON("b", fakeEnv(nil)))
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGHUP))
	require.Eventually(t, func() bool { return serving("b") }, 15*time.Second, 100*time.Millisecond,
		"SIGHUP did not reload the file source")

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
	select {
	case err := <-done:
		assert.NoError(t, err, "a signal-driven shutdown is a clean exit")
	case <-time.After(15 * time.Second):
		t.Fatal("the gateway did not drain after SIGTERM")
	}
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
	waitServingAs(t, a, "alice", "a")

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
	// A pool failure reaches the client as -32010, which means "re-issue". A
	// refusal that re-issuing cannot fix has to say so, or the contract of the
	// code the caller sees is the only advice it gets.
	assert.Contains(t, err.Error(), "do not retry", "an authoritative refusal must survive the suppression")
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

// TestFileSourceGuardAllowsSettingsThatDropNothing is the counterweight to
// TestBootParamsFileSourceRejectsIgnoredFlags. The guard exists to catch a
// setting the file source would silently discard, and refusing one it would
// have honoured anyway is the same class of harm pointed the other way: a
// deployment that already works stops booting.
//
// Every case here pins a flag to the value the process uses when the document
// is silent, so the flag changes nothing. The defaults live below the CLI —
// wire.DefaultQueueGroup owns the queue group and PoolConfig.fill owns the
// pool sizes, both deliberately, so that every config source inherits them —
// which is exactly why comparing against the document's raw zero was wrong.
func TestFileSourceGuardAllowsSettingsThatDropNothing(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		cmd  GatewayCmd
	}{
		{
			"queue group pinned to what an unscoped gateway computes",
			`{"servers":{}}`,
			GatewayCmd{QueueGroup: "mcpgw"},
		},
		{
			"queue group pinned to what a scoped gateway computes",
			`{"servers":{},"nats":{"tenant":"acme","user":"u_9f3a"}}`,
			GatewayCmd{QueueGroup: "mcpgw.acme.u_9f3a"},
		},
		{
			"pool sizes pinned to the pool's own defaults",
			`{"servers":{}}`,
			GatewayCmd{
				PoolMaxConcurrent: 32, PoolMaxProcsPerTenant: 16,
				PoolIdleTTL: 5 * time.Minute, PoolMaxLifetime: time.Hour,
			},
		},
		{
			// A pool flag <= 0 is not a request for a smaller pool: fill()
			// substitutes the default for it, so it asks for exactly what the
			// silent document already gets. The guard reads > 0 rather than
			// != 0 for that reason, and nothing else pins the difference.
			"pool sizes given as zero and negative",
			`{"servers":{}}`,
			GatewayCmd{
				PoolMaxConcurrent: -1, PoolMaxProcsPerTenant: 0,
				PoolIdleTTL: -time.Second, PoolMaxLifetime: 0,
			},
		},
		{
			// Read only when claim-check is on, so with no block they are inert.
			"claim sizing with claim-check absent from the document",
			`{"servers":{}}`,
			GatewayCmd{ClaimMaxAge: 10 * time.Minute, ClaimMaxBytes: 1 << 20},
		},
		{
			// An unsized claimCheck block gets wire's substituted limits, so a
			// chart materializing those numbers into env agrees with the
			// gateway and must not be refused for saying so out loud.
			"claim sizing pinned to the limits an unsized block receives",
			`{"claimCheck":{},"servers":{}}`,
			GatewayCmd{ClaimMaxAge: 5 * time.Minute, ClaimMaxBytes: 1 << 30},
		},
		{
			// As with the pool, a sizing flag <= 0 is not a request for a
			// zero-length TTL: FilledClaimLimits substitutes the default, so it
			// asks for exactly what the unsized block already gets. The guard
			// reads > 0 for that reason, and nothing else pins the difference.
			"claim sizing given as zero and negative",
			`{"claimCheck":{"maxAge":"9m","maxBytes":5},"servers":{}}`,
			GatewayCmd{ClaimMaxAge: -time.Second, ClaimMaxBytes: 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := tt.cmd
			cmd.Config = writeConfig(t, tt.doc)
			_, err := cmd.bootParams(sourceFile)
			assert.NoError(t, err, "a flag that changes nothing must not refuse the boot")
		})
	}
}

// TestBuildBackendRefusesAPerUserServerKeyedWithoutAUser closes the factory
// half of the unattributed-credential refusal.
//
// proxy.Check refuses an unattributed caller on a per-user server, but the
// pool factory runs later and re-reads the config, so a reload that flips a
// server shared -> per-user mid-request reaches here with a key minted under
// the old mode. The generation comparison further down does reject it today,
// because a resolver-less key carries CredVersion 0 and globalGen starts at
// 1 — but that is an accident of numbering, and it also rejects only AFTER
// the resolver has been asked for "_", which runs an exec helper's side
// effects under an identity nothing will accept.
func TestBuildBackendRefusesAPerUserServerKeyedWithoutAUser(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "helper-ran")
	helper := filepath.Join(dir, "helper.sh")
	require.NoError(t, os.WriteFile(helper,
		[]byte("#!/bin/sh\ntouch \"$MARKER\"\necho '{\"headers\":{\"a\":\"b\"}}'\n"), 0o755))

	resolver := cred.Cached(&cred.Exec{
		Command: helper, Env: map[string]string{"MARKER": marker}, Timeout: 10 * time.Second,
	}, 0)

	srv := config.Server{
		Transport: "stdio", Command: "true",
		Auth: &config.Auth{Mode: "exec", Command: helper, Scope: "user"},
	}
	require.True(t, srv.Auth.PerUser(), "fixture must be a per-user server")

	// CredSet empty: the shape a pre-reload key has.
	_, err := buildBackend(
		backend.Key{Server: "aws", Tenant: "acme"},
		srv, resolver, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "per user")
	assert.ErrorIs(t, err, cred.ErrIdentityRequired,
		"the proxy maps this to -32014 by the sentinel, not by reading the message")
	_, statErr := os.Stat(marker)
	assert.True(t, os.IsNotExist(statErr),
		"the resolver must not be asked for the placeholder identity at all")
}

// TestTokenSourceDoesNotEchoTheResolverDetail covers the third resolve site,
// and the only one on the request path.
//
// The proxy's resolve and the pool factory's both suppress the resolver's own
// error text. credTokenSource did not — and an HTTP backend answering 401
// makes doWithAuthRetry invalidate and resolve AGAIN, so this path runs at
// exactly the moment the credential source is failing and saying why. From
// there the text is wrapped by newRequest, turned into a -32603 by
// roundTripOnce, and forwarded to the caller verbatim, because forwarding a
// backend's JSON-RPC error unchanged is what the proxy is for.
func TestTokenSourceDoesNotEchoTheResolverDetail(t *testing.T) {
	const secret = "sts assume-role arn:aws:iam::918273:role/prod failed reading " +
		"/var/run/secrets/acme/u1.jwt (token AKIAWOULDBEBAD)"

	dir := t.TempDir()
	helper := filepath.Join(dir, "helper.sh")
	require.NoError(t, os.WriteFile(helper,
		[]byte("#!/bin/sh\necho '"+secret+"' >&2\nexit 1\n"), 0o755))

	resolver := cred.Cached(&cred.Exec{Command: helper, Timeout: 10 * time.Second}, 0)
	ts := &credTokenSource{
		resolver: resolver, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		tenant: "acme", user: "u1", server: "aws",
	}

	_, err := ts.Headers(context.Background())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "AKIAWOULDBEBAD", "the helper's stderr reached the caller")
	assert.NotContains(t, err.Error(), "/var/run/secrets", "a pod path reached the caller")
	assert.NotContains(t, err.Error(), "arn:aws:iam", "an internal identity reached the caller")
	assert.Contains(t, err.Error(), "gateway ref ", "the operator needs a handle to correlate")
}
