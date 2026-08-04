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

package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/proxy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakemcp.EnvFlag) == "1" {
		fakemcp.Main()
		return
	}
	os.Exit(m.Run())
}

func srv(command string, args ...string) config.Server {
	return config.Server{
		Protocol:  mcpspec.ProtocolVersion,
		Transport: "stdio",
		Command:   command,
		Args:      args,
	}
}

func cfg(servers map[string]config.Server) *config.Config {
	return &config.Config{Servers: servers}
}

func TestDiffServers(t *testing.T) {
	base := cfg(map[string]config.Server{
		"a": srv("cmd-a"),
		"b": srv("cmd-b"),
	})
	tests := []struct {
		name              string
		old, next         *config.Config
		add, rem, changed []string
	}{
		{"first apply is all added", nil, base, []string{"a", "b"}, nil, nil},
		{"no change", base, cfg(map[string]config.Server{"a": srv("cmd-a"), "b": srv("cmd-b")}), nil, nil, nil},
		{"add one", base, cfg(map[string]config.Server{"a": srv("cmd-a"), "b": srv("cmd-b"), "c": srv("cmd-c")}), []string{"c"}, nil, nil},
		{"remove one", base, cfg(map[string]config.Server{"a": srv("cmd-a")}), nil, []string{"b"}, nil},
		{"env change", base, cfg(map[string]config.Server{
			"a": {Protocol: mcpspec.ProtocolVersion, Transport: "stdio", Command: "cmd-a", Env: map[string]string{"K": "v"}},
			"b": srv("cmd-b"),
		}), nil, nil, []string{"a"}},
		{"arg change", base, cfg(map[string]config.Server{"a": srv("cmd-a", "--flag"), "b": srv("cmd-b")}), nil, nil, []string{"a"}},
		{"remove all", base, cfg(map[string]config.Server{}), nil, []string{"a", "b"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := diffServers(tt.old, tt.next)
			assert.Equal(t, tt.add, d.Added, "added")
			assert.Equal(t, tt.rem, d.Removed, "removed")
			assert.Equal(t, tt.changed, d.Changed, "changed")
		})
	}
}

// stack wires embedded NATS + pool + proxy + wire + reconciler, the way the
// gateway command does, with the pool factory reading through the reconciler.
type stack struct {
	nc   *nats.Conn
	rec  *Reconciler
	ws   *wire.Server
	pool *backend.Pool
}

func newStack(t *testing.T, log *slog.Logger) *stack {
	t.Helper()
	nc, _ := natstest.Run(t, nil)

	st := &stack{nc: nc}
	st.pool = backend.NewPool(backend.PoolConfig{}, func(key backend.Key) (backend.Backend, error) {
		cur := st.rec.Current()
		sc := cur.Servers[key.Server]
		env := map[string]string{fakemcp.EnvFlag: "1"}
		for k, v := range sc.Env {
			env[k] = v
		}
		return &backend.StdioBackend{Command: sc.Command, Args: sc.Args, Env: env}, nil
	}, nil)
	t.Cleanup(st.pool.Shutdown)

	ws, err := wire.Serve(nc, wire.ServerConfig{KeepAlive: 50 * time.Millisecond}, proxy.New(st.pool, nil).Handler())
	require.NoError(t, err)
	st.ws = ws
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})
	st.rec = New(ws, st.pool, log)
	return st
}

// logCapture records what an operator would actually see, so a test can assert
// on a warning that has no other observable effect.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *logCapture) WithGroup(string) slog.Handler { return c }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

// warns renders each warn-or-worse record as message plus attributes — the
// whole line, because the field names the warning carries are half of what
// makes it actionable.
func (c *logCapture) warns() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, r := range c.records {
		if r.Level < slog.LevelWarn {
			continue
		}
		line := r.Message
		r.Attrs(func(a slog.Attr) bool {
			line += " " + a.String()
			return true
		})
		out = append(out, line)
	}
	return out
}

// warnsAbout narrows to the warnings whose line mentions sub, so a test about
// one warning is not disturbed by another the same apply emits.
func (c *logCapture) warnsAbout(sub string) []string {
	var out []string
	for _, line := range c.warns() {
		if strings.Contains(line, sub) {
			out = append(out, line)
		}
	}
	return out
}

// evictCall is one thing an apply asked of the pool. windowed separates the
// rollback's bounded eviction from the unfiltered one an adopted revision does.
type evictCall struct {
	server   string
	windowed bool
}

// recordingPool is a Pool that only remembers what it was asked to do. A live
// pool answers "did this apply evict anything?" only through side effects —
// a respawned pid, a killed call — which cannot tell an eviction that matched
// nothing from one that never happened.
type recordingPool struct {
	mu     sync.Mutex
	got    []evictCall
	onCall func()
}

func (p *recordingPool) EvictServer(server string) {
	p.record(evictCall{server: server})
}

func (p *recordingPool) EvictServerSince(server string, _ time.Time) {
	p.record(evictCall{server: server, windowed: true})
}

func (p *recordingPool) record(c evictCall) {
	p.mu.Lock()
	p.got = append(p.got, c)
	hook := p.onCall
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (p *recordingPool) calls() []evictCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]evictCall(nil), p.got...)
}

func (p *recordingPool) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.got = nil
}

// newRecordingStack is newStack without the backends: a real wire server,
// because SetServers is what an apply succeeds or fails at, and a pool that
// records rather than spawns. No request is issued against it.
func newRecordingStack(t *testing.T, log *slog.Logger) (*Reconciler, *recordingPool) {
	t.Helper()
	nc, _ := natstest.Run(t, nil)
	pool := backend.NewPool(backend.PoolConfig{}, func(backend.Key) (backend.Backend, error) {
		return nil, errors.New("this stack serves no requests")
	}, nil)
	t.Cleanup(pool.Shutdown)
	ws, err := wire.Serve(nc, wire.ServerConfig{}, proxy.New(pool, nil).Handler())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})
	rp := &recordingPool{}
	return New(ws, rp, log), rp
}

func (st *stack) call(t *testing.T, tenant, serverName, tool string) wire.Frame {
	t.Helper()
	c, err := wire.NewClient(st.nc, wire.ClientConfig{Tenant: tenant, Inactivity: 3 * time.Second})
	require.NoError(t, err)
	params, _ := json.Marshal(map[string]any{
		"name": tool, "arguments": map[string]any{},
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

// stream issues a call and hands back its frames without waiting for them, so a
// test can hold a request open across a reload and see what the reload does to
// it. The inactivity window is long: only the gateway should be able to end
// this stream.
func (st *stack) stream(t *testing.T, tenant, serverName, tool string) <-chan wire.Frame {
	t.Helper()
	c, err := wire.NewClient(st.nc, wire.ClientConfig{Tenant: tenant, Inactivity: 30 * time.Second})
	require.NoError(t, err)
	params, _ := json.Marshal(map[string]any{
		"name": tool, "arguments": map[string]any{},
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
	out := make(chan wire.Frame, 8)
	go func() {
		defer close(out)
		for f := range s.C {
			out <- f
		}
	}()
	// The request has to be AT the backend before a reload can be said to have
	// spared it; the wedge tool never answers, so there is no frame to wait on.
	time.Sleep(250 * time.Millisecond)
	return out
}

func pidOf(t *testing.T, f wire.Frame) float64 {
	t.Helper()
	require.Equal(t, wire.FrameEnd, f.Kind)
	m, err := jsonrpc.Decode(f.Body)
	require.NoError(t, err)
	var r struct {
		PID float64 `json:"pid"`
	}
	require.NoError(t, json.Unmarshal(m.Result, &r))
	return r.PID
}

func TestApplyAddChangeRemove(t *testing.T) {
	st := newStack(t, nil)

	// Boot with server "a".
	d, err := st.rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, d.Added)

	pidA := pidOf(t, st.call(t, "acme", "a", "echo"))

	// Reload: change a's env (forces respawn) and add b.
	d, err = st.rec.Apply(cfg(map[string]config.Server{
		"a": fakeSrv(map[string]string{"EXTRA": "1"}),
		"b": fakeSrv(nil),
	}))
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, d.Added)
	assert.Equal(t, []string{"a"}, d.Changed)

	// b serves now; a respawned with a new pid.
	assert.Equal(t, wire.FrameEnd, st.call(t, "acme", "b", "echo").Kind)
	assert.NotEqual(t, pidA, pidOf(t, st.call(t, "acme", "a", "echo")), "changed server must respawn")

	// Reload: drop a entirely.
	d, err = st.rec.Apply(cfg(map[string]config.Server{"b": fakeSrv(nil)}))
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, d.Removed)

	start := time.Now()
	f := st.call(t, "acme", "a", "echo")
	require.NotNil(t, f.Err)
	assert.Equal(t, wire.ErrCodeNoGateway, f.Err.Code)
	assert.Less(t, time.Since(start), 2*time.Second, "removed server must fail fast")
}

func TestApplyUnchangedIsNoop(t *testing.T) {
	st := newStack(t, nil)
	c := cfg(map[string]config.Server{"a": fakeSrv(nil)})
	_, err := st.rec.Apply(c)
	require.NoError(t, err)
	pid := pidOf(t, st.call(t, "acme", "a", "echo"))

	// Re-apply an identical config: no eviction, same process.
	d, err := st.rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)
	assert.True(t, d.Empty())
	assert.Equal(t, pid, pidOf(t, st.call(t, "acme", "a", "echo")), "no-op reload must not respawn")
}

// Editing the file's nats block and reloading changes nothing: the connection
// and the wire's scope were fixed before the Reconciler existed. The warning is
// the only signal there is — the diff covers servers only, so an operator who
// drops nats.tenant and HUPs otherwise sees "config unchanged" while the
// gateway goes on serving every tenant.
func TestApplyWarnsWhenNatsBlockDriftsFromBoot(t *testing.T) {
	logs := &logCapture{}
	st := newStack(t, slog.New(logs))

	boot := config.NATS{Tenant: "acme", QueueGroup: "gw"}
	_, err := st.rec.Apply(&config.Config{NATS: boot, Servers: map[string]config.Server{"a": fakeSrv(nil)}})
	require.NoError(t, err)
	require.Empty(t, logs.warns(), "the first revision is what the process booted on")

	// A reload that leaves the block alone is not drift, however many times it
	// happens.
	_, err = st.rec.Apply(&config.Config{
		NATS:    boot,
		Servers: map[string]config.Server{"a": fakeSrv(nil), "b": fakeSrv(nil)},
	})
	require.NoError(t, err)
	require.Empty(t, logs.warns(), "an unchanged nats block must not warn on every reload")

	// Drop the tenant. Nothing in the server set moved, so this warning is the
	// only signal the operator gets that the scope did not narrow.
	d, err := st.rec.Apply(&config.Config{
		NATS:    config.NATS{QueueGroup: "gw"},
		Servers: map[string]config.Server{"a": fakeSrv(nil), "b": fakeSrv(nil)},
	})
	require.NoError(t, err)
	assert.True(t, d.Empty(), "the server set is all the diff covers, and it did not change")
	got := logs.warns()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "tenant")
	assert.NotContains(t, got[0], "queueGroup", "only the fields that actually moved")

	// A later, unrelated edit reports the drift again: it is measured against
	// what is RUNNING, so a second revision must not make it look settled.
	_, err = st.rec.Apply(&config.Config{
		NATS:    config.NATS{QueueGroup: "gw"},
		Servers: map[string]config.Server{"a": fakeSrv(nil)},
	})
	require.NoError(t, err)
	assert.Len(t, logs.warns(), 2, "the block is still diverged from the running connection")
}

// nats.url carries its password in the userinfo form and credsFile names a path
// worth nothing to an attacker but everything to a log scraper, so the warning
// names the fields that moved and never their values.
func TestNatsDriftWarningNamesFieldsNotValues(t *testing.T) {
	logs := &logCapture{}
	st := newStack(t, slog.New(logs))

	_, err := st.rec.Apply(&config.Config{NATS: config.NATS{URL: "nats://gw:s3cret@old:4222"}})
	require.NoError(t, err)
	_, err = st.rec.Apply(&config.Config{NATS: config.NATS{URL: "nats://gw:rotated@new:4222"}})
	require.NoError(t, err)

	got := logs.warns()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "url")
	assert.NotContains(t, got[0], "s3cret")
	assert.NotContains(t, got[0], "rotated")
}

// config.NATS is all plain-tagged strings today, so marshalling emits every key
// on both sides. The day a field is tagged omitempty it drops out of whichever
// side it is empty on, and comparing over the boot side's keys alone would miss
// it in exactly the direction that matters: booted without the field, reloaded
// with it set — the narrowing edit. The warning would still fire (the struct
// compare catches it) while naming nothing, so the comparison covers the union.
func TestDriftFieldsCoversKeysPresentOnOneSideOnly(t *testing.T) {
	boot := map[string]json.RawMessage{"same": []byte(`"x"`), "bootOnly": []byte(`"y"`)}
	next := map[string]json.RawMessage{"same": []byte(`"x"`), "nextOnly": []byte(`"z"`)}
	assert.Equal(t, []string{"bootOnly", "nextOnly"}, driftFields(boot, next))
}

// A rolled-back revision must not leave its backends behind, and must not take
// anything else with it. Current() publishes next BEFORE the wire update, so
// for as long as the update runs the pool factory resolves definitions out of a
// config that may never become live; a backend spawned in that window outlives
// the rollback under the same (server, tenant) key, and no later apply evicts
// it — once prev is live again its definition matches, so the diff is empty.
// Apply therefore evicts on the failure path, bounded to the window.
//
// The bound is what this test pins: a backend that predates the failed apply
// belongs to prev, which is live again, so it must survive. Killing it would
// mean a revision that never took effect could still fail live calls. The
// other half — that a backend born INSIDE the window is dropped — is the
// pool's contract and is pinned there (TestEvictServerSince); reaching it from
// here would need a hook to hold Apply open mid-window, and there is none.
func TestApplyRollbackKeepsWhatThePreviousConfigOwns(t *testing.T) {
	st := newStack(t, nil)

	_, err := st.rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)
	pidA := pidOf(t, st.call(t, "acme", "a", "echo"))

	// A revision that redefines a and carries a server the wire cannot bind:
	// the wire update fails, so nothing of it ever becomes live.
	_, err = st.rec.Apply(cfg(map[string]config.Server{
		"a":        fakeSrv(map[string]string{"EXTRA": "1"}),
		"bad name": fakeSrv(nil),
	}))
	require.Error(t, err)

	cur := st.rec.Current()
	require.NotNil(t, cur)
	assert.Equal(t, []string{"a"}, cur.ServerNames(), "the previous config must be live again")
	assert.Nil(t, cur.Servers["a"].Env, "the rolled-back definition must not survive")

	assert.Equal(t, pidA, pidOf(t, st.call(t, "acme", "a", "echo")),
		"a revision that never took effect must not respawn a backend prev still owns")

	// And the gateway is still reconcilable afterwards: the same change without
	// the unbindable name applies, and NOW a is expected to respawn.
	_, err = st.rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(map[string]string{"EXTRA": "1"})}))
	require.NoError(t, err)
	assert.NotEqual(t, pidA, pidOf(t, st.call(t, "acme", "a", "echo")),
		"a changed server must respawn once its revision really is live")
}

// A nil revision is the one the package doc invites an embedder to produce:
// pkg/configsource advertises a hand-written fetch as an extension point, and
// (nil, nil) is what one returns to empty the gateway. It has to reconcile like
// any other revision — and leave Current dereferenceable, because the pool
// factory reaches through it on every spawn.
func TestApplyNilRevisionRemovesEveryServer(t *testing.T) {
	st := newStack(t, nil)

	_, err := st.rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)
	require.Equal(t, wire.FrameEnd, st.call(t, "acme", "a", "echo").Kind)

	d, err := st.rec.Apply(nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, d.Removed, "a nil revision removes every server")

	cur := st.rec.Current()
	require.NotNil(t, cur, "the pool factory dereferences whatever Current returns")
	assert.Empty(t, cur.ServerNames())

	f := st.call(t, "acme", "a", "echo")
	require.NotNil(t, f.Err)
	assert.Equal(t, wire.ErrCodeNoGateway, f.Err.Code, "the removed server stops answering")

	// And the gateway reconciles back out of it.
	_, err = st.rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)
	assert.Equal(t, wire.FrameEnd, st.call(t, "acme", "a", "echo").Kind)
}

// A rejected revision must not reach into calls the previous config is serving
// right now. The rollback drops what the revision could have started, and a
// backend that predates it started under the definition that is live again —
// so a call on it is doing exactly what the operator asked for, and a revision
// that never took effect has no business ending it.
func TestApplyRollbackLeavesInFlightWorkAlone(t *testing.T) {
	st := newStack(t, nil)

	_, err := st.rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)
	pidA := pidOf(t, st.call(t, "acme", "a", "echo"))

	// wedge never answers, so the only thing that can end this stream early is
	// the gateway. Keepalives hold it open otherwise.
	inflight := st.stream(t, "acme", "a", "wedge")

	// A revision that redefines a — and that the wire refuses.
	_, err = st.rec.Apply(cfg(map[string]config.Server{
		"a":        fakeSrv(map[string]string{"EXTRA": "1"}),
		"bad name": fakeSrv(nil),
	}))
	require.Error(t, err)

	select {
	case f := <-inflight:
		t.Fatalf("a rejected revision ended a call the live config was serving: kind=%v err=%+v", f.Kind, f.Err)
	case <-time.After(750 * time.Millisecond):
	}
	assert.Equal(t, pidA, pidOf(t, st.call(t, "acme", "a", "echo")),
		"and the backend serving it is the same one")
}

// Run hands a failed revision back to its source to re-deliver, so a config the
// wire refuses arrives again on every trigger for as long as the operator
// leaves it in place. Each attempt that publishes opens a window a request can
// pool a backend in, and the rollback then kills it — which at one attempt per
// tick is a backend-churn loop, across every tenant, for a revision that never
// served anything. Only the first attempt pays it.
func TestRepeatedFailingRevisionEvictsOnce(t *testing.T) {
	logs := &logCapture{}
	rec, pool := newRecordingStack(t, slog.New(logs))

	_, err := rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)
	pool.reset()

	bad := cfg(map[string]config.Server{
		"a":        fakeSrv(map[string]string{"EXTRA": "1"}),
		"bad name": fakeSrv(nil),
	})
	for i := 0; i < 20; i++ {
		_, err := rec.Apply(bad)
		require.Error(t, err, "every attempt still reaches the wire, so a transient refusal recovers")
	}
	assert.Equal(t, []evictCall{
		{server: "a", windowed: true},
		{server: "bad name", windowed: true},
	}, pool.calls(), "only the attempt that published has anything to undo")

	// And the operator can tell which attempt cost them a backend. The caller's
	// own failure line knows only that the apply returned an error; "the
	// previous config keeps serving" is the whole truth about the wire and none
	// of it about the pools.
	rolled := logs.warnsAbout("rolled back")
	require.Len(t, rolled, 1, "the attempts that touched nothing say nothing")
	assert.Contains(t, rolled[0], "a")
	assert.Contains(t, rolled[0], "bad name")

	// Still retryable: the moment the wire can take the revision, it applies —
	// and now the changed server really does drop its backends.
	pool.reset()
	_, err = rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(map[string]string{"EXTRA": "1"})}))
	require.NoError(t, err)
	assert.Equal(t, []evictCall{{server: "a"}}, pool.calls())
}

// An apply's eviction runs under the same lock as the state change that
// decided it, and that ordering is the point rather than an accident.
//
// It was split off once so a queued reload would not wait out the previous
// one's terminate grace. That split published inside the critical section and
// evicted outside it, so a later revision could go live and then have its
// backends killed by the earlier apply's eviction — with no window to bound it
// for an adopted revision, which evicts unconditionally. Run applies serially
// in one goroutine, so the latency it bought is a shape the gateway never
// produces, while the race it opened is exactly what concurrent Apply is
// supposed to be safe for.
func TestEvictionRunsBeforeTheNextApplyCanPublish(t *testing.T) {
	rec, pool := newRecordingStack(t, nil)

	_, err := rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(nil)}))
	require.NoError(t, err)

	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	pool.onCall = func() {
		once.Do(func() { close(entered) })
		<-release
	}

	go func() { _, _ = rec.Apply(cfg(map[string]config.Server{"a": fakeSrv(map[string]string{"EXTRA": "1"})})) }()
	<-entered

	published := make(chan struct{})
	go func() {
		defer close(published)
		_, _ = rec.Apply(cfg(map[string]config.Server{
			"a": fakeSrv(map[string]string{"EXTRA": "1"}),
			"b": fakeSrv(nil),
		}))
	}()

	// Nothing of the second apply is visible while the first is still inside
	// the pool: a revision that went live here would be one the eviction still
	// running behind it could reach.
	select {
	case <-published:
		t.Fatal("a revision was published while a previous apply's eviction was still running")
	case <-time.After(200 * time.Millisecond):
	}
	assert.Len(t, rec.Current().Servers, 1)

	close(release)
	<-published
	assert.Len(t, rec.Current().Servers, 2)
}

// The drift warning is about an EDIT the operator made, and #24 made a refused
// revision arrive again on every source trigger. Warning from the top of an
// apply therefore reported the same unapplied block once per tick, forever;
// warning only for a revision the gateway adopts keeps it to one line per edit.
func TestNatsDriftWarnsPerAdoptedEditNotPerRetry(t *testing.T) {
	logs := &logCapture{}
	st := newStack(t, slog.New(logs))
	const drift = "nats block"

	_, err := st.rec.Apply(&config.Config{
		NATS:    config.NATS{Tenant: "acme"},
		Servers: map[string]config.Server{"a": fakeSrv(nil)},
	})
	require.NoError(t, err)
	require.Empty(t, logs.warnsAbout(drift), "the first revision is what the process booted on")

	// The operator moves the nats block in a document the wire cannot bind.
	bad := &config.Config{
		NATS:    config.NATS{Tenant: "other"},
		Servers: map[string]config.Server{"a": fakeSrv(nil), "bad name": fakeSrv(nil)},
	}
	for i := 0; i < 12; i++ {
		_, err := st.rec.Apply(bad)
		require.Error(t, err)
	}
	assert.Empty(t, logs.warnsAbout(drift),
		"a revision the gateway never adopted asserts nothing about the block it is running")

	// The same edit, now bindable: adopted, and reported once.
	_, err = st.rec.Apply(&config.Config{
		NATS:    config.NATS{Tenant: "other"},
		Servers: map[string]config.Server{"a": fakeSrv(nil)},
	})
	require.NoError(t, err)
	got := logs.warnsAbout(drift)
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "tenant")
}

func fakeSrv(extraEnv map[string]string) config.Server {
	return config.Server{
		Protocol:  mcpspec.ProtocolVersion,
		Transport: "stdio",
		Command:   os.Args[0],
		Env:       extraEnv,
	}
}
