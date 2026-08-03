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

package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
)

// TestMain re-execs the test binary as the fake MCP server when spawned by a
// StdioBackend below — real subprocess, zero extra build steps.
func TestMain(m *testing.M) {
	if os.Getenv(fakemcp.EnvFlag) == "1" {
		fakemcp.Main()
		return
	}
	os.Exit(m.Run())
}

func fakeBackend(extraEnv map[string]string) *StdioBackend {
	env := map[string]string{fakemcp.EnvFlag: "1"}
	for k, v := range extraEnv {
		env[k] = v
	}
	return &StdioBackend{Command: os.Args[0], Env: env}
}

func newTestMux(t *testing.T) *Mux {
	t.Helper()
	conn, err := fakeBackend(nil).Connect(context.Background())
	require.NoError(t, err)
	m := NewMux(conn, nil)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func callTool(id, tool, args string, progressToken string) *jsonrpc.Message {
	params := map[string]any{"name": tool}
	if args != "" {
		params["arguments"] = json.RawMessage(args)
	}
	if progressToken != "" {
		params["_meta"] = map[string]any{"progressToken": progressToken}
	}
	raw, _ := json.Marshal(params)
	return jsonrpc.NewRequest(id, "tools/call", raw)
}

func TestStdioEcho(t *testing.T) {
	m := newTestMux(t)
	resp, err := m.Call(context.Background(), callTool("c1", "echo", `{"hello":"world"}`, ""), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	assert.JSONEq(t, `"c1"`, string(resp.ID), "caller id must be restored")
	assert.Contains(t, string(resp.Result), "hello")
}

// Only the Start failure cleaned up after itself. The three pipe setups above
// it returned straight out, leaving the scratch workdir behind — and the
// condition that fails a pipe is descriptor exhaustion, which is exactly the
// condition that repeats. A gateway under it leaked a directory per attempt,
// and the descriptors of whichever pipes had already succeeded.
func TestFailedConnectLeavesNothingBehind(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp) // os.MkdirTemp("", ...) lands here

	var lim syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim))
	// Descriptors this process already holds keep working; only new ones fail.
	// The window is closed on the next line either way.
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE,
		&syscall.Rlimit{Cur: 8, Max: lim.Max}))
	conn, err := fakeBackend(nil).Connect(context.Background())
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim))

	if err == nil {
		_ = conn.Close()
		t.Fatal("connect succeeded with no descriptors to spare; the test proved nothing")
	}
	entries, readErr := os.ReadDir(tmp)
	require.NoError(t, readErr)
	assert.Empty(t, entries, "the workdir outlived the failed connect")
}

// Close escalates to a process-GROUP kill, addressed by the leader's pid.
// Once that leader has been reaped the pid belongs to the kernel again, and
// signalling it then can reach a group that has nothing to do with us. The
// race cannot be closed from Go — the pid is freed inside os/exec's Wait, and
// nothing tells us so until Wait returns — but the branch where we already
// know the process is gone can be, and it is the branch the escalation
// reaches when a subprocess dies right as the grace period expires.
//
// The victim here stands in for whatever inherited the recycled pid.
func TestSignalGroupSkipsAProcessAlreadyKnownDead(t *testing.T) {
	victim := exec.Command("/bin/sleep", "30")
	victim.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, victim.Start())
	reaped := make(chan struct{})
	go func() { _ = victim.Wait(); close(reaped) }()
	t.Cleanup(func() {
		_ = syscall.Kill(-victim.Process.Pid, syscall.SIGKILL)
		<-reaped
	})

	c := &stdioConn{cmd: victim, dead: make(chan struct{})}
	close(c.dead) // waitLoop has seen the exit: this pid is no longer ours
	c.signalGroup(syscall.SIGKILL)

	select {
	case <-reaped:
		t.Fatal("signalGroup killed a process group its own subprocess no longer owned")
	case <-time.After(250 * time.Millisecond):
	}
}

func TestMuxConcurrentCallsCorrelate(t *testing.T) {
	m := newTestMux(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			arg := fmt.Sprintf(`{"n":%d}`, i)
			id := fmt.Sprintf("c%d", i)
			resp, err := m.Call(context.Background(), callTool(id, "echo", arg, ""), nil)
			require.NoError(t, err)
			assert.JSONEq(t, fmt.Sprintf("%q", id), string(resp.ID))
			assert.Contains(t, string(resp.Result), fmt.Sprintf(`\"n\":%d`, i),
				"response must belong to this caller's request")
		}(i)
	}
	wg.Wait()
}

func TestProgressTokenRoutingWithCollidingTokens(t *testing.T) {
	m := newTestMux(t)
	run := func(id string) []string {
		var tokens []string
		var mu sync.Mutex
		// Both callers deliberately pick the SAME token "tok": the mux must
		// still route each caller its own three notifications, restored.
		resp, err := m.Call(context.Background(), callTool(id, "slow", "", "tok"),
			func(n *jsonrpc.Message) {
				var p struct {
					ProgressToken string `json:"progressToken"`
				}
				_ = json.Unmarshal(n.Params, &p)
				mu.Lock()
				tokens = append(tokens, p.ProgressToken)
				mu.Unlock()
			})
		require.NoError(t, err)
		require.Nil(t, resp.Error)
		return tokens
	}

	var wg sync.WaitGroup
	results := make([][]string, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = run(fmt.Sprintf("p%d", i))
		}(i)
	}
	wg.Wait()

	for i, tokens := range results {
		assert.Len(t, tokens, 3, "caller %d must receive exactly its own 3 progress events", i)
		for _, tok := range tokens {
			assert.Equal(t, "tok", tok, "caller's original token must be restored")
		}
	}
}

// TestProgressTokenRewriteLeavesCaseVariantSibling records what the test
// above depends on, and what defends it.
//
// The rewrite gives each caller a mux-unique token so two of them cannot
// collide — that is the whole point of the test above. It swaps the exact
// key and has no opinion about a "ProgressToken" sibling, which rides through
// untouched. A case-folding backend binds THAT one instead, echoes it on its
// progress notifications, and the mux routes them by token to whichever call
// registered it. Mux tokens are a counter, not a secret, so a caller can name
// another caller's.
//
// Nothing here is wrong: this function is far past the point where a request
// can still be refused. pkg/proxy.Check is that point, and it refuses the
// collision — mcpspec.metaKeysRead names progressToken for exactly this
// reason. If that ever goes away, this test is the record of what it held up.
func TestProgressTokenRewriteLeavesCaseVariantSibling(t *testing.T) {
	orig, out, ok := rewriteProgressToken(
		json.RawMessage(`{"name":"x","_meta":{"progressToken":"mine","ProgressToken":"gt7"}}`),
		"gt99",
	)
	require.True(t, ok)
	assert.JSONEq(t, `"mine"`, string(orig), "the caller's own token is what gets restored later")
	assert.Contains(t, string(out), `"progressToken":"gt99"`, "the exact key is rewritten")
	assert.Contains(t, string(out), `"ProgressToken":"gt7"`,
		"the sibling survives, which is why the request must be refused upstream")
}

func TestCrashFailsAllInFlight(t *testing.T) {
	m := newTestMux(t)

	// One call wedges, another crashes the process; both must fail, not hang.
	var wg sync.WaitGroup
	wg.Add(2)
	errs := make([]error, 2)
	go func() {
		defer wg.Done()
		_, errs[0] = m.Call(context.Background(), callTool("w1", "wedge", "", ""), nil)
	}()
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond) // let wedge get in flight first
		_, errs[1] = m.Call(context.Background(), callTool("k1", "crash", "", ""), nil)
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight calls hung after backend crash")
	}
	assert.Error(t, errs[0])
	assert.Error(t, errs[1])
	assert.True(t, m.Dead())
}

func TestServerInitiatedRequestRejectedWithoutWedging(t *testing.T) {
	m := newTestMux(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := m.Call(ctx, callTool("s1", "sample", "", ""), nil)
	require.NoError(t, err, "the fake server blocks until its sampling request is answered — a hang here means the mux never rejected it")
	require.Nil(t, resp.Error)
	assert.Contains(t, string(resp.Result), fmt.Sprintf("sample-rejected:%d", jsonrpc.CodeMethodNotFound))
}

func TestCancelSendsCancelledNotification(t *testing.T) {
	m := newTestMux(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	_, err := m.Call(ctx, callTool("z1", "wedge", "", ""), nil)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, m.Dead(), "cancelling one call must not kill the connection")

	// The connection must still serve other calls.
	resp, err := m.Call(context.Background(), callTool("z2", "echo", `{}`, ""), nil)
	require.NoError(t, err)
	require.Nil(t, resp.Error)
}

func poolFactory(key Key) (Backend, error) {
	return fakeBackend(nil), nil
}

func TestPoolTenantIsolation(t *testing.T) {
	p := NewPool(PoolConfig{}, poolFactory, nil)
	t.Cleanup(p.Shutdown)

	pidOf := func(tenant string) float64 {
		mux, release, err := p.Get(context.Background(), Key{Server: "s", Tenant: tenant})
		require.NoError(t, err)
		defer release()
		resp, err := mux.Call(context.Background(), callTool("i-"+tenant, "echo", `{}`, ""), nil)
		require.NoError(t, err)
		var r struct {
			PID float64 `json:"pid"`
		}
		require.NoError(t, json.Unmarshal(resp.Result, &r))
		return r.PID
	}

	pidA1 := pidOf("acme")
	pidA2 := pidOf("acme")
	pidB := pidOf("bravo")

	assert.Equal(t, pidA1, pidA2, "same tenant must reuse its process")
	assert.NotEqual(t, pidA1, pidB, "different tenants must never share a process")
}

func TestEvictServer(t *testing.T) {
	p := NewPool(PoolConfig{}, poolFactory, nil)
	t.Cleanup(p.Shutdown)

	pidOf := func(server, tenant string) float64 {
		mux, release, err := p.Get(context.Background(), Key{Server: server, Tenant: tenant})
		require.NoError(t, err)
		defer release()
		resp, err := mux.Call(context.Background(), callTool("i", "echo", `{}`, ""), nil)
		require.NoError(t, err)
		var r struct {
			PID float64 `json:"pid"`
		}
		require.NoError(t, json.Unmarshal(resp.Result, &r))
		return r.PID
	}

	// Two servers, one with two tenants: evicting "a" must drop both of a's
	// processes and leave b's untouched.
	a1 := pidOf("a", "acme")
	a2 := pidOf("a", "bravo")
	b := pidOf("b", "acme")

	// Hold a live mux for server a to prove in-flight calls fail on evict.
	mux, release, err := p.Get(context.Background(), Key{Server: "a", Tenant: "acme"})
	require.NoError(t, err)
	defer release()

	p.EvictServer("a")

	// In-flight mux is now closed.
	_, err = mux.Call(context.Background(), callTool("x", "echo", `{}`, ""), nil)
	require.Error(t, err, "evicted server's live mux must fail")

	// b is untouched (same pid, same process).
	assert.Equal(t, b, pidOf("b", "acme"), "unrelated server must survive eviction")

	// a respawns fresh for both tenants (new pids).
	assert.NotEqual(t, a1, pidOf("a", "acme"), "evicted server must respawn")
	assert.NotEqual(t, a2, pidOf("a", "bravo"), "evicted server must respawn per tenant")
}

// EvictServerSince is reconcile.Apply's rollback eviction. A failed apply has to
// drop backends built from the revision it is undoing, but only those: the
// previous config is live again and its definition never changed, so anything
// older is serving exactly what it should. The cutoff is what separates them.
func TestEvictServerSince(t *testing.T) {
	p := NewPool(PoolConfig{}, poolFactory, nil)
	t.Cleanup(p.Shutdown)

	pidOf := func(server, tenant string) float64 {
		mux, release, err := p.Get(context.Background(), Key{Server: server, Tenant: tenant})
		require.NoError(t, err)
		defer release()
		resp, err := mux.Call(context.Background(), callTool("i", "echo", `{}`, ""), nil)
		require.NoError(t, err)
		var r struct {
			PID float64 `json:"pid"`
		}
		require.NoError(t, json.Unmarshal(resp.Result, &r))
		return r.PID
	}

	before := pidOf("a", "acme")
	other := pidOf("b", "acme")

	// Everything after here stands in for the window in which a failed
	// revision was briefly the live config.
	windowStart := time.Now()
	during := pidOf("a", "bravo")

	p.EvictServerSince("a", windowStart)

	assert.Equal(t, before, pidOf("a", "acme"),
		"a backend older than the window came from the config that is live again")
	assert.NotEqual(t, during, pidOf("a", "bravo"),
		"a backend born in the window may have come from the rolled-back revision")
	assert.Equal(t, other, pidOf("b", "acme"), "an unrelated server must be untouched")

	// The zero cutoff means everything, which is how EvictServer is built on it.
	p.EvictServerSince("a", time.Time{})
	assert.NotEqual(t, before, pidOf("a", "acme"), "the zero cutoff must spare nothing")
}

func TestPoolTenantQuota(t *testing.T) {
	p := NewPool(PoolConfig{MaxProcsPerTenant: 2}, poolFactory, nil)
	t.Cleanup(p.Shutdown)

	for i := 0; i < 2; i++ {
		_, release, err := p.Get(context.Background(), Key{Server: fmt.Sprintf("s%d", i), Tenant: "acme"})
		require.NoError(t, err)
		release()
	}
	_, _, err := p.Get(context.Background(), Key{Server: "s9", Tenant: "acme"})
	require.Error(t, err, "third live backend must exceed the tenant quota")
	assert.Contains(t, err.Error(), "max live backends")

	// Other tenants are unaffected.
	_, release, err := p.Get(context.Background(), Key{Server: "s0", Tenant: "bravo"})
	require.NoError(t, err)
	release()
}

// A new credential generation supersedes the entries keyed on the old one.
// Leaving them to the 5m IdleTTL is what turns a backend that keeps rejecting
// a caller's credentials into a tenant-wide outage: every request keys a
// fresh entry, the superseded ones count toward MaxProcsPerTenant, and once
// they fill it every OTHER server of that tenant is refused too.
func TestNewGenerationRetiresTheEntryItSupersedes(t *testing.T) {
	p := NewPool(PoolConfig{MaxProcsPerTenant: 2, IdleTTL: time.Hour}, poolFactory, nil)
	t.Cleanup(p.Shutdown)

	key := Key{Server: "s", Tenant: "acme", CredSet: "u1", CredVersion: 1}
	firstMux, release, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	release()

	for gen := 2; gen <= 8; gen++ {
		k := key
		k.CredVersion = gen
		_, release, err := p.Get(context.Background(), k)
		require.NoError(t, err, "generation %d must still get a backend", gen)
		release()
	}

	p.mu.Lock()
	live := len(p.entries)
	p.mu.Unlock()
	assert.Equal(t, 1, live, "only the newest generation may hold a pool entry")

	require.Eventually(t, firstMux.Dead, 10*time.Second, 10*time.Millisecond,
		"the superseded backend must be closed, not merely unmapped")

	// An unrelated server for the same tenant must still fit under the cap.
	_, release, err = p.Get(context.Background(), Key{Server: "other", Tenant: "acme"})
	require.NoError(t, err, "one server's credential churn must not lock out the tenant")
	release()
}

// TestRotationIsNotRefusedByTheCapItsPredecessorOccupies is the same failure
// as above met from the other side: not a tenant filling its cap with dead
// generations, but a tenant already at the cap for legitimate reasons trying
// to rotate one credential.
//
// Retiring after the spawn cannot help there, because the cap is measured
// BEFORE the spawn — so the entry that is about to be superseded is still
// counted, and the rotation is refused by the predecessor it was about to
// replace. Nothing recovers it until the IdleTTL reaps an entry that was
// already unreachable: the proxy resolves before it keys, so no later request
// can carry the old generation.
func TestRotationIsNotRefusedByTheCapItsPredecessorOccupies(t *testing.T) {
	p := NewPool(PoolConfig{MaxProcsPerTenant: 1, IdleTTL: time.Hour}, poolFactory, nil)
	t.Cleanup(p.Shutdown)

	key := Key{Server: "s", Tenant: "acme", CredSet: "u1", CredVersion: 1}
	_, release, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	release()

	rotated := key
	rotated.CredVersion = 2
	_, release, err = p.Get(context.Background(), rotated)
	require.NoError(t, err,
		"a rotation must not be refused by the cap its own superseded entry is holding")
	release()

	p.mu.Lock()
	live := len(p.entries)
	p.mu.Unlock()
	assert.Equal(t, 1, live, "the superseded entry must not have survived the rotation")
}

// gatedBackend holds Connect open until released, so every racing Get sits
// inside the window between the tenant check and the insert at the same time.
// Connect really does take that long in production — it spawns an npx or uvx
// subprocess — so this is the normal shape of the race, not a contrived one.
type gatedBackend struct {
	Backend
	arrived *atomic.Int32
	gate    <-chan struct{}
}

func (b *gatedBackend) Connect(ctx context.Context) (Conn, error) {
	b.arrived.Add(1)
	<-b.gate
	return b.Backend.Connect(ctx)
}

// MaxProcsPerTenant is the guard against a tenant's fan-out OOMing the host,
// so it has to hold against the fan-out itself. Per-user stdio servers make
// concurrent Gets on distinct keys for one tenant the normal traffic shape.
func TestTenantQuotaHoldsAgainstConcurrentGets(t *testing.T) {
	const (
		limit = 2
		burst = 8
	)
	var arrived, refused atomic.Int32
	gate := make(chan struct{})
	p := NewPool(PoolConfig{MaxProcsPerTenant: limit}, func(Key) (Backend, error) {
		return &gatedBackend{Backend: fakeBackend(nil), arrived: &arrived, gate: gate}, nil
	}, nil)
	t.Cleanup(p.Shutdown)

	var wg sync.WaitGroup
	releases := make(chan func(), burst)
	for i := range burst {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, release, err := p.Get(context.Background(), Key{
				Server: fmt.Sprintf("s%d", i), Tenant: "acme",
			})
			if err != nil {
				refused.Add(1)
				return
			}
			releases <- release
		}(i)
	}

	// Everyone has either been refused or is holding at the gate: the whole
	// burst is now past the check, which is the state the quota has to survive.
	require.Eventually(t, func() bool {
		return arrived.Load()+refused.Load() == burst
	}, 10*time.Second, time.Millisecond, "the burst never settled")
	close(gate)
	wg.Wait()
	close(releases)
	for release := range releases {
		release()
	}

	assert.LessOrEqual(t, arrived.Load(), int32(limit),
		"the tenant cap must be spent before the spawns happen, not counted after")
	p.mu.Lock()
	live := p.tenantCountLocked("acme")
	p.mu.Unlock()
	assert.LessOrEqual(t, live, limit, "a burst must not leave the tenant over its cap")
	assert.Positive(t, burst-int(refused.Load()), "the cap must not refuse everything either")
}

func TestPoolCircuitBreaker(t *testing.T) {
	badFactory := func(key Key) (Backend, error) {
		return &StdioBackend{Command: "/nonexistent/binary-xyz"}, nil
	}
	p := NewPool(PoolConfig{BreakerThreshold: 2, BreakerCooldown: time.Minute}, badFactory, nil)
	t.Cleanup(p.Shutdown)

	key := Key{Server: "s", Tenant: "acme"}
	for i := 0; i < 2; i++ {
		_, _, err := p.Get(context.Background(), key)
		require.Error(t, err)
	}
	_, _, err := p.Get(context.Background(), key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circuit open", "after threshold failures the breaker must fail fast without spawning")
}

// slowBackend stands in for the spawn a real backend is: a legacy backend
// runs the initialize handshake inside Connect, and a cold `npx` server can
// take seconds to answer it.
//
// spawns counts Connect ATTEMPTS, not successes, and the difference is the
// whole point: a spawn that was killed and re-run leaves one success behind
// just like a spawn that was inherited, so counting successes cannot tell the
// two apart. Counting entries can.
type slowBackend struct {
	Backend
	delay  time.Duration
	spawns *atomic.Int32
}

func (s slowBackend) Connect(ctx context.Context) (Conn, error) {
	s.spawns.Add(1)
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.Backend.Connect(ctx)
}

func slowPool(t *testing.T, delay time.Duration, cfg PoolConfig) (*Pool, *atomic.Int32) {
	t.Helper()
	spawns := &atomic.Int32{}
	p := NewPool(cfg, func(Key) (Backend, error) {
		return slowBackend{Backend: fakeBackend(nil), delay: delay, spawns: spawns}, nil
	}, nil)
	t.Cleanup(p.Shutdown)
	return p, spawns
}

// The breaker exists to stop the gateway hammering a backend that cannot
// start. A caller that gave up says nothing about whether the backend can
// start — and counting it means impatient clients, not a broken server, are
// what opens the circuit. Three of them (the default threshold) and every
// other caller of that server is refused for the whole cooldown, including
// the ones prepared to wait. Clients timing out on a cold spawn is the NORMAL
// case, not a pathological one: a legacy backend handshakes inside Connect,
// so `npx` fetching a package on first use routinely outlasts a client.
func TestCallerCancellationDoesNotOpenTheCircuit(t *testing.T) {
	p, _ := slowPool(t, time.Second,
		PoolConfig{BreakerThreshold: 3, BreakerCooldown: time.Minute})

	key := Key{Server: "s", Tenant: "acme"}
	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, _, err := p.Get(ctx, key)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}

	p.mu.Lock()
	br := p.broken[key]
	p.mu.Unlock()
	assert.Nil(t, br, "a caller that stopped waiting was counted as a spawn failure")
}

// The spawn belongs to the pool, not to whoever happened to trigger it.
// Tearing the subprocess down when that caller gives up means the next
// request starts the same cold spawn from zero — so a backend slower to boot
// than its clients are patient never finishes coming up, however many times
// it is asked for. Not counting the cancellation only stops that from being
// reported as a broken backend; inheriting the spawn is what fixes it.
func TestASlowSpawnOutlivesTheCallerThatTriggeredIt(t *testing.T) {
	p, spawns := slowPool(t, 300*time.Millisecond, PoolConfig{})

	key := Key{Server: "s", Tenant: "acme"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, _, err := p.Get(ctx, key)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	mux, release, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	defer release()
	resp, err := mux.Call(context.Background(), callTool("c1", "echo", `{"hello":"world"}`, ""), nil)
	require.NoError(t, err)
	assert.Contains(t, string(resp.Result), "hello")
	assert.Equal(t, int32(1), spawns.Load(),
		"the abandoned spawn was killed and started over instead of being inherited")
}

// Concurrent Gets for one key share a subprocess. Without that, moving the
// spawn off the caller's context would only relocate the churn: ten
// simultaneous callers for a cold server would start ten `npx` processes and
// throw nine away.
func TestConcurrentGetsShareOneSpawn(t *testing.T) {
	p, spawns := slowPool(t, 100*time.Millisecond, PoolConfig{})

	key := Key{Server: "s", Tenant: "acme"}
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release, err := p.Get(context.Background(), key)
			if assert.NoError(t, err) {
				release()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), spawns.Load(), "each concurrent Get started its own subprocess")
}

// The breaker still opens on the failure it is actually for — and with the
// spawn on the pool's own deadline there is nothing left to disambiguate:
// every error that reaches recordFailure is the backend failing to start.
func TestGenuineSpawnFailureStillOpensTheCircuit(t *testing.T) {
	p := NewPool(PoolConfig{BreakerThreshold: 3, BreakerCooldown: time.Minute},
		func(Key) (Backend, error) { return &StdioBackend{Command: "/nonexistent/binary-xyz"}, nil }, nil)
	t.Cleanup(p.Shutdown)

	key := Key{Server: "s", Tenant: "acme"}
	for range 3 {
		_, _, err := p.Get(context.Background(), key)
		require.Error(t, err)
	}
	_, _, err := p.Get(context.Background(), key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circuit open",
		"a backend that genuinely cannot start must still open the circuit")
}

// unstoppableBackend ignores cancellation, so a spawn can be landed on a pool
// that shut down while it ran — the race Shutdown's closed flag exists for,
// made deterministic. Cancelling spawnCtx handles every backend that honours
// its context; this covers the one that does not.
type unstoppableBackend struct {
	Backend
	started chan struct{}
	release chan struct{}
}

func (u unstoppableBackend) Connect(context.Context) (Conn, error) {
	close(u.started)
	<-u.release
	return u.Backend.Connect(context.Background())
}

// A spawn that lands after Shutdown must close what it built. Installing it
// would leak the subprocess outright: Shutdown has already walked the entries,
// and nothing walks them again.
func TestSpawnLandingAfterShutdownIsClosedNotInstalled(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	p := NewPool(PoolConfig{}, func(Key) (Backend, error) {
		return unstoppableBackend{Backend: fakeBackend(nil), started: started, release: release}, nil
	}, nil)

	key := Key{Server: "s", Tenant: "acme"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, _, err := p.Get(ctx, key)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	<-started
	p.Shutdown()
	close(release)

	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.spawning) == 0
	}, 10*time.Second, time.Millisecond, "the spawn never finished")
	p.mu.Lock()
	defer p.mu.Unlock()
	assert.Empty(t, p.entries, "a spawn landed in a pool that had already been drained")
}

func TestPoolReplacesDeadConn(t *testing.T) {
	p := NewPool(PoolConfig{}, poolFactory, nil)
	t.Cleanup(p.Shutdown)
	key := Key{Server: "s", Tenant: "acme"}

	mux, release, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	_, _ = mux.Call(context.Background(), callTool("k", "crash", "", ""), nil)
	release()
	require.True(t, mux.Dead())

	mux2, release2, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	defer release2()
	resp, err := mux2.Call(context.Background(), callTool("e", "echo", `{}`, ""), nil)
	require.NoError(t, err, "pool must transparently replace a crashed backend")
	require.Nil(t, resp.Error)
}

// The 2026-07-28 subscriptions/listen shape: a notification carrying
// _meta subscriptionId routes to the call whose (muxed) id it references,
// with the caller's original id restored on the way out.
func TestListenNotificationRoutesBySubscriptionID(t *testing.T) {
	m := newTestMux(t)

	var mu sync.Mutex
	var notes []*jsonrpc.Message
	notify := func(n *jsonrpc.Message) {
		mu.Lock()
		notes = append(notes, n)
		mu.Unlock()
	}

	resp, err := m.Call(context.Background(), callTool("sub-77", "listen_event", `{}`, ""), notify)
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	assert.Equal(t, `"sub-77"`, string(resp.ID), "response id must be restored to the caller's")

	// fakemcp writes the notification line before the result line, and the
	// read loop routes in order — by the time Call returned, notify ran.
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, notes, 1, "the subscription event must be forwarded to its call")
	assert.Equal(t, "notifications/resources/updated", notes[0].Method)

	var p struct {
		Meta map[string]json.RawMessage `json:"_meta"`
	}
	require.NoError(t, json.Unmarshal(notes[0].Params, &p))
	assert.Equal(t, `"sub-77"`, string(p.Meta["io.modelcontextprotocol/subscriptionId"]),
		"subscriptionId must be rewritten from the muxed id back to the caller's original")
}

// A notification with no correlator (progressToken or subscriptionId) is
// fundamentally unattributable in a multiplexed process: logged, never
// forwarded to any caller.
func TestUncorrelatedNotificationIsNotForwarded(t *testing.T) {
	m := newTestMux(t)

	var count int
	var mu sync.Mutex
	notify := func(*jsonrpc.Message) {
		mu.Lock()
		count++
		mu.Unlock()
	}

	// notify_changed emits notifications/tools/list_changed with no
	// correlator before its result.
	resp, err := m.Call(context.Background(), callTool("n1", "notify_changed", `{}`, ""), notify)
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	mu.Lock()
	defer mu.Unlock()
	assert.Zero(t, count, "an uncorrelated notification must not reach any caller")
}

// The reaper's three eviction reasons, each in isolation. reap() is invoked
// directly: its loop ticks every 30s in production, far too slow for a test.
func TestReapIdleEntries(t *testing.T) {
	p := NewPool(PoolConfig{IdleTTL: 50 * time.Millisecond}, poolFactory, nil)
	t.Cleanup(p.Shutdown)
	key := Key{Server: "s", Tenant: "acme"}

	mux, release, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	resp, err := mux.Call(context.Background(), callTool("i", "echo", `{}`, ""), nil)
	require.NoError(t, err)
	pid1 := pidOfResp(t, resp)
	release()

	time.Sleep(100 * time.Millisecond) // exceed IdleTTL
	p.reap()

	p.mu.Lock()
	remaining := len(p.entries)
	p.mu.Unlock()
	assert.Zero(t, remaining, "an idle entry past IdleTTL must be reaped")

	// The next Get transparently spawns a fresh backend.
	mux2, release2, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	defer release2()
	resp, err = mux2.Call(context.Background(), callTool("j", "echo", `{}`, ""), nil)
	require.NoError(t, err)
	assert.NotEqual(t, pid1, pidOfResp(t, resp), "reaped entry must respawn as a new process")
}

func TestReapDeadAndCredExpiredEntries(t *testing.T) {
	// Long IdleTTL/MaxLifetime so ONLY the dead and credential-deadline
	// branches can fire.
	expiry := time.Now().Add(credExpirySkew + 100*time.Millisecond)
	factory := func(key Key) (Backend, error) {
		if key.Server == "expiring" {
			return &expiringFake{Backend: fakeBackend(nil), expiresAt: expiry}, nil
		}
		return fakeBackend(nil), nil
	}
	p := NewPool(PoolConfig{IdleTTL: time.Hour, MaxLifetime: time.Hour}, factory, nil)
	t.Cleanup(p.Shutdown)

	// Entry 1: killed subprocess -> dead.
	mux, release, err := p.Get(context.Background(), Key{Server: "doomed", Tenant: "acme"})
	require.NoError(t, err)
	_, _ = mux.Call(context.Background(), callTool("k", "crash", "", ""), nil)
	release()
	require.True(t, mux.Dead())

	// Entry 2: credentials expire mid-life.
	_, release, err = p.Get(context.Background(), Key{Server: "expiring", Tenant: "acme", CredSet: "u1", CredVersion: 1})
	require.NoError(t, err)
	release()

	// Entry 3: healthy control — must survive.
	_, release, err = p.Get(context.Background(), Key{Server: "healthy", Tenant: "acme"})
	require.NoError(t, err)
	release()

	time.Sleep(150 * time.Millisecond) // pass the credential deadline
	p.reap()

	p.mu.Lock()
	_, doomed := p.entries[Key{Server: "doomed", Tenant: "acme"}]
	_, expiring := p.entries[Key{Server: "expiring", Tenant: "acme", CredSet: "u1", CredVersion: 1}]
	_, healthy := p.entries[Key{Server: "healthy", Tenant: "acme"}]
	p.mu.Unlock()

	assert.False(t, doomed, "a dead entry must be reaped")
	assert.False(t, expiring, "an entry past its credential deadline must be reaped")
	assert.True(t, healthy, "a live entry within all deadlines must survive the reaper")
}

func pidOfResp(t *testing.T, resp *jsonrpc.Message) float64 {
	t.Helper()
	var r struct {
		PID float64 `json:"pid"`
	}
	require.NoError(t, json.Unmarshal(resp.Result, &r))
	return r.PID
}

// A Get past the credential deadline replaces the entry but must NOT kill
// its in-flight calls: the old mux is orphaned until it drains, then the
// reaper closes it.
func TestDeadlineReplacementSparesInFlightCalls(t *testing.T) {
	expiry := time.Now().Add(credExpirySkew + 150*time.Millisecond)
	p := NewPool(PoolConfig{}, func(key Key) (Backend, error) {
		return &expiringFake{Backend: fakeBackend(nil), expiresAt: expiry}, nil
	}, nil)
	t.Cleanup(p.Shutdown)
	key := Key{Server: "s", Tenant: "acme", CredSet: "u1", CredVersion: 1}

	oldMux, release, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	// Keep the call slot held across the deadline: the entry is busy.
	time.Sleep(250 * time.Millisecond)

	newMux, release2, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	defer release2()
	require.NotSame(t, oldMux, newMux, "a past-deadline entry must be replaced")

	// The busy old mux survives: its in-flight work still completes.
	resp, err := oldMux.Call(context.Background(), callTool("live", "echo", `{}`, ""), nil)
	require.NoError(t, err, "in-flight work on the orphaned mux must not be killed")
	require.Nil(t, resp.Error)

	// Once drained, the reaper collects the orphan.
	release()
	p.reap()
	require.True(t, oldMux.Dead(), "a drained orphan must be closed by the reaper")
	p.mu.Lock()
	orphans := len(p.orphans)
	p.mu.Unlock()
	assert.Zero(t, orphans)
}

// Orphaned backends are still live subprocesses: they must count toward the
// tenant cap and be closed by EvictServer despite being busy.
func TestOrphansCountedAndEvictable(t *testing.T) {
	expiry := time.Now().Add(credExpirySkew + 120*time.Millisecond)
	p := NewPool(PoolConfig{MaxProcsPerTenant: 2}, func(key Key) (Backend, error) {
		return &expiringFake{Backend: fakeBackend(nil), expiresAt: expiry}, nil
	}, nil)
	t.Cleanup(p.Shutdown)
	key := Key{Server: "s", Tenant: "acme", CredSet: "u1", CredVersion: 1}

	oldMux, release, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // cross the deadline while busy

	// Replacement spawn orphans the busy entry.
	_, release2, err := p.Get(context.Background(), key)
	require.NoError(t, err)
	release2()

	p.mu.Lock()
	count := p.tenantCountLocked("acme")
	p.mu.Unlock()
	assert.Equal(t, 2, count, "a live orphan must count toward the tenant cap")

	// The cap therefore rejects a second server for this tenant.
	_, _, err = p.Get(context.Background(), Key{Server: "other", Tenant: "acme"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max live backends")

	// EvictServer must reach the busy orphan too.
	p.EvictServer("s")
	_, err = oldMux.Call(context.Background(), callTool("x", "echo", `{}`, ""), nil)
	require.Error(t, err, "an evicted server's orphan must be closed even while busy")
	release()

	p.mu.Lock()
	orphans := len(p.orphans)
	p.mu.Unlock()
	assert.Zero(t, orphans)
}

// Credentials already inside the skew window at spawn must not produce a
// born-dead entry (which would respawn a subprocess on every Get): the
// deadline falls back to the literal expiry.
func TestShortLivedCredsDoNotChurnSpawns(t *testing.T) {
	expiry := time.Now().Add(credExpirySkew / 2) // inside the skew window
	p := NewPool(PoolConfig{}, func(key Key) (Backend, error) {
		return &expiringFake{Backend: fakeBackend(nil), expiresAt: expiry}, nil
	}, nil)
	t.Cleanup(p.Shutdown)
	key := Key{Server: "s", Tenant: "acme", CredSet: "u1", CredVersion: 1}

	pidOf := func() float64 {
		mux, release, err := p.Get(context.Background(), key)
		require.NoError(t, err)
		defer release()
		resp, err := mux.Call(context.Background(), callTool("e", "echo", `{}`, ""), nil)
		require.NoError(t, err)
		return pidOfResp(t, resp)
	}
	pid1 := pidOf()
	assert.Equal(t, pid1, pidOf(), "an entry spawned inside the skew window must live to the literal expiry, not respawn per request")
}
