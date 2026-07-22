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
	"sync"
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
