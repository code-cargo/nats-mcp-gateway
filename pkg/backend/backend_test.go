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
