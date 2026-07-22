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
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
)

// expiringFake wraps a Backend with a credential expiry (Expiring).
type expiringFake struct {
	Backend
	expiresAt time.Time
}

func (b *expiringFake) CredExpiresAt() time.Time { return b.expiresAt }

func TestPoolRecyclesAtCredentialExpiry(t *testing.T) {
	// Deadline = expiry - credExpirySkew = ~150ms from now. The reaper ticks
	// every 30s, so a replacement inside the test window proves the
	// GET-TIME check, not the reaper.
	expiry := time.Now().Add(credExpirySkew + 150*time.Millisecond)
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
		var r struct {
			PID float64 `json:"pid"`
		}
		require.NoError(t, json.Unmarshal(resp.Result, &r))
		return r.PID
	}

	pid1 := pidOf()
	assert.Equal(t, pid1, pidOf(), "before the deadline the entry is reused")

	time.Sleep(250 * time.Millisecond)
	assert.NotEqual(t, pid1, pidOf(), "a Get past the credential deadline must replace the entry")
}

// fakeTokenSource serves a swappable bearer and counts invalidations.
type fakeTokenSource struct {
	mu          sync.Mutex
	token       string
	invalidated int
}

func (f *fakeTokenSource) Headers(context.Context) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return map[string]string{"Authorization": f.token}, nil
}

func (f *fakeTokenSource) Invalidate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invalidated++
	f.token = "Bearer good" // the refetched credential
}

func (f *fakeTokenSource) set(tok string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.token = tok
}

// authEchoServer answers each POST with a JSON-RPC result carrying the
// request's Authorization header; wantAuth != "" makes it 401 anything else.
func authEchoServer(t *testing.T, wantAuth string, requests *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		auth := r.Header.Get("Authorization")
		if wantAuth != "" && auth != wantAuth {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		msg := &jsonrpc.Message{}
		require.NoError(t, json.NewDecoder(r.Body).Decode(msg))
		result, _ := json.Marshal(map[string]string{"auth": auth})
		w.Header().Set("Content-Type", "application/json")
		resp, _ := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, result))
		_, _ = w.Write(resp)
	}))
}

func callAuth(t *testing.T, conn Conn, id string) string {
	t.Helper()
	require.NoError(t, conn.Write(context.Background(), callTool(id, "echo", `{}`, "")))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Nil(t, resp.Error, "unexpected error: %+v", resp.Error)
	var r struct {
		Auth string `json:"auth"`
	}
	require.NoError(t, json.Unmarshal(resp.Result, &r))
	return r.Auth
}

func TestHTTPTokenSourcePerRequest(t *testing.T) {
	var requests atomic.Int64
	srv := authEchoServer(t, "", &requests)
	t.Cleanup(srv.Close)

	ts := &fakeTokenSource{token: "Bearer one"}
	b := &HTTPBackend{URL: srv.URL, Headers: map[string]string{"X-Static": "s"}, TokenSource: ts}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	assert.Equal(t, "Bearer one", callAuth(t, conn, "r1"))

	// A refreshed token applies on the SAME conn — no rebuild.
	ts.set("Bearer two")
	assert.Equal(t, "Bearer two", callAuth(t, conn, "r2"))
}

func TestHTTP401RefetchesAndRetriesOnce(t *testing.T) {
	var requests atomic.Int64
	srv := authEchoServer(t, "Bearer good", &requests)
	t.Cleanup(srv.Close)

	ts := &fakeTokenSource{token: "Bearer stale"}
	b := &HTTPBackend{URL: srv.URL, TokenSource: ts}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// stale -> 401 -> Invalidate (source refetches "good") -> retry succeeds.
	assert.Equal(t, "Bearer good", callAuth(t, conn, "r1"))
	assert.Equal(t, 1, ts.invalidated)
	assert.Equal(t, int64(2), requests.Load(), "exactly one retry")
}

func TestHTTP401OnRetryFailsRequest(t *testing.T) {
	var requests atomic.Int64
	srv := authEchoServer(t, "Bearer unobtainable", &requests)
	t.Cleanup(srv.Close)

	ts := &fakeTokenSource{token: "Bearer stale"} // Invalidate yields "good", still wrong
	b := &HTTPBackend{URL: srv.URL, TokenSource: ts}
	conn, err := b.Connect(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	require.NoError(t, conn.Write(context.Background(), callTool("r1", "echo", `{}`, "")))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := conn.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp.Error, "a 401 after the single retry must surface as an error")
	assert.Equal(t, int64(2), requests.Load(), "no retry storm: one retry only")
}
