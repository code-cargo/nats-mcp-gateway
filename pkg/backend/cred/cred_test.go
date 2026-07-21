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

package cred

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestStatic(t *testing.T) {
	r := Static(map[string]string{"Authorization": "Bearer x"}, map[string]string{"KEY": "v"})
	c, err := r.Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Equal(t, "Bearer x", c.Headers["Authorization"])
	assert.Equal(t, "v", c.Env["KEY"])
	assert.True(t, c.ExpiresAt.IsZero())
}

func TestExec(t *testing.T) {
	// The helper proves scrubbing (a secret from the gateway env must not
	// leak through) and identity delivery (the NATSMCP_CRED_* vars).
	t.Setenv("GATEWAY_SECRET", "leak-me-not")
	script := filepath.Join(t.TempDir(), "helper.sh")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
echo "{\"env\":{\"TOKEN\":\"tok-$NATSMCP_CRED_USER\",\"LEAK\":\"$GATEWAY_SECRET\",\"EXTRA\":\"$EXTRA_VAR\"},\"expiresAt\":\"2100-01-01T00:00:00Z\"}"
`), 0o755))

	r := &Exec{Command: script, Env: map[string]string{"EXTRA_VAR": "passed"}}
	c, err := r.Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Equal(t, "tok-u1", c.Env["TOKEN"])
	assert.Empty(t, c.Env["LEAK"], "helper must not inherit the gateway environment")
	assert.Equal(t, "passed", c.Env["EXTRA"], "explicit passthrough env must arrive")
	assert.Equal(t, 2100, c.ExpiresAt.Year())
}

func TestExecFailureIncludesStderr(t *testing.T) {
	script := filepath.Join(t.TempDir(), "helper.sh")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\necho 'not authorized' >&2\nexit 1\n"), 0o755))
	_, err := (&Exec{Command: script}).Resolve(ctxT(t), "acme", "u1", "srv")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not authorized")
}

func TestFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "u1.json"),
		[]byte(`{"headers":{"Authorization":"Bearer file-u1"}}`), 0o600))

	r := &File{Path: filepath.Join(dir, "{user}.json")}
	c, err := r.Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Equal(t, "Bearer file-u1", c.Headers["Authorization"])
	// No expiresAt in the file -> the default TTL applies so rotation is
	// eventually picked up.
	assert.False(t, c.ExpiresAt.IsZero())
	assert.WithinDuration(t, time.Now().Add(time.Minute), c.ExpiresAt, 5*time.Second)

	_, err = r.Resolve(ctxT(t), "acme", "other", "srv")
	require.Error(t, err, "missing per-user file must fail")
}

// tokenEndpoint fakes an RFC 6749 token endpoint capturing the last form.
func tokenEndpoint(t *testing.T, status int, body string) (*httptest.Server, *url.Values) {
	t.Helper()
	var last url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		last = r.PostForm
		if u, _, ok := r.BasicAuth(); ok {
			last.Set("_basic_user", u)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &last
}

func TestOAuthClientCredentials(t *testing.T) {
	srv, form := tokenEndpoint(t, http.StatusOK,
		`{"access_token":"at-1","token_type":"bearer","expires_in":3600}`)
	r := &OAuthClientCredentials{
		TokenURL: srv.URL, ClientID: "cid", ClientSecret: "cs",
		Scope: "read", Audience: "aud",
	}
	c, err := r.Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Equal(t, "Bearer at-1", c.Headers["Authorization"])
	assert.WithinDuration(t, time.Now().Add(time.Hour), c.ExpiresAt, 5*time.Second)
	assert.Equal(t, "client_credentials", form.Get("grant_type"))
	assert.Equal(t, "read", form.Get("scope"))
	assert.Equal(t, "aud", form.Get("audience"))
	assert.Equal(t, "cid", form.Get("_basic_user"))
}

func TestOAuthTokenExchange(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "u1.jwt"), []byte("subject-jwt\n"), 0o600))
	srv, form := tokenEndpoint(t, http.StatusOK, `{"access_token":"at-x","expires_in":60}`)

	r := &OAuthTokenExchange{
		TokenURL: srv.URL, ClientID: "cid", ClientSecret: "cs", Audience: "jira",
		SubjectTokenFile: filepath.Join(dir, "{user}.jwt"),
	}
	c, err := r.Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Equal(t, "Bearer at-x", c.Headers["Authorization"])
	assert.Equal(t, "urn:ietf:params:oauth:grant-type:token-exchange", form.Get("grant_type"))
	assert.Equal(t, "subject-jwt", form.Get("subject_token"), "subject token must be trimmed")
	assert.Equal(t, "urn:ietf:params:oauth:token-type:access_token", form.Get("subject_token_type"))
	assert.Equal(t, "jira", form.Get("audience"))
}

func TestOAuthTerminalOn4xx(t *testing.T) {
	srv, _ := tokenEndpoint(t, http.StatusBadRequest, `{"error":"invalid_grant"}`)
	r := &OAuthClientCredentials{TokenURL: srv.URL, ClientID: "cid", ClientSecret: "cs"}
	_, err := r.Resolve(ctxT(t), "a", "u", "s")
	require.Error(t, err)
	assert.True(t, IsTerminal(err), "an IdP 4xx is authoritative")

	srv5, _ := tokenEndpoint(t, http.StatusBadGateway, "upstream down")
	r.TokenURL = srv5.URL
	_, err = r.Resolve(ctxT(t), "a", "u", "s")
	require.Error(t, err)
	assert.False(t, IsTerminal(err), "a 5xx is retryable")
}

func TestOAuthRefreshRotation(t *testing.T) {
	dir := t.TempDir()
	tokFile := filepath.Join(dir, "u1.refresh")
	require.NoError(t, os.WriteFile(tokFile, []byte("refresh-1"), 0o600))
	srv, form := tokenEndpoint(t, http.StatusOK,
		`{"access_token":"at-r","expires_in":60,"refresh_token":"refresh-2"}`)

	r := &OAuthRefresh{
		TokenURL: srv.URL, ClientID: "cid", ClientSecret: "cs",
		Store: &FileTokenStore{Path: filepath.Join(dir, "{user}.refresh")},
	}
	c, err := r.Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Equal(t, "Bearer at-r", c.Headers["Authorization"])
	assert.Equal(t, "refresh_token", form.Get("grant_type"))
	assert.Equal(t, "refresh-1", form.Get("refresh_token"))

	rotated, err := os.ReadFile(tokFile)
	require.NoError(t, err)
	assert.Equal(t, "refresh-2", string(rotated), "a rotated refresh token must be persisted")
}

func TestNATSResolver(t *testing.T) {
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true}
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	sub, err := nc.Subscribe("mcp.v1.cred.acme.u1.grafana", func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"headers":{"Authorization":"Bearer nats-u1"},"expiresAt":"2100-01-01T00:00:00Z"}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	subErr, err := nc.Subscribe("mcp.v1.cred.acme.u2.grafana", func(m *nats.Msg) {
		_ = m.RespondMsg(&nats.Msg{Header: nats.Header{"Nats-Service-Error": []string{"user not authorized"}}})
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = subErr.Unsubscribe() })

	r := &NATS{Conn: nc}
	c, err := r.Resolve(ctxT(t), "acme", "u1", "grafana")
	require.NoError(t, err)
	assert.Equal(t, "Bearer nats-u1", c.Headers["Authorization"])

	_, err = r.Resolve(ctxT(t), "acme", "u2", "grafana")
	require.Error(t, err)
	assert.True(t, IsTerminal(err), "a responder refusal is authoritative")
	assert.Contains(t, err.Error(), "user not authorized")

	// No responder at all: transport failure, retryable.
	rShort := &NATS{Conn: nc, RequestTimeout: 200 * time.Millisecond}
	_, err = rShort.Resolve(ctxT(t), "acme", "u1", "unknown")
	require.Error(t, err)
	assert.False(t, IsTerminal(err))
}

// countingResolver scripts results and counts inner calls.
type countingResolver struct {
	mu    sync.Mutex
	calls int
	next  func(user string) (*Credentials, error)
	delay time.Duration
}

func (c *countingResolver) Resolve(_ context.Context, _, user, _ string) (*Credentials, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.next(user)
}

func (c *countingResolver) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestCachedCachesUntilExpiryThenRefreshes(t *testing.T) {
	inner := &countingResolver{next: func(user string) (*Credentials, error) {
		return &Credentials{
			Headers:   map[string]string{"Authorization": "Bearer " + user},
			ExpiresAt: time.Now().Add(80 * time.Millisecond),
		}, nil
	}}
	c := Cached(inner, time.Millisecond)

	_, gen1, err := c.ResolveGen(ctxT(t), "t", "u1", "s")
	require.NoError(t, err)
	_, gen2, err := c.ResolveGen(ctxT(t), "t", "u1", "s")
	require.NoError(t, err)
	assert.Equal(t, gen1, gen2, "unexpired credentials must be served from cache")
	assert.Equal(t, 1, inner.count())

	time.Sleep(100 * time.Millisecond)
	_, gen3, err := c.ResolveGen(ctxT(t), "t", "u1", "s")
	require.NoError(t, err)
	assert.Greater(t, gen3, gen2, "a refresh must advance the generation")
	assert.Equal(t, 2, inner.count())
}

func TestCachedKeysPerUser(t *testing.T) {
	inner := &countingResolver{next: func(user string) (*Credentials, error) {
		return &Credentials{Headers: map[string]string{"Authorization": "Bearer " + user}}, nil
	}}
	c := Cached(inner, 0)

	a, genA, err := c.ResolveGen(ctxT(t), "t", "alice", "s")
	require.NoError(t, err)
	b, genB, err := c.ResolveGen(ctxT(t), "t", "bob", "s")
	require.NoError(t, err)
	assert.Equal(t, "Bearer alice", a.Headers["Authorization"])
	assert.Equal(t, "Bearer bob", b.Headers["Authorization"])
	assert.NotEqual(t, genA, genB, "distinct keys must never share a generation")
	assert.Equal(t, 2, inner.count())
}

func TestCachedSingleFlight(t *testing.T) {
	inner := &countingResolver{delay: 50 * time.Millisecond, next: func(string) (*Credentials, error) {
		return &Credentials{}, nil
	}}
	c := Cached(inner, 0)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Resolve(context.Background(), "t", "u", "s")
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, 1, inner.count(), "concurrent misses on one key must single-flight")
}

func TestCachedNegativeCacheAndInvalidate(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	inner := &countingResolver{next: func(string) (*Credentials, error) {
		if fail.Load() {
			return nil, errors.New("controller down")
		}
		return &Credentials{}, nil
	}}
	c := Cached(inner, 0)

	_, err := c.Resolve(ctxT(t), "t", "u", "s")
	require.Error(t, err)
	_, err = c.Resolve(ctxT(t), "t", "u", "s")
	require.Error(t, err)
	assert.Equal(t, 1, inner.count(), "a failure inside the backoff window must be memoized, not re-hammered")

	// Invalidate clears the backoff (the 401 path needs an immediate
	// refetch), and the now-healthy source succeeds.
	fail.Store(false)
	c.Invalidate("t", "u", "s")
	_, err = c.Resolve(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	assert.Equal(t, 2, inner.count())
}

func TestFailBackoffCaps(t *testing.T) {
	assert.Equal(t, failBackoffBase, failBackoff(1))
	assert.Equal(t, 2*failBackoffBase, failBackoff(2))
	assert.Equal(t, failBackoffCap, failBackoff(20))
	assert.Equal(t, failBackoffCap, failBackoff(200), "huge fail counts must not overflow")
}

func TestDecodeCredentials(t *testing.T) {
	c, err := decodeCredentials([]byte(`{"headers":{"A":"1"},"env":{"B":"2"},"expiresAt":"2100-01-02T03:04:05Z","futureField":true}`))
	require.NoError(t, err)
	assert.Equal(t, "1", c.Headers["A"])
	assert.Equal(t, "2", c.Env["B"])
	assert.Equal(t, 2100, c.ExpiresAt.Year())

	_, err = decodeCredentials([]byte(`{"expiresAt":"not-a-time"}`))
	require.Error(t, err)
	_, err = decodeCredentials([]byte(`nonsense`))
	require.Error(t, err)
}

func TestTerminalWrapping(t *testing.T) {
	base := errors.New("no")
	assert.False(t, IsTerminal(base))
	assert.True(t, IsTerminal(Terminal(base)))
	assert.True(t, IsTerminal(fmt.Errorf("wrapped: %w", Terminal(base))))
	assert.NoError(t, Terminal(nil))
	assert.ErrorIs(t, Terminal(base), base)
}
