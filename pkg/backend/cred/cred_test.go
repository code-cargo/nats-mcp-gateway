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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
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

// startResolve runs one Exec.Resolve in the background, so a test can act on
// the helper — cancel it, watch for it — while the resolve is in flight.
func startResolve(ctx context.Context, r *Exec) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := r.Resolve(ctx, "acme", "u1", "srv")
		done <- err
	}()
	return done
}

// awaitResolve fails the test if the resolve has not finished within d. The
// hangs these tests provoke are unbounded, so waiting on the channel directly
// would stall the whole package until the go test panic timeout instead of
// naming the invariant that broke.
func awaitResolve(t *testing.T, done <-chan error, d time.Duration, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("Exec.Resolve did not return within %s: %s", d, what)
		return nil
	}
}

func TestExecCancellationKillsTheWholeHelperTree(t *testing.T) {
	// Wrapper-script helpers fork, the same shape StdioBackend deals with in
	// npx/uvx. Killing the helper alone leaves that child running and still
	// holding the write end of stdout, so Wait sits on a pipe that will never
	// reach EOF and no remaining deadline can rescue it — all under
	// CachedResolver's per-key mutex, which no context can interrupt. The
	// (tenant, user, server) key is then wedged for the life of the gateway:
	// later requests for it park on the lock forever instead of failing and
	// backing off. Cancelling here is the helper's own Timeout expiring on a
	// slower clock; os/exec runs the identical path for both.
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	script := filepath.Join(dir, "helper.sh")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
# The long-lived child is forked BEFORE the marker is written, so waiting for
# the marker proves the process which will hold stdout past the kill already
# exists. Written the other way round the marker proves only that the
# marker-WRITER exists, and the cancel can arrive before that writer has forked
# the sleep — leaving the test asserting the death of a child that was never
# born.
sleep 60 &
echo $! > "$READY"
sleep 60
`), 0o755))

	r := &Exec{Command: script, Env: map[string]string{"READY": ready}, Timeout: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startResolve(ctx, r)

	// The child writes this itself, so its existence proves a forked process
	// is running and holding stdout — without it the assertions below would
	// hold trivially on a helper that never forked at all.
	require.Eventually(t, func() bool {
		_, err := os.Stat(ready)
		return err == nil
	}, 30*time.Second, 10*time.Millisecond, "the helper never forked the child this test is about")

	childPID := readPID(t, ready)
	cancel()
	require.Error(t, awaitResolve(t, done, 30*time.Second, "the helper's child still holds stdout"),
		"a cancelled helper must fail, not hang")
	assertReaped(t, childPID)
}

func TestExecReturnsWhenAChildOutlivesTheHelper(t *testing.T) {
	// Here the helper prints its credentials and exits cleanly, so nothing
	// ever cancels the command and its timeout is irrelevant — but a child it
	// left behind still holds stdout open. os/exec stops watching the context
	// the moment the process is reaped, so the read that follows is bounded by
	// WaitDelay alone: without one, Resolve never returns at all, and no
	// configured timeout changes that.
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	script := filepath.Join(dir, "helper.sh")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
sleep 60 &
echo $! > "$READY"
echo '{"env":{"TOKEN":"tok"},"expiresAt":"2100-01-01T00:00:00Z"}'
`), 0o755))

	// A generous timeout, to show it is not what ends the wait.
	r := &Exec{Command: script, Env: map[string]string{"READY": ready}, Timeout: time.Minute}
	err := awaitResolve(t, startResolve(context.Background(), r), 30*time.Second,
		"a child of the exited helper still holds stdout")
	// Abandoning the credentials is deliberate. A helper that hands our stdout
	// to a process outliving it has broken its side of the contract, and a
	// resolve that fails feeds the backoff and reaches the caller as -32014,
	// where one that blocks forever reaches nobody.
	require.ErrorIs(t, err, exec.ErrWaitDelay)

	// Returning is only half of it. Nothing cancelled this command, so the
	// group kill never ran, and all os/exec does at WaitDelay is close OUR end
	// of the pipe — the child is still out there, reparented to init, still in
	// the group. Backoff brings the gateway back down this path for as long as
	// the helper keeps the habit, so a child left behind here is a child left
	// behind per attempt.
	assertReaped(t, readPID(t, ready))
}

// The scrubbing the environment gets is worth nothing if HOME still points at
// the gateway's own home directory: a helper is third-party code by
// construction (that is the entire premise of the mode), and the dotfiles it
// can reach from there include the NATS creds file the gateway authenticates
// with.
func TestExecDoesNotInheritTheGatewaysHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	require.NoError(t, os.WriteFile(filepath.Join(home, ".nats.creds"),
		[]byte("gateway-operator-creds"), 0o600))

	script := filepath.Join(t.TempDir(), "helper.sh")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
echo "{\"env\":{\"HOME\":\"$HOME\",\"STOLEN\":\"$(cat "$HOME/.nats.creds" 2>/dev/null)\"},\"expiresAt\":\"2100-01-01T00:00:00Z\"}"
`), 0o755))

	c, err := (&Exec{Command: script}).Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Empty(t, c.Env["STOLEN"], "the helper read a dotfile out of the gateway's home directory")
	assert.NotEqual(t, home, c.Env["HOME"], "the helper was handed the gateway's own HOME")
	// The scratch home is per-run, so nothing a helper leaves in it survives to
	// be read by the next one — including the next one resolving for a
	// different user.
	_, err = os.Stat(c.Env["HOME"])
	assert.True(t, os.IsNotExist(err), "the helper's scratch home outlived the run")
}

// The escape hatch for helpers that genuinely need a populated home (an `aws`
// or `gcloud` wrapper reading its own config): naming HOME in auth.env is how
// an operator opts back in, deliberately and per server.
func TestExecHomeCanBeSetExplicitly(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	real := t.TempDir()
	script := filepath.Join(t.TempDir(), "helper.sh")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
echo "{\"env\":{\"HOME\":\"$HOME\"},\"expiresAt\":\"2100-01-01T00:00:00Z\"}"
`), 0o755))

	r := &Exec{Command: script, Env: map[string]string{"HOME": real}}
	c, err := r.Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Equal(t, real, c.Env["HOME"])
}

// The private home is half a boundary. A helper is third-party code that is
// under no obligation to write absolute paths, and whatever it writes relative
// lands where it was started — which, unless it is told otherwise, is the
// gateway's own working directory, at the gateway's uid. StdioBackend points a
// spawned server at its workdir for the same reason.
func TestExecRunsInItsOwnDirectory(t *testing.T) {
	script := filepath.Join(t.TempDir(), "helper.sh")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
: > stray-file
echo "{\"env\":{\"CWD\":\"$(pwd)\"},\"expiresAt\":\"2100-01-01T00:00:00Z\"}"
`), 0o755))

	gatewayCwd, err := os.Getwd()
	require.NoError(t, err)
	stray := filepath.Join(gatewayCwd, "stray-file")
	t.Cleanup(func() { _ = os.Remove(stray) })

	c, err := (&Exec{Command: script}).Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.NotEqual(t, gatewayCwd, c.Env["CWD"], "the helper ran in the gateway's working directory")
	assert.NoFileExists(t, stray, "a relative-path write by the helper landed among the gateway's files")
	// Per run and removed with the run, like the home it is.
	_, err = os.Stat(c.Env["CWD"])
	assert.True(t, os.IsNotExist(err), "the helper's working directory outlived the run")
}

// The wait delay decides whether a helper that leaves something on stdout is a
// failed resolve or a wedged one, so its length is a property of the HELPER: a
// wrapper whose child holds the pipe for longer than the default resolved
// successfully before the bound existed and cannot resolve at all after it.
// Configurable for that helper, and short for the operator who would rather
// fail fast — the wait is charged to a cache mutex either way.
func TestExecWaitDelayIsConfigurable(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	script := filepath.Join(dir, "helper.sh")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
sleep "$HOLD" &
echo $! > "$READY"
echo '{"env":{"TOKEN":"tok"},"expiresAt":"2100-01-01T00:00:00Z"}'
`), 0o755))
	helper := func(hold string, delay time.Duration) *Exec {
		return &Exec{
			Command:   script,
			Env:       map[string]string{"READY": ready, "HOLD": hold},
			Timeout:   time.Minute,
			WaitDelay: delay,
		}
	}

	t.Run("shorter than the child holds", func(t *testing.T) {
		start := time.Now()
		err := awaitResolve(t, startResolve(context.Background(), helper("60", 50*time.Millisecond)),
			10*time.Second, "the configured wait delay was not what ended the wait")
		require.ErrorIs(t, err, exec.ErrWaitDelay)
		// Which bound ended it is the whole question, and here only the clock
		// can answer: both bounds produce this same error. The margin is 40x
		// the configured delay and well under the default it has to beat.
		assert.Less(t, time.Since(start), 2*time.Second,
			"the wait ran to the default instead of the configured delay")
		assertReaped(t, readPID(t, ready))
	})

	// Longer than the child holds AND longer than the default, which is the
	// helper the fixed bound broke: it prints its credentials, exits, and the
	// child it forgot to detach releases stdout a second past the default.
	t.Run("longer than the child holds", func(t *testing.T) {
		err := awaitResolve(t, startResolve(context.Background(), helper("4", 30*time.Second)),
			20*time.Second, "a helper within its configured wait delay never resolved")
		require.NoError(t, err, "the configured wait delay was ignored in favour of the default")
	})
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

// A public client has a client id and no secret — the shape of every
// authorization-code/refresh registration for a client that cannot keep one,
// and oauth-refresh with no clientSecret is a configuration the gateway
// accepts. RFC 6749 §3.2.1 says such a client identifies itself with client_id
// in the request BODY. Authenticating instead as (id, empty password) over
// Basic is not the same claim, and Okta, Auth0 and Keycloak all answer it with
// a terminal invalid_client — so the mode failed on every attempt, forever,
// with a backoff that never expires it.
func TestOAuthPublicClientSendsClientIDInTheBody(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "u1.refresh"), []byte("refresh-1"), 0o600))
	srv, form := tokenEndpoint(t, http.StatusOK, `{"access_token":"at-p","expires_in":60}`)

	r := &OAuthRefresh{
		TokenURL: srv.URL, ClientID: "public-cid",
		Store: &FileTokenStore{Path: filepath.Join(dir, "{user}.refresh")},
	}
	c, err := r.Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Equal(t, "Bearer at-p", c.Headers["Authorization"])
	assert.Equal(t, "public-cid", form.Get("client_id"))
	assert.Empty(t, form.Get("_basic_user"), "a public client must not send an Authorization header at all")
}

// A confidential client keeps Basic — it is what RFC 6749 §2.3.1 prefers and
// what every IdP the gateway has been pointed at expects.
func TestOAuthConfidentialClientKeepsBasic(t *testing.T) {
	srv, form := tokenEndpoint(t, http.StatusOK, `{"access_token":"at-c","expires_in":60}`)
	_, err := (&OAuthClientCredentials{TokenURL: srv.URL, ClientID: "cid", ClientSecret: "cs"}).
		Resolve(ctxT(t), "acme", "u1", "srv")
	require.NoError(t, err)
	assert.Equal(t, "cid", form.Get("_basic_user"))
	assert.Empty(t, form.Get("client_id"), "a secret-bearing client authenticates in the header, not the body")
}

func TestFileTokenStoreReplacesAtomically(t *testing.T) {
	// A refresh token is the one credential the gateway cannot re-derive: lose
	// it and the user goes back through consent. Truncate-then-write leaves a
	// window in which the file holds neither the old token nor the new one,
	// and anything landing inside that window — a crash, a full disk, a second
	// writer — leaves it holding nothing at all.
	//
	// The token here is deliberately large, because that is what makes the
	// window wide enough to catch from another goroutine in a test. Real ones
	// are smaller and the window is narrower, not absent; a process that dies
	// inside it loses the token whatever its size.
	dir := t.TempDir()
	store := &FileTokenStore{Path: filepath.Join(dir, "{user}.refresh")}
	tokens := []string{strings.Repeat("a", 64<<10), strings.Repeat("b", 64<<10)}
	require.NoError(t, store.Save("acme", "u1", "srv", tokens[0]))

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			assert.NoError(t, store.Save("acme", "u1", "srv", tokens[i%2]))
		}
	}()

	for range 3000 {
		got, err := store.Load("acme", "u1", "srv")
		require.NoError(t, err, "the token file must never be missing")
		require.Contains(t, tokens, got,
			"a reader must see one whole token or the other, never a half-written file")
	}
	close(stop)
	writer.Wait()

	// And nothing left behind: the replacement is one file, not a growing
	// litter of partial ones beside it.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "the write must not leave temporary files in the token directory")
}

func TestNATSResolver(t *testing.T) {
	nc, _ := natstest.Run(t, nil)

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
	next, delay := c.next, c.delay
	c.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	return next(user)
}

func (c *countingResolver) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestCachedCachesUntilExpiryThenRefreshes(t *testing.T) {
	// Each resolve returns DIFFERENT material, so a real refresh must
	// advance the generation.
	inner := &countingResolver{}
	inner.next = func(user string) (*Credentials, error) {
		return &Credentials{
			Headers:   map[string]string{"Authorization": fmt.Sprintf("Bearer %s-%d", user, inner.count())},
			ExpiresAt: time.Now().Add(80 * time.Millisecond),
		}, nil
	}
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
	assert.Greater(t, gen3, gen2, "changed credentials must advance the generation")
	assert.Equal(t, 2, inner.count())
}

func TestCachedIdenticalRefreshKeepsGeneration(t *testing.T) {
	// Identical material on refresh keeps the generation, so steadily
	// renewed same-value credentials never churn pooled backends.
	inner := &countingResolver{next: func(user string) (*Credentials, error) {
		return &Credentials{
			Env:       map[string]string{"TOKEN": "constant"},
			ExpiresAt: time.Now().Add(60 * time.Millisecond),
		}, nil
	}}
	c := Cached(inner, time.Millisecond)

	_, gen1, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	time.Sleep(80 * time.Millisecond) // past expiry -> blocking re-resolve
	_, gen2, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, inner.count(), 2, "expired credentials must be re-resolved")
	assert.Equal(t, gen1, gen2, "identical material must keep its generation")
}

func TestCachedShortTTLStillCaches(t *testing.T) {
	// TTL far below the skew: the refresh-ahead lead is clamped to TTL/2 so
	// the credentials still cache instead of resolving on every request.
	inner := &countingResolver{next: func(user string) (*Credentials, error) {
		return &Credentials{ExpiresAt: time.Now().Add(1 * time.Second)}, nil
	}}
	c := Cached(inner, 0) // DefaultSkew 30s >> TTL

	_, _, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	_, _, err = c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	assert.Equal(t, 1, inner.count(), "short-TTL credentials must still be served from cache within TTL/2")
}

func TestCachedCallerCancellationNotMemoized(t *testing.T) {
	inner := &countingResolver{delay: 100 * time.Millisecond, next: func(string) (*Credentials, error) {
		return nil, context.Canceled // what an inner resolver returns when its ctx dies
	}}
	c := Cached(inner, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, err := c.ResolveGen(ctx, "t", "u", "s")
	require.Error(t, err)

	// A fresh caller must NOT be served the cancelled caller's failure.
	inner.mu.Lock()
	inner.next = func(string) (*Credentials, error) { return &Credentials{}, nil }
	inner.delay = 0
	inner.mu.Unlock()
	_, _, err = c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err, "a caller-side cancellation must not poison the negative cache")
}

func TestGenerationsMonotonicAcrossResolvers(t *testing.T) {
	// The gateway rebuilds a resolver when a server's auth config changes;
	// generations must never repeat across rebuilds or new-resolver keys
	// would alias pool entries minted by the old one.
	mk := func(token string) *CachedResolver {
		return Cached(ResolveFunc(func(context.Context, string, string, string) (*Credentials, error) {
			return &Credentials{Env: map[string]string{"T": token}}, nil
		}), 0)
	}
	_, gen1, err := mk("a").ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	_, gen2, err := mk("b").ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	assert.Greater(t, gen2, gen1, "a rebuilt resolver must not reuse generations")
}

func TestCachedRefreshAheadDoesNotBlockCallers(t *testing.T) {
	var n atomic.Int64
	inner := &countingResolver{}
	inner.next = func(user string) (*Credentials, error) {
		if n.Add(1) > 1 {
			time.Sleep(300 * time.Millisecond) // slow renewal
		}
		return &Credentials{
			Env:       map[string]string{"T": fmt.Sprint(n.Load())},
			ExpiresAt: time.Now().Add(400 * time.Millisecond),
		}, nil
	}
	c := Cached(inner, 100*time.Millisecond) // lead = min(100ms+jitter, 200ms)

	first, _, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)

	time.Sleep(320 * time.Millisecond) // inside the lead window, before expiry
	start := time.Now()
	got, _, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 100*time.Millisecond,
		"a caller inside the refresh window must be served immediately, not block on the renewal")
	assert.Equal(t, first.Env["T"], got.Env["T"], "the still-valid credentials are served during refresh")

	// The background refresh eventually lands the new credentials.
	require.Eventually(t, func() bool {
		cur, _, err := c.ResolveGen(context.Background(), "t", "u", "s")
		return err == nil && cur.Env["T"] != first.Env["T"]
	}, 3*time.Second, 20*time.Millisecond)
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

// Invalidate promises the next resolve refetches immediately, "any failure
// backoff cleared too". It cleared the deadline but not the count the next
// deadline is computed from, so the promise only held for one attempt: every
// 401-driven invalidation that met a still-unhealthy source doubled the wait
// again, and after six rounds a key that should retry in a second was pinned
// at the 30s cap. The rounds cost nothing to reach because Invalidate is what
// admits each one — a backend rejecting credentials on every request drives
// them at request rate.
func TestInvalidateRestartsTheFailureBackoff(t *testing.T) {
	inner := &countingResolver{next: func(string) (*Credentials, error) {
		return nil, errors.New("controller down")
	}}
	c := Cached(inner, 0)

	for range 6 {
		_, err := c.Resolve(ctxT(t), "t", "u", "s")
		require.Error(t, err)
		c.Invalidate("t", "u", "s")
	}
	_, err := c.Resolve(ctxT(t), "t", "u", "s")
	require.Error(t, err)

	e := c.entry("t", "u", "s")
	e.mu.Lock()
	backoff := time.Until(e.retryAt)
	e.mu.Unlock()
	assert.InDelta(t, failBackoffBase, backoff, float64(500*time.Millisecond),
		"an invalidated key must retry at the first backoff step, not wherever the old count had reached")
}

func TestInvalidateThenIdenticalMaterialKeepsGeneration(t *testing.T) {
	// A backend that rejects a credential the source keeps re-issuing (wrong
	// audience, revoked upstream, clock skew) drives Invalidate on every
	// request. The generation keys the backend pool, so advancing it for
	// material that did not change spawns a second backend identical to the
	// one it supersedes — once per request, until the tenant's cap is spent.
	inner := &countingResolver{next: func(string) (*Credentials, error) {
		return &Credentials{
			Headers:   map[string]string{"Authorization": "Bearer constant"},
			ExpiresAt: time.Now().Add(time.Hour),
		}, nil
	}}
	c := Cached(inner, 0)

	_, gen1, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)

	for range 5 {
		c.Invalidate("t", "u", "s")
		_, gen, err := c.ResolveGen(ctxT(t), "t", "u", "s")
		require.NoError(t, err)
		assert.Equal(t, gen1, gen,
			"re-resolving the same material after a 401 must not mint a new generation")
	}
	// The first invalidation refetches; the four behind it are inside the window
	// that refetch bought by coming back with the same credential. Serving the
	// rejected credentials there is the point — the caller gets the backend's
	// 401 without an IdP round-trip in front of it.
	assert.Equal(t, 2, inner.count(),
		"a rejection the source cannot answer must not reach it once per request")
}

// Invalidate is the one path that both drops the cache and clears the failure
// backoff, and a backend rejecting the credentials on every request calls it on
// every request. Unlimited, that is a fresh IdP resolve per request with the
// guard that exists to prevent exactly that cleared each time — resolution runs
// outside the pool's circuit breaker, so this backoff is the only thing between
// a credential source and the gateway's full request rate.
func TestInvalidateIsRateLimitedByAFutileRefetch(t *testing.T) {
	inner := &countingResolver{next: func(string) (*Credentials, error) {
		return &Credentials{
			Headers:   map[string]string{"Authorization": "Bearer constant"},
			ExpiresAt: time.Now().Add(time.Hour),
		}, nil
	}}
	c := Cached(inner, 0)

	_, err := c.Resolve(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	// The rejection of material nobody has judged yet: acted on at once.
	c.Invalidate("t", "u", "s")
	_, err = c.Resolve(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	require.Equal(t, 2, inner.count(), "the first rejection must refetch immediately")

	// That refetch answered nothing, and the request rate carries on.
	for range 50 {
		c.Invalidate("t", "u", "s")
		got, resolveErr := c.Resolve(ctxT(t), "t", "u", "s")
		require.NoError(t, resolveErr)
		require.Equal(t, "Bearer constant", got.Headers["Authorization"],
			"the cached credentials must keep serving inside the window")
	}
	assert.Equal(t, 2, inner.count(), "a 401 loop drove one credential-source resolve per request")

	// The other half: the backoff Invalidate clears is the one a failing source
	// is throttled by, so a rate-limited invalidation must leave it standing.
	e := c.entry("t", "u", "s")
	e.mu.Lock()
	e.fails, e.retryAt = 4, time.Now().Add(10*time.Second)
	e.mu.Unlock()
	c.Invalidate("t", "u", "s")
	e.mu.Lock()
	fails, retryAt := e.fails, e.retryAt
	e.mu.Unlock()
	assert.Equal(t, 4, fails, "a rate-limited invalidation reset the failure count")
	assert.False(t, retryAt.IsZero(), "a rate-limited invalidation cleared the failure backoff")
}

// The rate limit is bought by a refetch that returned the same credential, so
// it must never be paid by the case Invalidate exists for. A rotation is a
// rejection whose refetch DOES have something new to return: the material
// moves, the generation moves, the pool keys a new backend on it — and the next
// rejection after that is about a credential nothing has judged yet.
func TestInvalidateStaysPromptAcrossRotations(t *testing.T) {
	var n atomic.Int64
	inner := &countingResolver{next: func(string) (*Credentials, error) {
		return &Credentials{
			Headers:   map[string]string{"Authorization": fmt.Sprintf("Bearer rotation-%d", n.Add(1))},
			ExpiresAt: time.Now().Add(time.Hour),
		}, nil
	}}
	c := Cached(inner, 0)

	_, gen, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	seen := map[int]bool{gen: true}

	for range 5 {
		c.Invalidate("t", "u", "s")
		creds, g, resolveErr := c.ResolveGen(ctxT(t), "t", "u", "s")
		require.NoError(t, resolveErr)
		require.Falsef(t, seen[g], "rotated material was served under generation %d twice", g)
		seen[g] = true
		assert.Equal(t, fmt.Sprintf("Bearer rotation-%d", n.Load()), creds.Headers["Authorization"],
			"a rotation must reach the caller on the request that follows the rejection")
	}
	assert.Equal(t, 6, inner.count(), "a rotation was withheld by the rate limit a 401 loop is for")
}

func TestInvalidateDoesNotCollapseTheRefreshCadence(t *testing.T) {
	// Remembering what Invalidate dropped answers one question — did the
	// material move — and must not be mistaken for an answer to the other. A
	// refresh returning an unchanged ExpiresAt is unproductive and re-arms at a
	// fixed skew cadence; a refetch after a 401 returning the same credential
	// is not a refresh at all, and re-arming it that way would put an hour-long
	// credential into a resolve every 30s for the rest of its life — trading a
	// backend spawned per request for a credential source called per skew.
	exp := time.Now().Add(time.Hour)
	inner := &countingResolver{next: func(string) (*Credentials, error) {
		return &Credentials{
			Headers:   map[string]string{"Authorization": "Bearer constant"},
			ExpiresAt: exp,
		}, nil
	}}
	c := Cached(inner, 0)

	_, _, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	e := c.entry("t", "u", "s")
	e.mu.Lock()
	primed := e.refreshAt
	e.mu.Unlock()
	require.WithinDuration(t, exp, primed, 2*DefaultSkew,
		"the lead window opens just before expiry, not just after the resolve")

	c.Invalidate("t", "u", "s")
	_, _, err = c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)

	e.mu.Lock()
	refetched := e.refreshAt
	e.mu.Unlock()
	assert.WithinDuration(t, primed, refetched, 2*DefaultSkew,
		"a refetch after a 401 must leave the refresh-ahead where the credential's expiry put it")
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

func TestExpiredCredentialsHitBackoffNotRequestRate(t *testing.T) {
	// A source serving already-expired material (late rotator) must land in
	// the failure backoff, not be re-resolved on every request.
	inner := &countingResolver{next: func(string) (*Credentials, error) {
		return &Credentials{
			Env:       map[string]string{"T": "stale"},
			ExpiresAt: time.Now().Add(-time.Minute),
		}, nil
	}}
	c := Cached(inner, 0)

	_, _, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.Error(t, err, "expired material must surface as a resolve failure")
	assert.Contains(t, err.Error(), "already expired")

	for range 5 {
		_, _, err = c.ResolveGen(ctxT(t), "t", "u", "s")
		require.Error(t, err)
	}
	assert.Equal(t, 1, inner.count(), "requests inside the backoff window must not re-resolve")
}

func TestStaleRefreshResultDiscarded(t *testing.T) {
	// A background refresh that completes AFTER the entry moved on
	// (Invalidate, or a foreground re-resolve) must discard its result
	// rather than overwrite fresher credentials last-writer-wins.
	release := make(chan struct{})
	var slowStarted atomic.Bool
	inner := &countingResolver{}
	inner.next = func(string) (*Credentials, error) {
		if slowStarted.CompareAndSwap(false, true) {
			// Only the FIRST post-prime resolve (the background refresh)
			// blocks; later resolves return fresh material immediately.
			<-release
			return &Credentials{
				Env:       map[string]string{"T": "stale-refresh"},
				ExpiresAt: time.Now().Add(time.Minute),
			}, nil
		}
		return &Credentials{
			Env:       map[string]string{"T": "fresh"},
			ExpiresAt: time.Now().Add(time.Minute),
		}, nil
	}
	c := Cached(inner, time.Hour)

	// Prime with a short-TTL credential: the lead clamps to ttl/2, so the
	// refresh window opens at half-life. (The prime itself must not be the
	// slow resolve; flip the flag around it.)
	slowStarted.Store(true)
	primeExpiry := time.Now().Add(400 * time.Millisecond)
	inner.mu.Lock()
	primeNext := inner.next
	inner.next = func(string) (*Credentials, error) {
		return &Credentials{Env: map[string]string{"T": "prime"}, ExpiresAt: primeExpiry}, nil
	}
	inner.mu.Unlock()
	_, gen0, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	inner.mu.Lock()
	inner.next = primeNext
	inner.mu.Unlock()
	slowStarted.Store(false)

	// Enter the refresh window (past half-life, before expiry) and kick the
	// background refresh; it blocks on the release channel.
	time.Sleep(250 * time.Millisecond)
	_, _, err = c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return slowStarted.Load() }, 2*time.Second, time.Millisecond)

	// Invalidate, as a 401 would: whatever the refresh is about to return
	// answers a question about credentials this entry no longer holds.
	c.Invalidate("t", "u", "s")

	// Let it land, then resolve. The caller joins the refresh already in
	// flight rather than opening a second one, so this single call covers
	// both halves: it returns only once that refresh is done, and what it
	// returns must come from a NEW resolve rather than from the superseded
	// result that landed while it waited.
	close(release)
	fresh, genFresh, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	assert.Equal(t, "fresh", fresh.Env["T"], "a superseded refresh must not overwrite fresher credentials")
	assert.Greater(t, genFresh, gen0)

	got, genAfter, err := c.ResolveGen(context.Background(), "t", "u", "s")
	require.NoError(t, err)
	assert.Equal(t, "fresh", got.Env["T"])
	assert.Equal(t, genFresh, genAfter)
}

func TestExpiredMissWaitsForTheRefreshInFlight(t *testing.T) {
	// The refresh-ahead deliberately resolves without holding e.mu, so callers
	// with still-valid credentials are not blocked behind it. A refresh slower
	// than the lead window outlives those credentials, and the next caller
	// then takes the expired path — which single-flights against other
	// foreground callers but knows nothing about the refresh already running.
	//
	// Two resolves against one source at once is not merely wasteful for the
	// refresh_token grant: both load the same refresh token, and an IdP with
	// one-time-use rotation and reuse detection (the Auth0 and Okta defaults)
	// answers the second use by revoking the whole token family. The user's
	// grant is then dead until they consent again.
	const ttl = 600 * time.Millisecond // lead clamps to ttl/2 = 300ms
	var live, peak, calls atomic.Int32
	release := make(chan struct{})
	refreshStarted := make(chan struct{})
	inner := ResolveFunc(func(context.Context, string, string, string) (*Credentials, error) {
		n := live.Add(1)
		defer live.Add(-1)
		for {
			was := peak.Load()
			if n <= was || peak.CompareAndSwap(was, n) {
				break
			}
		}
		if calls.Add(1) == 2 { // the refresh-ahead: slower than the lead window
			close(refreshStarted)
			select {
			case <-release:
			case <-time.After(30 * time.Second): // never block the suite forever
			}
		}
		return &Credentials{
			Env:       map[string]string{"T": fmt.Sprint(calls.Load())},
			ExpiresAt: time.Now().Add(ttl),
		}, nil
	})
	c := Cached(inner, time.Hour)

	primed, _, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)

	time.Sleep(2 * ttl / 3) // inside the lead window, comfortably before expiry
	served, _, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)
	require.Equal(t, primed.Env["T"], served.Env["T"],
		"this caller must have been served from cache and merely kicked the refresh")
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the refresh-ahead never ran")
	}

	time.Sleep(2 * ttl / 3) // the credentials expire under the running refresh
	done := make(chan error, 1)
	go func() {
		_, _, err := c.ResolveGen(ctxT(t), "t", "u", "s")
		done <- err
	}()

	time.Sleep(200 * time.Millisecond) // long enough for a second grant to fire
	close(release)
	select {
	case err := <-done:
		require.NoError(t, err, "the waiting caller must be served, not failed")
	case <-time.After(5 * time.Second):
		t.Fatal("the expired-path caller never returned")
	}
	assert.Equal(t, int32(1), peak.Load(),
		"an expired miss must join the refresh already in flight, not open a second grant")
}

func TestUnproductiveRefreshHoldsFixedCadence(t *testing.T) {
	// A refresh returning the SAME ExpiresAt must not re-arm relative to
	// the shrinking remaining life (geometric acceleration); it re-arms at
	// a fixed skew cadence instead.
	fixedExpiry := time.Now().Add(30 * time.Second)
	inner := &countingResolver{next: func(string) (*Credentials, error) {
		return &Credentials{Env: map[string]string{"T": "same"}, ExpiresAt: fixedExpiry}, nil
	}}
	skew := 200 * time.Millisecond
	c := Cached(inner, skew)

	_, _, err := c.ResolveGen(ctxT(t), "t", "u", "s")
	require.NoError(t, err)

	// Hammer inside what would be the accelerating window: with a fixed
	// cadence at skew, at most ~(elapsed/skew)+2 refreshes may fire.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, _, err = c.ResolveGen(ctxT(t), "t", "u", "s")
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}
	assert.LessOrEqual(t, inner.count(), 6, "unchanged ExpiresAt must refresh at fixed cadence, not accelerate")
}

// TestCancelHelperDoesNotSignalAReapedPid guards the pid the cancel is aimed
// at.
//
// Cmd.Wait reaps the child before it reads the cancel result, so cancelHelper
// runs after the pid is free often enough to matter. The pid it signals is a
// process GROUP id, so aimed at a freed one it is a SIGKILL delivered to
// whatever now holds that pgid. Asking os.Process first is what catches that:
// it declines to signal a pid it has already reaped, where the raw
// syscall.Kill behind it consults nothing.
//
// Asserted against cancelHelper rather than by racing a real helper — the
// window is too narrow to hit on purpose, so the test has to be about the
// property that narrows it. ErrProcessDone and not some other error, because
// os/exec reads exactly that one as "nothing was interrupted" and so declines
// to report a helper that finished a hair early as cancelled.
func TestCancelHelperDoesNotSignalAReapedPid(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	require.NoError(t, cmd.Wait(), "the child is now reaped and its pid is free")

	assert.ErrorIs(t, cancelHelper(cmd.Process), os.ErrProcessDone,
		"cancel must ask os.Process before signalling a pid as a process group")
}

// readPID reads the pid the helper's forked child published for itself.
func readPID(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, err, "helper published %q as its child pid", raw)
	return pid
}

// assertReaped waits for a pid to stop existing.
//
// Asserted directly rather than inferred from how long the resolve took. The
// timing form — "it returned in under WaitDelay, so the group kill must have
// worked" — is a race against the very grace period it is trying to prove
// unnecessary, and it fails on a machine fast enough to reach the kill before
// the group has settled. What the group kill promises is that the child is
// gone; that is what this checks.
func assertReaped(t *testing.T, pid int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return syscall.Kill(pid, syscall.Signal(0)) != nil
	}, 10*time.Second, 20*time.Millisecond,
		"pid %d survived the group kill and still holds the helper's stdout", pid)
}
