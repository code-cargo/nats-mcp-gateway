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

package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend/cred"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

func decodeMsg(t *testing.T, body []byte) *jsonrpc.Message {
	t.Helper()
	m, err := jsonrpc.Decode(body)
	require.NoError(t, err)
	return m
}

// credStack is stack() plus a credential resolver wired the way cmd/gateway.go
// does it: the proxy resolves for the pool key, the factory injects the
// resolved env over the fakemcp base env. perUser is the grain the server's
// auth mode implies — the two grains take different paths through the proxy,
// so it is a parameter rather than a constant.
func credStack(t *testing.T, resolver *cred.CachedResolver, perUser bool) *nats.Conn {
	t.Helper()
	nc, _ := natstest.Run(t, nil)

	pool := backend.NewPool(backend.PoolConfig{}, func(key backend.Key) (backend.Backend, error) {
		env := map[string]string{fakemcp.EnvFlag: "1"}
		user := key.CredSet
		if user == "" {
			user = wire.UserUnattributed
		}
		creds, _, err := resolver.ResolveGen(context.Background(), key.Tenant, user, key.Server)
		if err != nil {
			return nil, err
		}
		for k, v := range creds.Env {
			env[k] = v
		}
		return &backend.StdioBackend{Command: os.Args[0], Env: env}, nil
	}, nil)
	t.Cleanup(pool.Shutdown)

	px := New(pool, nil)
	px.Creds = func(string) (*cred.CachedResolver, bool) { return resolver, perUser }
	ws, err := wire.Serve(nc, wire.ServerConfig{
		Servers:   []string{"fake"},
		KeepAlive: 50 * time.Millisecond,
	}, px.Handler())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})
	return nc
}

func envCall(t *testing.T, nc *nats.Conn, user, envVar string) (pid float64, value string) {
	t.Helper()
	wc, err := wire.NewClient(nc, wire.ClientConfig{Tenant: "acme", User: user, Inactivity: 5 * time.Second})
	require.NoError(t, err)
	frames := doCollect(t, wc, mcpRequest("1", "tools/call",
		map[string]any{"name": "env", "arguments": map[string]any{"name": envVar}}))
	require.NotEmpty(t, frames)
	last := frames[len(frames)-1]
	require.Equal(t, wire.FrameEnd, last.Kind, "expected a result, got %+v", last)
	m := decodeMsg(t, last.Body)
	require.Nil(t, m.Error)
	var r struct {
		PID   float64 `json:"pid"`
		Value string  `json:"value"`
	}
	require.NoError(t, json.Unmarshal(m.Result, &r))
	return r.PID, r.Value
}

func TestE2EPerUserCredentials(t *testing.T) {
	resolver := cred.Cached(cred.ResolveFunc(
		func(_ context.Context, _, user, _ string) (*cred.Credentials, error) {
			return &cred.Credentials{Env: map[string]string{"TOKEN": "tok-" + user}}, nil
		},
	), 0)
	nc := credStack(t, resolver, true)

	pidA, tokA := envCall(t, nc, "alice", "TOKEN")
	pidB, tokB := envCall(t, nc, "bob", "TOKEN")
	pidA2, tokA2 := envCall(t, nc, "alice", "TOKEN")

	assert.Equal(t, "tok-alice", tokA, "alice's process must carry alice's credential")
	assert.Equal(t, "tok-bob", tokB, "bob's process must carry bob's credential")
	assert.NotEqual(t, pidA, pidB, "per-user servers must not share a process across users")
	assert.Equal(t, pidA, pidA2, "the same user must reuse their process")
	assert.Equal(t, tokA, tokA2)
}

// recordingResolver hands out a per-user token and remembers every identity it
// was asked to resolve, so a test can assert on resolutions that must never
// happen at all — an error code alone cannot distinguish "refused" from
// "resolved and then failed for some other reason".
func recordingResolver(t *testing.T) (*cred.CachedResolver, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var asked []string
	r := cred.Cached(cred.ResolveFunc(
		func(_ context.Context, _, user, _ string) (*cred.Credentials, error) {
			mu.Lock()
			asked = append(asked, user)
			mu.Unlock()
			return &cred.Credentials{Env: map[string]string{"TOKEN": "tok-" + user}}, nil
		},
	), 0)
	return r, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), asked...)
	}
}

// TestE2EPerUserServerRefusesUnattributedCaller is the fail-closed rule on the
// per-user credential grain.
//
// "_" is the placeholder for "this deployment has no per-user auth". It is not
// an identity, and it is the one {user} token every caller in a tenant can
// reach: a deployment that grants mcp.v1.req.{tenant}.> rather than
// …{tenant}.{user}.> makes the token forgeable outright. Because {user} drives
// credential SELECTION and the pool's CredSet, resolving it on a per-user
// server does not merely mislabel the request — it hands every unattributed
// caller in the tenant one shared credential and one shared backend process,
// on precisely the servers configured so that must not happen.
//
// The subject cannot support the grain the server asked for, so the request is
// refused before any credential is resolved. That turns a silent
// credential-sharing misconfiguration into a legible error on the first
// request.
func TestE2EPerUserServerRefusesUnattributedCaller(t *testing.T) {
	resolver, asked := recordingResolver(t)
	nc := credStack(t, resolver, true)

	wc, err := wire.NewClient(nc, wire.ClientConfig{Tenant: "acme", Inactivity: 5 * time.Second})
	require.NoError(t, err)
	frames := doCollect(t, wc, mcpRequest("1", "tools/call",
		map[string]any{"name": "env", "arguments": map[string]any{"name": "TOKEN"}}))
	require.NotEmpty(t, frames)

	last := frames[len(frames)-1]
	require.Equal(t, wire.FrameErr, last.Kind,
		"an unattributed caller reached a per-user backend: %+v", last)
	m := decodeMsg(t, last.Body)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeCredentialUnavailable, m.Error.Code)
	assert.Contains(t, m.Error.Message, "do not retry",
		"a forgeable subject token will not become an identity on the next attempt")

	assert.NotContains(t, asked(), wire.UserUnattributed,
		"the placeholder must never be resolved as though it were an identity")
}

// TestE2ESharedServerStillServesUnattributedCallers is the other half of the
// rule above: "_" is the DOCUMENTED token for a deployment without per-user
// auth, so a server whose credentials are shared per tenant must keep serving
// it. Refusing the placeholder outright would break every deployment that has
// no per-user NATS auth to begin with.
func TestE2ESharedServerStillServesUnattributedCallers(t *testing.T) {
	resolver, asked := recordingResolver(t)
	nc := credStack(t, resolver, false)

	_, tok := envCall(t, nc, "", "TOKEN")
	assert.Equal(t, "tok-"+wire.UserUnattributed, tok)
	assert.Contains(t, asked(), wire.UserUnattributed,
		"a shared server resolves one credential for the whole tenant, under the placeholder")
}

func TestE2ECredentialFailureMapsToWireError(t *testing.T) {
	boom := errors.New("controller unreachable")
	resolver := cred.Cached(cred.ResolveFunc(
		func(_ context.Context, _, user, _ string) (*cred.Credentials, error) {
			if user == "denied" {
				return nil, cred.Terminal(fmt.Errorf("user %q not authorized", user))
			}
			return nil, boom
		},
	), 0)
	nc := credStack(t, resolver, true)

	errFrameFor := func(user string) *wire.Frame {
		wc, err := wire.NewClient(nc, wire.ClientConfig{Tenant: "acme", User: user, Inactivity: 5 * time.Second})
		require.NoError(t, err)
		frames := doCollect(t, wc, mcpRequest("1", "tools/call",
			map[string]any{"name": "env", "arguments": map[string]any{"name": "TOKEN"}}))
		require.NotEmpty(t, frames)
		return &frames[len(frames)-1]
	}

	f := errFrameFor("someone")
	require.Equal(t, wire.FrameErr, f.Kind)
	m := decodeMsg(t, f.Body)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeCredentialUnavailable, m.Error.Code)
	assert.Contains(t, m.Error.Message, "retry later", "transient failures must invite a retry")

	f = errFrameFor("denied")
	require.Equal(t, wire.FrameErr, f.Kind)
	m = decodeMsg(t, f.Body)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeCredentialUnavailable, m.Error.Code)
	assert.Contains(t, m.Error.Message, "do not retry", "authoritative refusals must say so")
}

func TestE2ECredentialFailureDoesNotEchoTheResolverDetail(t *testing.T) {
	// A resolver's error text is whatever its source emitted: the helper's
	// stderr, the IdP's response body, the expanded path of an assertion file.
	// The caller is precisely the party the gateway exists to keep that
	// material away from, and it can reach this path at will — a request for a
	// server whose credentials it is not entitled to is enough.
	const detail = "helper stderr: sts assume-role arn:aws:iam::918273:role/prod " +
		"failed reading /var/run/secrets/acme/u1.jwt (token AKIAWOULDBEBAD)"
	resolver := cred.Cached(cred.ResolveFunc(
		func(_ context.Context, _, user, _ string) (*cred.Credentials, error) {
			if user == "denied" {
				return nil, cred.Terminal(errors.New(detail))
			}
			return nil, errors.New(detail)
		},
	), 0)
	// Per-user, because the resolver above answers on the caller's identity:
	// under the shared grain both loop iterations would resolve as
	// wire.UserUnattributed and the terminal refusal would never be reached,
	// leaving half of what this test covers silently uncovered.
	nc := credStack(t, resolver, true)

	for _, user := range []string{"someone", "denied"} {
		wc, err := wire.NewClient(nc, wire.ClientConfig{Tenant: "acme", User: user, Inactivity: 5 * time.Second})
		require.NoError(t, err)
		frames := doCollect(t, wc, mcpRequest("1", "tools/call",
			map[string]any{"name": "env", "arguments": map[string]any{"name": "TOKEN"}}))
		require.NotEmpty(t, frames)
		f := frames[len(frames)-1]
		require.Equal(t, wire.FrameErr, f.Kind)
		m := decodeMsg(t, f.Body)
		require.NotNil(t, m.Error)

		assert.NotContains(t, m.Error.Message, "arn:aws",
			"the resolver's error text must not travel to the caller")
		assert.NotContains(t, m.Error.Message, "u1.jwt")
		assert.NotContains(t, m.Error.Message, "AKIAWOULDBEBAD")
		// The category still has to reach the caller — it is what tells a client
		// whether re-issuing can help — and a reference has to, or an operator
		// holding a user's screenshot cannot find the log line that says why.
		assert.Contains(t, m.Error.Message, "gateway ref ")
	}
}
