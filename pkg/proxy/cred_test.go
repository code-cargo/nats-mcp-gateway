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
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
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

// credStack is stack() plus a per-user credential resolver wired the way
// cmd/gateway.go does it: the proxy resolves for the pool key, the factory
// injects the resolved env over the fakemcp base env.
func credStack(t *testing.T, resolver *cred.CachedResolver) *nats.Conn {
	t.Helper()
	natsOpts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, MaxPayload: 8 * 1024 * 1024}
	srv, err := server.NewServer(natsOpts)
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

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
	px.Creds = func(string) (*cred.CachedResolver, bool) { return resolver, true }
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
	nc := credStack(t, resolver)

	pidA, tokA := envCall(t, nc, "alice", "TOKEN")
	pidB, tokB := envCall(t, nc, "bob", "TOKEN")
	pidA2, tokA2 := envCall(t, nc, "alice", "TOKEN")

	assert.Equal(t, "tok-alice", tokA, "alice's process must carry alice's credential")
	assert.Equal(t, "tok-bob", tokB, "bob's process must carry bob's credential")
	assert.NotEqual(t, pidA, pidB, "per-user servers must not share a process across users")
	assert.Equal(t, pidA, pidA2, "the same user must reuse their process")
	assert.Equal(t, tokA, tokA2)
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
	nc := credStack(t, resolver)

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
