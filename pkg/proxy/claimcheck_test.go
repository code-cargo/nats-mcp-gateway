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
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// The full gateway path with claim-check: the fakemcp "huge" tool returns a
// >2MiB result through a 1MB max_payload NATS — impossible inline, delivered
// transparently via the claim store.
func TestE2EClaimCheckHugeResult(t *testing.T) {
	natsOpts := &server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: t.TempDir(),
		MaxPayload: 1024 * 1024,
	}
	srv, err := server.NewServer(natsOpts)
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	claims := &wire.ObjectClaims{JS: js}

	pool := backend.NewPool(backend.PoolConfig{}, func(key backend.Key) (backend.Backend, error) {
		return &backend.StdioBackend{
			Command: os.Args[0],
			Env:     map[string]string{fakemcp.EnvFlag: "1"},
		}, nil
	}, nil)
	t.Cleanup(pool.Shutdown)

	ws, err := wire.Serve(nc, wire.ServerConfig{
		Servers:   []string{"fake"},
		KeepAlive: 50 * time.Millisecond,
		Claims:    claims,
	}, New(pool, nil).Handler())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})

	// Without claims: the huge result is a -32012 dead end (status quo).
	plain, err := wire.NewClient(nc, wire.ClientConfig{Tenant: "acme", Inactivity: 5 * time.Second})
	require.NoError(t, err)
	frames := doCollect(t, plain, mcpRequest("1", "tools/call", map[string]any{"name": "huge"}))
	last := frames[len(frames)-1]
	require.Equal(t, wire.FrameErr, last.Kind)
	m := decodeMsg(t, last.Body)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodePayloadTooLarge, m.Error.Code)

	// With claims: the same call delivers the full result.
	wc, err := wire.NewClient(nc, wire.ClientConfig{Tenant: "acme", Claims: claims, Inactivity: 10 * time.Second})
	require.NoError(t, err)
	frames = doCollect(t, wc, mcpRequest("1", "tools/call", map[string]any{"name": "huge"}))
	last = frames[len(frames)-1]
	require.Equal(t, wire.FrameEnd, last.Kind, "claimed huge result must arrive, got %+v", last)
	m = decodeMsg(t, last.Body)
	require.Nil(t, m.Error)
	assert.Greater(t, len(m.Result), 2*1024*1024, "the full >2MiB result must round-trip")
}
