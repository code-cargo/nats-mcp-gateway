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
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakemcp.EnvFlag) == "1" {
		fakemcp.Main()
		return
	}
	os.Exit(m.Run())
}

// stack boots embedded NATS + pool + proxy + wire server: the whole gateway
// minus the CLI.
func stack(t *testing.T, natsOpts *server.Options) (*nats.Conn, *wire.Client) {
	t.Helper()
	if natsOpts == nil {
		natsOpts = &server.Options{}
	}
	natsOpts.Host = "127.0.0.1"
	natsOpts.Port = -1
	natsOpts.NoLog = true
	natsOpts.NoSigs = true
	srv, err := server.NewServer(natsOpts)
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

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
	}, New(pool, nil).Handler())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})

	wc, err := wire.NewClient(nc, wire.ClientConfig{Tenant: "acme", Inactivity: 5 * time.Second})
	require.NoError(t, err)
	return nc, wc
}

// mcpRequest builds a spec-correct 2026-07-28 request body plus its wire
// envelope, the way the shim would.
func mcpRequest(id, method string, params map[string]any) *wire.Request {
	if params == nil {
		params = map[string]any{}
	}
	meta, _ := params["_meta"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta[mcpspec.MetaProtocolVersion] = mcpspec.ProtocolVersion
	params["_meta"] = meta

	name := ""
	if n, ok := params["name"].(string); ok {
		name = n
	}
	if u, ok := params["uri"].(string); ok {
		name = u
	}
	raw, _ := json.Marshal(params)
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": method, "params": json.RawMessage(raw),
	})
	return &wire.Request{
		Server:          "fake",
		Method:          method,
		Name:            name,
		ProtocolVersion: mcpspec.ProtocolVersion,
		Body:            body,
	}
}

func doCollect(t *testing.T, wc *wire.Client, req *wire.Request) []wire.Frame {
	t.Helper()
	s, err := wc.Do(context.Background(), req)
	require.NoError(t, err)
	var frames []wire.Frame
	deadline := time.After(15 * time.Second)
	for {
		select {
		case f, ok := <-s.C:
			if !ok {
				return frames
			}
			frames = append(frames, f)
			if f.Kind.Terminal() {
				for range s.C {
				}
				return frames
			}
		case <-deadline:
			t.Fatal("stream did not terminate")
		}
	}
}

func TestE2EToolsList(t *testing.T) {
	_, wc := stack(t, nil)
	frames := doCollect(t, wc, mcpRequest("1", "tools/list", nil))
	require.Len(t, frames, 1)
	assert.Equal(t, wire.FrameEnd, frames[0].Kind)
	assert.Contains(t, string(frames[0].Body), `"echo"`)
}

func TestE2ESlowStreamsProgressInOrderBeforeResponse(t *testing.T) {
	_, wc := stack(t, nil)
	req := mcpRequest("2", "tools/call", map[string]any{
		"name":  "slow",
		"_meta": map[string]any{"progressToken": "cli-tok"},
	})
	frames := doCollect(t, wc, req)

	require.Len(t, frames, 4, "3 progress notifications then the response")
	for i := 0; i < 3; i++ {
		assert.Equal(t, wire.FrameMsg, frames[i].Kind)
		m, err := jsonrpc.Decode(frames[i].Body)
		require.NoError(t, err)
		assert.Equal(t, mcpspec.NotifProgress, m.Method)
		assert.Contains(t, string(m.Params), fmt.Sprintf(`"progress":%d`, i+1),
			"progress must arrive in order")
		assert.Contains(t, string(m.Params), `"cli-tok"`,
			"caller's own progressToken must be restored")
	}
	assert.Equal(t, wire.FrameEnd, frames[3].Kind)
	assert.Contains(t, string(frames[3].Body), "done")
}

func TestE2ECrashYieldsStreamLostNotHang(t *testing.T) {
	_, wc := stack(t, nil)
	frames := doCollect(t, wc, mcpRequest("3", "tools/call", map[string]any{"name": "crash"}))
	require.Len(t, frames, 1)
	assert.Equal(t, wire.FrameErr, frames[0].Kind)
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeStreamLost, m.Error.Code)
	assert.JSONEq(t, `"3"`, string(m.ID))

	// And the gateway must recover: next call spawns a fresh subprocess.
	frames = doCollect(t, wc, mcpRequest("4", "tools/call", map[string]any{"name": "echo", "arguments": map[string]any{}}))
	require.Len(t, frames, 1)
	assert.Equal(t, wire.FrameEnd, frames[0].Kind)
}

func TestE2EHugeResultBecomesPayloadError(t *testing.T) {
	_, wc := stack(t, &server.Options{MaxPayload: 1024 * 1024})
	frames := doCollect(t, wc, mcpRequest("5", "tools/call", map[string]any{"name": "huge"}))
	require.Len(t, frames, 1)
	assert.Equal(t, wire.FrameErr, frames[0].Kind)
	m, err := jsonrpc.Decode(frames[0].Body)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodePayloadTooLarge, m.Error.Code)
	assert.Contains(t, string(m.Error.Data), "limit")
}

func TestE2EIntegrityRejectsForgedBody(t *testing.T) {
	nc, _ := stack(t, nil)

	// Publish a request whose subject says tools/list but whose body calls a
	// tool: the NATS-permission dodge the integrity check exists to stop.
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"6","method":"tools/call","params":{"name":"echo","_meta":{%q:%q}}}`,
		mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion)
	reply := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(reply)
	require.NoError(t, err)
	subj, err := wire.BuildSubject("", "acme", "_", "fake", "tools/list", "")
	require.NoError(t, err)
	require.NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: subj,
		Reply:   reply,
		Data:    []byte(body),
		Header: nats.Header{
			wire.HeaderWire:            []string{wire.WireVersion},
			wire.HeaderMethod:          []string{"tools/list"},
			wire.HeaderProtocolVersion: []string{mcpspec.ProtocolVersion},
		},
	}))
	msg, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, string(wire.FrameErr), msg.Header.Get(wire.HeaderFrame))
	m, err := jsonrpc.Decode(msg.Data)
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, mcpspec.ErrHeaderMismatch, m.Error.Code)
}

func TestE2ECancelWedgedTool(t *testing.T) {
	_, wc := stack(t, nil)
	s, err := wc.Do(context.Background(), mcpRequest("7", "tools/call", map[string]any{"name": "wedge"}))
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond) // let it reach the backend
	require.NoError(t, s.Cancel([]byte(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"7"}}`)))

	deadline := time.After(10 * time.Second)
	for {
		select {
		case f, ok := <-s.C:
			require.True(t, ok, "stream closed without terminal frame")
			if f.Kind.Terminal() {
				assert.Equal(t, wire.FrameEnd, f.Kind)
				assert.Empty(t, f.Body, "cancelled request must end with the empty-body end frame")
				return
			}
		case <-deadline:
			t.Fatal("cancel did not terminate the stream")
		}
	}
}

func TestE2EPermissions(t *testing.T) {
	// Two users: "reader" may only publish tools.list; "admin" may publish
	// everything. Both may use inboxes. The gateway user is separate.
	opts := &server.Options{
		Users: []*server.User{
			{
				Username: "gateway", Password: "gw",
				Permissions: &server.Permissions{
					Publish:   &server.SubjectPermission{Allow: []string{"_INBOX.>"}},
					Subscribe: &server.SubjectPermission{Allow: []string{"mcp.v1.req.>", "_INBOX.>", "$SRV.>"}},
				},
			},
			{
				Username: "reader", Password: "r",
				Permissions: &server.Permissions{
					// User token "_" (this client is unattributed); reader may
					// publish only tools/list for the fake server.
					Publish:   &server.SubjectPermission{Allow: []string{"mcp.v1.req.acme._.fake.tools.list._", "_INBOX.>"}},
					Subscribe: &server.SubjectPermission{Allow: []string{"_INBOX.>"}},
				},
			},
		},
	}
	opts.Host = "127.0.0.1"
	opts.Port = -1
	opts.NoLog = true
	opts.NoSigs = true
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	t.Cleanup(srv.Shutdown)

	// Gateway on its own user.
	gwConn, err := nats.Connect(srv.ClientURL(), nats.UserInfo("gateway", "gw"))
	require.NoError(t, err)
	t.Cleanup(gwConn.Close)
	backendSpawned := false
	pool := backend.NewPool(backend.PoolConfig{}, func(key backend.Key) (backend.Backend, error) {
		backendSpawned = true
		return &backend.StdioBackend{Command: os.Args[0], Env: map[string]string{fakemcp.EnvFlag: "1"}}, nil
	}, nil)
	t.Cleanup(pool.Shutdown)
	ws, err := wire.Serve(gwConn, wire.ServerConfig{Servers: []string{"fake"}}, New(pool, nil).Handler())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})

	// Reader user: tools/list is allowed and works end to end.
	rConn, err := nats.Connect(srv.ClientURL(), nats.UserInfo("reader", "r"))
	require.NoError(t, err)
	t.Cleanup(rConn.Close)
	rc, err := wire.NewClient(rConn, wire.ClientConfig{Tenant: "acme", Inactivity: 3 * time.Second})
	require.NoError(t, err)

	frames := doCollect(t, rc, mcpRequest("1", "tools/list", nil))
	require.Len(t, frames, 1)
	require.Equal(t, wire.FrameEnd, frames[0].Kind)

	// tools/call is NOT in the reader's permissions: NATS refuses the
	// publish and reports it as an async connection error, which the wire
	// client converts into a FAST permission-denied failure — not an
	// inactivity timeout.
	backendSpawned = false
	start := time.Now()
	s, err := rc.Do(context.Background(), mcpRequest("2", "tools/call", map[string]any{
		"name": "echo", "arguments": map[string]any{},
	}))
	require.NoError(t, err)
	var terminal wire.Frame
	for f := range s.C {
		terminal = f
	}
	assert.Equal(t, wire.FrameErr, terminal.Kind)
	require.NotNil(t, terminal.Err)
	assert.Equal(t, wire.ErrCodePermissionDenied, terminal.Err.Code)
	assert.Less(t, time.Since(start), 2*time.Second,
		"denied publish must fail fast, not wait out the inactivity timer")
	assert.False(t, backendSpawned, "a denied publish must never reach the backend")
}
