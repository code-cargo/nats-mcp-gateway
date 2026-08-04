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
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend/cred"
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
	return stackLogged(t, natsOpts, nil)
}

// stackLogged is stack with the proxy's logger under the test's control, for
// the one thing the gateway promises that is only observable in its output.
func stackLogged(t *testing.T, natsOpts *server.Options, log *slog.Logger) (*nats.Conn, *wire.Client) {
	t.Helper()
	return stackFactory(t, natsOpts, log, func(backend.Key) (backend.Backend, error) {
		return &backend.StdioBackend{
			Command: os.Args[0],
			Env:     map[string]string{fakemcp.EnvFlag: "1"},
		}, nil
	})
}

// stackFactory is stackLogged with the pool's factory under the test's
// control, for the refusals that happen at spawn rather than on the request.
func stackFactory(
	t *testing.T, natsOpts *server.Options, log *slog.Logger, factory backend.Factory,
) (*nats.Conn, *wire.Client) {
	t.Helper()
	nc, _ := natstest.Run(t, natsOpts)

	pool := backend.NewPool(backend.PoolConfig{}, factory, nil)
	t.Cleanup(pool.Shutdown)

	ws, err := wire.Serve(nc, wire.ServerConfig{
		Servers:   []string{"fake"},
		KeepAlive: 50 * time.Millisecond,
	}, New(pool, log).Handler())
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

	// Through NameField, like the shim: a helper that reads whichever of
	// name/uri it finds would build fixtures no real client can produce.
	name, _ := params[mcpspec.NameField(method)].(string)
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

// logSink collects records as flat maps, keeping the attrs a With() chain
// accumulated, because the audit line's identifying fields are attached that
// way and asserting on a formatted string would pass on a substring match the
// operator's log query would not make.
type logSink struct {
	mu   *sync.Mutex
	recs *[]map[string]any
	pre  []slog.Attr
}

func newLogSink() (*slog.Logger, func() []map[string]any) {
	var mu sync.Mutex
	recs := &[]map[string]any{}
	return slog.New(&logSink{mu: &mu, recs: recs}), func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]any(nil), *recs...)
	}
}

func (h *logSink) Enabled(context.Context, slog.Level) bool { return true }

func (h *logSink) Handle(_ context.Context, r slog.Record) error {
	m := map[string]any{"msg": r.Message}
	for _, a := range h.pre {
		m[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value.Any(); return true })
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, m)
	return nil
}

func (h *logSink) WithAttrs(as []slog.Attr) slog.Handler {
	pre := make([]slog.Attr, 0, len(h.pre)+len(as))
	return &logSink{mu: h.mu, recs: h.recs, pre: append(append(pre, h.pre...), as...)}
}

func (h *logSink) WithGroup(string) slog.Handler { return h }

// waitLine waits for the record one method wrote under msg. The summary line
// is written from a defer, after the response reaches the caller, so a test
// that read the records the moment its reply arrived would race the thing it
// came to assert on.
func waitLine(t *testing.T, records func() []map[string]any, msg, method string) map[string]any {
	t.Helper()
	var line map[string]any
	require.Eventually(t, func() bool {
		for _, r := range records() {
			if r["msg"] == msg && r["method"] == method {
				line = r
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond, "no %q line logged for %s", msg, method)
	return line
}

// requestLine waits for the summary line of one method's request.
func requestLine(t *testing.T, records func() []map[string]any, method string) map[string]any {
	t.Helper()
	return waitLine(t, records, "request", method)
}

// TestAuditRecordsTheNameTheCheckValidated pins the audit trail to the request
// that actually ran.
//
// The gateway is a policy plane and its per-request line is the record of what
// crossed it, so the name on that line has to be the name the integrity check
// authorized — not a caller-supplied header that happened to be nearby. The
// Mcp-Name header is only compared to the body for methods that HAVE a name;
// on an unnamed method nothing constrains it, and a caller may set it to
// whatever they would rather the log said. Filing a tools/list under
// name=delete_repo is a lie in the one place an operator goes to find out what
// happened.
func TestAuditRecordsTheNameTheCheckValidated(t *testing.T) {
	log, records := newLogSink()
	nc, wc := stackLogged(t, nil, log)

	frames := doCollect(t, wc, mcpRequest("1", "tools/call",
		map[string]any{"name": "echo", "arguments": map[string]any{}}))
	require.Equal(t, wire.FrameEnd, frames[len(frames)-1].Kind)
	assert.Equal(t, "echo", requestLine(t, records, "tools/call")["name"],
		"a named call must be audited under the name that executed")

	// tools/list carries no name, so Check never looks at Mcp-Name and the
	// request below is entirely valid — which is what makes the header a free
	// field for the caller to write the audit trail with.
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"2","method":"tools/list","params":{"_meta":{%q:%q}}}`,
		mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion)
	subj, err := wire.BuildSubject("", "acme", "_", "fake", "tools/list", "")
	require.NoError(t, err)
	msg := publishRaw(t, nc, subj, nats.Header{
		wire.HeaderWire:            []string{wire.WireVersion},
		wire.HeaderMethod:          []string{"tools/list"},
		wire.HeaderName:            []string{"delete_repo"},
		wire.HeaderProtocolVersion: []string{mcpspec.ProtocolVersion},
	}, body)
	require.Equal(t, string(wire.FrameEnd), msg.Header.Get(wire.HeaderFrame),
		"the unchecked header must not change whether the request is valid")

	line := requestLine(t, records, "tools/list")
	assert.NotContains(t, line, "name",
		"the audit line named a request whose name the check never validated")
}

// publishRaw sends one request with its subject, headers and bytes entirely
// under the test's control, and returns the first reply frame. The wire client
// builds only conformant requests by construction, so this is the only way to
// state a request whose header disagrees with its body — or with the check.
func publishRaw(t *testing.T, nc *nats.Conn, subj string, h nats.Header, body string) *nats.Msg {
	t.Helper()
	reply := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(reply)
	require.NoError(t, err)
	require.NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: subj, Reply: reply, Data: []byte(body), Header: h,
	}))
	msg, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	return msg
}

// TestRejectionLineKeepsTheClaimedName is the other side of the audit rule: a
// name the check did not validate must not be recorded AS the request's name,
// but it must still be recorded.
//
// A refusal for a reason that has nothing to do with the name — an
// unsupported protocol version here — is where that matters. The reason text
// names the version, not the tool, so without claimed_name the operator
// answering "which of my tools started failing" has a rejection line with
// nothing in it to answer from. The distinct key is what keeps the two
// readings apart: name is what ran, claimed_name is what was asserted by
// someone whose request did not run.
func TestRejectionLineKeepsTheClaimedName(t *testing.T) {
	log, records := newLogSink()
	nc, _ := stackLogged(t, nil, log)

	// Header and body agree on the name and on a version that is consistent
	// but unsupported, so Check clears every name rule and refuses on the
	// version — the claim is honest and still unauthorized.
	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"echo","_meta":{%q:%q}}}`,
		mcpspec.MetaProtocolVersion, mcpspec.LegacyProtocolVersion)
	subj, err := wire.BuildSubject("", "acme", "_", "fake", mcpspec.MethodToolsCall, "echo")
	require.NoError(t, err)
	msg := publishRaw(t, nc, subj, nats.Header{
		wire.HeaderWire:            []string{wire.WireVersion},
		wire.HeaderMethod:          []string{mcpspec.MethodToolsCall},
		wire.HeaderName:            []string{"echo"},
		wire.HeaderProtocolVersion: []string{mcpspec.LegacyProtocolVersion},
	}, body)
	require.Equal(t, string(wire.FrameErr), msg.Header.Get(wire.HeaderFrame))

	rejected := waitLine(t, records, "integrity check rejected request", mcpspec.MethodToolsCall)
	assert.Equal(t, "echo", rejected["claimed_name"],
		"a rejection that is not about the name still has to say which tool was attempted")

	line := requestLine(t, records, mcpspec.MethodToolsCall)
	assert.NotContains(t, line, "name",
		"the summary line named a request the check refused")
}

// TestFactoryIdentityRefusalIsACredentialFailure holds the two halves of the
// unattributed-credential refusal to one wire code.
//
// The proxy refuses an unattributed caller on a per-user server itself, but
// the pool factory refuses the same thing again for the request that was
// keyed before a reload changed the grain. Everything else a factory can fail
// with is a spawn problem, which the client is told is a lost stream — and a
// caller told "the stream broke" for a credential-grain refusal re-issues
// blind against a condition that has a real answer (-32014 says the gateway
// could not resolve credentials; the message says whether waiting helps).
func TestFactoryIdentityRefusalIsACredentialFailure(t *testing.T) {
	_, wc := stackFactory(t, nil, nil, func(key backend.Key) (backend.Backend, error) {
		return nil, fmt.Errorf(
			"server %q resolves credentials per user and this backend was keyed without one: %w",
			key.Server, cred.ErrIdentityRequired)
	})

	frames := doCollect(t, wc, mcpRequest("1", "tools/list", nil))
	require.NotEmpty(t, frames)
	last := frames[len(frames)-1]
	require.Equal(t, wire.FrameErr, last.Kind)
	m := decodeMsg(t, last.Body)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeCredentialUnavailable, m.Error.Code,
		"a credential-grain refusal reported as a lost stream")
	assert.Contains(t, m.Error.Message, "retry later",
		"unlike the request-path refusal, this one clears once the reload lands")
}

// TestFactoryCredentialFailureIsACredentialFailure is the identity refusal's
// sibling: the factory's RESOLVE failing, rather than its grain check.
//
// The proxy resolves for the pool key and the factory resolves again to build
// the backend, so the second can miss a cache the first hit — a TTL boundary,
// a concurrent 401 Invalidate — and fail where the request path succeeded.
// That failure used to arrive as -32010, "the stream broke, re-issue", with
// cred.CallerMessage's "do not retry" sitting inside it: a client switching on
// the code (which is what codes are for) retry-loops a permanent refusal.
func TestFactoryCredentialFailureIsACredentialFailure(t *testing.T) {
	const detail = "helper stderr: sts assume-role failed (token AKIAWOULDBEBAD)"
	_, wc := stackFactory(t, nil, nil, func(key backend.Key) (backend.Backend, error) {
		ref := cred.FailureRef()
		return nil, &cred.Unavailable{Message: fmt.Sprintf("%s/%s: %s", key.Tenant, key.Server,
			cred.CallerMessage(cred.Terminal(errors.New(detail)), ref))}
	})

	frames := doCollect(t, wc, mcpRequest("1", "tools/list", nil))
	require.NotEmpty(t, frames)
	last := frames[len(frames)-1]
	require.Equal(t, wire.FrameErr, last.Kind)
	m := decodeMsg(t, last.Body)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeCredentialUnavailable, m.Error.Code,
		"a terminal credential refusal reported as a lost stream")
	assert.Contains(t, m.Error.Message, "do not retry")
	assert.NotContains(t, m.Error.Message, "AKIAWOULDBEBAD",
		"the resolver's error text must not travel to the caller")
}

// TestPoolFailureDetailStaysInTheGatewayLog covers the first of the two sites
// that handed the pool's error to the caller verbatim.
//
// Everything the factory can fail with ends up here, and it is all
// gateway-internal: the MCP server's executable path and argv, a gateway-pod
// temp path, the name of a server this instance does not serve. The caller
// acts on the code, so it gets the category and a ref; the detail goes to the
// log the ref points at.
func TestPoolFailureDetailStaysInTheGatewayLog(t *testing.T) {
	const detail = `spawn: exec "/opt/acme/bin/mcp-github" --token AKIAWOULDBEBAD: no such file or directory`
	log, records := newLogSink()
	_, wc := stackFactory(t, nil, log, func(backend.Key) (backend.Backend, error) {
		return nil, errors.New(detail)
	})

	frames := doCollect(t, wc, mcpRequest("1", "tools/list", nil))
	require.NotEmpty(t, frames)
	last := frames[len(frames)-1]
	require.Equal(t, wire.FrameErr, last.Kind)
	m := decodeMsg(t, last.Body)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeStreamLost, m.Error.Code)
	assert.NotContains(t, m.Error.Message, "/opt/acme/bin", "a backend path reached the caller")
	assert.NotContains(t, m.Error.Message, "AKIAWOULDBEBAD", "a backend argv reached the caller")
	assert.Contains(t, m.Error.Message, "re-issue", "-32010 still means the request can be retried")

	line := waitLine(t, records, "backend unavailable", mcpspec.MethodToolsList)
	assert.Contains(t, fmt.Sprint(line["err"]), "AKIAWOULDBEBAD",
		"suppressed for the caller, not for the operator")
	ref, _ := line["ref"].(string)
	require.NotEmpty(t, ref, "the log line needs the ref the caller was given")
	assert.Contains(t, m.Error.Message, ref, "the caller's ref must find the log line")
}

// writeFailBackend connects, then fails every write — a broken pipe to a
// subprocess that is still running, which is the shape Mux.Call reports as
// something other than ErrConnDead.
type writeFailBackend struct{ detail string }

func (b *writeFailBackend) Connect(context.Context) (backend.Conn, error) {
	return &writeFailConn{detail: b.detail, closed: make(chan struct{})}, nil
}

type writeFailConn struct {
	detail string
	closed chan struct{}
	once   sync.Once
}

// Read blocks until Close, so the mux's read loop does not declare the
// connection dead before the write is attempted.
func (c *writeFailConn) Read(ctx context.Context) (*jsonrpc.Message, error) {
	select {
	case <-c.closed:
		return nil, errors.New("connection closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *writeFailConn) Write(context.Context, *jsonrpc.Message) error {
	return errors.New(c.detail)
}

func (c *writeFailConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// TestBackendCallFailureDetailStaysInTheGatewayLog is the same suppression on
// the second site: a mux failure that is neither cancellation nor a dead
// connection, whose text is whatever the transport wrote about the pipe or the
// endpoint it was talking to.
func TestBackendCallFailureDetailStaysInTheGatewayLog(t *testing.T) {
	const detail = `write /tmp/natsmcp-3f9a/acme-github.sock: broken pipe`
	log, records := newLogSink()
	_, wc := stackFactory(t, nil, log, func(backend.Key) (backend.Backend, error) {
		return &writeFailBackend{detail: detail}, nil
	})

	frames := doCollect(t, wc, mcpRequest("1", "tools/list", nil))
	require.NotEmpty(t, frames)
	last := frames[len(frames)-1]
	require.Equal(t, wire.FrameErr, last.Kind)
	m := decodeMsg(t, last.Body)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeStreamLost, m.Error.Code)
	assert.NotContains(t, m.Error.Message, "/tmp/natsmcp-3f9a", "a gateway path reached the caller")
	assert.Contains(t, m.Error.Message, "re-issue")

	line := waitLine(t, records, "backend call failed", mcpspec.MethodToolsList)
	assert.Contains(t, fmt.Sprint(line["err"]), "/tmp/natsmcp-3f9a",
		"suppressed for the caller, not for the operator")
	ref, _ := line["ref"].(string)
	require.NotEmpty(t, ref)
	assert.Contains(t, m.Error.Message, ref, "the caller's ref must find the log line")
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
