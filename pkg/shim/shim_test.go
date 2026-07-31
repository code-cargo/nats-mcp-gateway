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

package shim

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/fakemcp"
	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/proxy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakemcp.EnvFlag) == "1" {
		fakemcp.Main()
		return
	}
	os.Exit(m.Run())
}

// harness is a full stack (NATS + gateway + shim) driven through the shim's
// stdio pipes, exactly the way an MCP client would.
type harness struct {
	stdin  io.WriteCloser
	lines  chan string
	runErr chan error
}

func newHarness(t *testing.T, withGateway bool) *harness {
	t.Helper()
	nc, _ := natstest.Run(t, nil)

	if withGateway {
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
		}, proxy.New(pool, nil).Handler())
		require.NoError(t, err)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = ws.Shutdown(ctx)
		})
	}

	wc, err := wire.NewClient(nc, wire.ClientConfig{Tenant: "acme", Inactivity: 5 * time.Second})
	require.NoError(t, err)

	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	s := New(wc, Config{Server: "fake"})

	h := &harness{stdin: stdinW, lines: make(chan string, 64), runErr: make(chan error, 1)}
	go func() { h.runErr <- s.Run(context.Background(), stdinR, stdoutW) }()
	go func() {
		sc := bufio.NewScanner(stdoutR)
		sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
		for sc.Scan() {
			h.lines <- sc.Text()
		}
		close(h.lines)
	}()
	t.Cleanup(func() { _ = stdinW.Close() })
	return h
}

func (h *harness) send(t *testing.T, line string) {
	t.Helper()
	_, err := h.stdin.Write([]byte(line + "\n"))
	require.NoError(t, err)
}

func (h *harness) next(t *testing.T, timeout time.Duration) string {
	t.Helper()
	select {
	case l, ok := <-h.lines:
		require.True(t, ok, "shim stdout closed unexpectedly")
		return l
	case <-time.After(timeout):
		t.Fatal("timed out waiting for shim output")
		return ""
	}
}

func (h *harness) expectSilence(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case l := <-h.lines:
		t.Fatalf("expected no output, got %q", l)
	case <-time.After(d):
	}
}

// req builds a modern client request line with the required _meta.
func req(id, method string, extraParams string) string {
	params := fmt.Sprintf(`{"_meta":{%q:%q}`, mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion)
	if extraParams != "" {
		params += "," + extraParams
	}
	params += "}"
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":%s}`, id, method, params)
}

func TestShimToolsListRoundTrip(t *testing.T) {
	h := newHarness(t, true)
	h.send(t, req("1", "tools/list", ""))
	line := h.next(t, 10*time.Second)
	m, err := jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	assert.JSONEq(t, `"1"`, string(m.ID))
	assert.Contains(t, string(m.Result), `"echo"`)
}

func TestShimStreamsProgressBeforeResponse(t *testing.T) {
	h := newHarness(t, true)
	h.send(t, req("2", "tools/call", `"name":"slow","_x":null`))
	// note: progressToken rides in _meta alongside protocolVersion
	h.send(t, req("3", "tools/call", `"name":"slow"`))

	// Simplest deterministic check: send one request carrying a token.
	line := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"4","method":"tools/call","params":{"name":"slow","_meta":{%q:%q,"progressToken":"tok-4"}}}`,
		mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion,
	)
	h.send(t, line)

	var got []string
	deadline := time.After(15 * time.Second)
	progress := 0
	for {
		select {
		case l := <-h.lines:
			m, err := jsonrpc.Decode([]byte(l))
			require.NoError(t, err)
			if m.Method == mcpspec.NotifProgress {
				assert.Contains(t, string(m.Params), "tok-4")
				progress++
				continue
			}
			got = append(got, string(m.ID))
			if len(got) == 3 { // responses for ids 2, 3, 4
				assert.Equal(t, 3, progress, "the token-carrying call must yield 3 progress lines")
				return
			}
		case <-deadline:
			t.Fatalf("incomplete: responses=%v progress=%d", got, progress)
		}
	}
}

func TestShimCancelSuppressesResponse(t *testing.T) {
	h := newHarness(t, true)
	h.send(t, req("5", "tools/call", `"name":"wedge"`))
	time.Sleep(300 * time.Millisecond)
	h.send(t, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"5"}}`)

	// A cancelled request gets NO response line...
	h.expectSilence(t, 2*time.Second)

	// ...and the shim keeps working afterwards.
	h.send(t, req("6", "tools/list", ""))
	line := h.next(t, 10*time.Second)
	m, err := jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	assert.JSONEq(t, `"6"`, string(m.ID))
}

func TestShimNoGatewayYieldsLegibleError(t *testing.T) {
	h := newHarness(t, false) // no gateway serving "fake"
	h.send(t, req("7", "tools/list", ""))
	line := h.next(t, 10*time.Second)
	m, err := jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeNoGateway, m.Error.Code)
	assert.JSONEq(t, `"7"`, string(m.ID), "error must carry the caller's id")
}

func TestShimCrashYieldsStreamLost(t *testing.T) {
	h := newHarness(t, true)
	h.send(t, req("8", "tools/call", `"name":"crash"`))
	line := h.next(t, 10*time.Second)
	m, err := jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	require.NotNil(t, m.Error)
	assert.Equal(t, wire.ErrCodeStreamLost, m.Error.Code)
}

func TestShimInjectsMissingProtocolVersion(t *testing.T) {
	h := newHarness(t, true)
	// No _meta at all: the shim must inject it or the gateway rejects.
	h.send(t, `{"jsonrpc":"2.0","id":"9","method":"tools/list","params":{}}`)
	line := h.next(t, 10*time.Second)
	m, err := jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	require.Nil(t, m.Error, "gateway must have accepted the injected version: %s", line)
	var r struct {
		Tools []json.RawMessage `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(m.Result, &r))
	assert.NotEmpty(t, r.Tools)
}

// TestShimRejectsAmbiguousParams covers the shim's refusal to guess.
//
// The shim builds the subject and the Mcp-Name header from params, and the
// gateway then checks the body against exactly those. So a body the shim
// cannot read unambiguously is one it must not publish: whichever reading it
// picked would be a claim about a body that has no single meaning. Each case
// here is answered locally, and nothing reaches the wire.
//
// -32600 rather than the -32020 the gateway answers the same
// mcpspec.AmbiguousKeyError with: at this point no headers exist to disagree
// with the body, so this is simply a request the shim cannot interpret.
func TestShimRejectsAmbiguousParams(t *testing.T) {
	meta := fmt.Sprintf(`"_meta":{%q:%q}`, mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion)
	tests := []struct {
		name   string
		params string
	}{
		{"duplicate name key", `{"name":"a","name":"b",` + meta + `}`},
		{"case-colliding name key", `{"name":"a","NAME":"b",` + meta + `}`},
		{"duplicate key nested in arguments", `{"name":"echo","arguments":{"k":1,"k":2},` + meta + `}`},
		{"name is not a string", `{"name":{"toString":"echo"},` + meta + `}`},
		{"params is an array", `[]`},
		{"colliding protocol version key in _meta", fmt.Sprintf(
			`{"name":"echo","_meta":{%q:%q,"io.modelcontextprotocol/protocolversion":"1999-01-01"}}`,
			mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No gateway: if the shim published anything, the request would
			// hang and time out rather than answer, which is the failure this
			// test is looking for.
			h := newHarness(t, false)
			h.send(t, fmt.Sprintf(
				`{"jsonrpc":"2.0","id":"1","method":"tools/call","params":%s}`, tt.params))

			line := h.next(t, 10*time.Second)
			m, err := jsonrpc.Decode([]byte(line))
			require.NoError(t, err)
			require.NotNil(t, m.Error, "ambiguous params must be refused, got %s", line)
			assert.Equal(t, jsonrpc.CodeInvalidRequest, m.Error.Code)
			assert.JSONEq(t, `"1"`, string(m.ID), "the error must answer the caller's id")
			assert.NotEmpty(t, m.Error.Message, "the message carries which key was ambiguous")
		})
	}
}

// TestShimAcceptsCaseCollisionsInsideToolArguments pins the other side of the
// rule. Tool arguments are opaque caller data whose keys the shim never
// reads, so two of them differing only by case is legal and must travel.
func TestShimAcceptsCaseCollisionsInsideToolArguments(t *testing.T) {
	h := newHarness(t, true)
	h.send(t, req("1", "tools/call", `"name":"echo","arguments":{"Msg":"a","msg":"b"}`))
	line := h.next(t, 10*time.Second)
	m, err := jsonrpc.Decode([]byte(line))
	require.NoError(t, err)
	assert.Nil(t, m.Error, "opaque argument keys are none of the shim's business: %s", line)
}
