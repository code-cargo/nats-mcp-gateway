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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
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

// One client line past the cap must cost that line and nothing else. Read
// through bufio.Scanner it cost the session: ErrTooLong ends the scan, Run
// returns, and every concurrent request is abandoned with no error frame while
// the MCP client watches its server process exit.
func TestShimOversizeClientLineDoesNotEndTheSession(t *testing.T) {
	h := newHarness(t, true)

	// Written from its own goroutine: a shim that stops reading part-way
	// through the line would otherwise wedge the test on the pipe rather than
	// fail it.
	go func() {
		_, _ = h.stdin.Write(append(bytes.Repeat([]byte("x"), maxLineBytes+1), '\n'))
		_, _ = h.stdin.Write([]byte(req("1", "tools/list", "") + "\n"))
	}()

	select {
	case err := <-h.runErr:
		t.Fatalf("the shim exited over one oversize line: %v", err)
	case line, ok := <-h.lines:
		require.True(t, ok, "shim stdout closed")
		m, err := jsonrpc.Decode([]byte(line))
		require.NoError(t, err)
		require.Nil(t, m.Error, "the request behind the oversize line must be served normally: %s", line)
		assert.JSONEq(t, `"1"`, string(m.ID))
	case <-time.After(20 * time.Second):
		t.Fatal("the shim stopped reading after the oversize line")
	}
}

// The line reader is hand-rolled, so its edges are pinned here: the cap counts
// the line and not its terminator, an oversize line costs only itself, and a
// stream that ends mid-line still ends the loop.
func TestReadLineSkipsOnlyTheOversizeLine(t *testing.T) {
	const max = 8
	read := func(in string) (lines []string, drops int, err error) {
		r := bufio.NewReaderSize(strings.NewReader(in), 16)
		for {
			line, rerr := readLine(r, max)
			if errors.Is(rerr, errLineTooLong) {
				drops++
				continue
			}
			if len(line) > 0 {
				lines = append(lines, string(line))
			}
			if rerr != nil {
				return lines, drops, rerr
			}
		}
	}

	// The oversize line must be longer than the READER'S BUFFER, not merely
	// longer than the cap: ReadSlice only returns ErrBufferFull when the line
	// outruns the buffer, and that refill loop is the only path an oversize
	// line takes in production, where the cap is 16MiB and the buffer 64KiB.
	// A line that fits the buffer exercises none of the draining.
	lines, drops, err := read("a\n" + strings.Repeat("x", 40) + "\nb\n")
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []string{"a", "b"}, lines, "the lines around an oversize one are untouched")
	assert.Equal(t, 1, drops)

	lines, drops, err = read(strings.Repeat("y", max) + "\ntrailing")
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []string{strings.Repeat("y", max), "trailing"}, lines,
		"a line exactly at the cap is kept, and so is an unterminated last line")
	assert.Zero(t, drops)

	// A stream cut off mid-oversize-line must report the drop and then stop,
	// not spin on a reader that will never produce a newline.
	lines, drops, err = read("a\n" + strings.Repeat("z", max+1))
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []string{"a"}, lines)
	assert.Equal(t, 1, drops)

	// The drop carries how far past the cap the line went: the cap alone is
	// the one number the operator reading that log line already knows.
	_, err = readLine(bufio.NewReaderSize(strings.NewReader(strings.Repeat("w", 40)+"\n"), 16), max)
	require.ErrorIs(t, err, errLineTooLong)
	assert.Contains(t, err.Error(), "40 bytes")
}

// EOF ends the loop without being reported; anything else is a real failure of
// the client's pipe and has to reach Run's caller, or the shim exits 0 on a
// broken stdin and whatever supervises it sees a clean shutdown.
func TestRunReportsAReadFailure(t *testing.T) {
	boom := errors.New("stdin exploded")
	s := New(nil, Config{Server: "test", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	err := s.Run(context.Background(),
		io.MultiReader(strings.NewReader("not json\n"), errReader{boom}), io.Discard)
	assert.ErrorIs(t, err, boom)
}

// errReader fails every read, standing in for a pipe that breaks mid-session.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

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
			mcpspec.MetaProtocolVersion, mcpspec.ProtocolVersion,
		)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No gateway: if the shim published anything, the request would
			// hang and time out rather than answer, which is the failure this
			// test is looking for.
			h := newHarness(t, false)
			h.send(t, fmt.Sprintf(
				`{"jsonrpc":"2.0","id":"1","method":"tools/call","params":%s}`, tt.params,
			))

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
