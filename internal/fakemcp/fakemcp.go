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

// Package fakemcp is a scriptable stdio MCP server for tests and demos. Test
// binaries re-exec themselves into it (TestMain checks EnvFlag), so
// subprocess tests need no separate build step.
//
// Tools: echo, env (returns one env var, for credential-injection
// assertions), slow (N progress notifications then a result), crash (exits
// mid-request), huge (returns > 1 MiB), wedge (never responds), sample
// (initiates a server->client request, which the gateway must reject without
// wedging this process), listen_event (emits a subscription-correlated
// notification, for mux routing tests).
//
// Env knobs: FAKEMCP_PROTOCOL=2025-11-25 makes it a legacy server that
// requires the initialize handshake before serving. FAKEMCP_BANNER=1 prints
// a non-JSON line to stdout at startup (misbehaving-server simulation).
package fakemcp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// EnvFlag re-execs the test binary into the fake server when set to "1".
const EnvFlag = "NATSMCP_FAKEMCP"

type server struct {
	mu          sync.Mutex // stdout writes
	out         *bufio.Writer
	legacy      bool
	initialized bool
	seq         int

	pendMu  sync.Mutex
	pending map[string]chan *jsonrpc.Message // server-initiated request id -> response
}

// Main runs the fake server on stdin/stdout; it returns when stdin closes.
func Main() {
	s := &server{
		out:     bufio.NewWriter(os.Stdout),
		legacy:  os.Getenv("FAKEMCP_PROTOCOL") == mcpspec.LegacyProtocolVersion,
		pending: make(map[string]chan *jsonrpc.Message),
	}
	if os.Getenv("FAKEMCP_BANNER") == "1" {
		fmt.Println("fakemcp starting up (this banner is not JSON-RPC)")
	}
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var wg sync.WaitGroup
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		msg, err := jsonrpc.Decode([]byte(line))
		if err != nil {
			continue
		}
		// Notifications and responses are handled INLINE so stdin ordering
		// is preserved: notifications/initialized must take effect before
		// any later request line is dispatched, or a racing tools/call sees
		// "server not initialized" (a real CI flake). Both are cheap and
		// non-blocking (pending channels are buffered). Only requests go to
		// goroutines — wedge/slow need a live read loop while they block.
		if msg.Kind() != jsonrpc.KindRequest {
			s.handle(msg)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handle(msg)
		}()
	}
	wg.Wait()
}

func (s *server) send(m *jsonrpc.Message) {
	data, err := jsonrpc.Encode(m)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.out.Write(data)
	s.out.WriteByte('\n')
	s.out.Flush()
}

func (s *server) result(id json.RawMessage, v any) {
	raw, _ := json.Marshal(v)
	s.send(jsonrpc.NewResponse(id, raw))
}

func (s *server) rpcError(id json.RawMessage, code int, msg string) {
	s.send(jsonrpc.NewErrorResponse(id, code, msg, nil))
}

func (s *server) handle(msg *jsonrpc.Message) {
	if msg.Kind() == jsonrpc.KindNotification {
		// initialized, cancelled, etc.: accepted silently.
		if s.legacy && msg.Method == mcpspec.NotifInitialized {
			s.mu.Lock()
			s.initialized = true
			s.mu.Unlock()
		}
		return
	}
	if msg.Kind() == jsonrpc.KindResponse {
		// A response to one of OUR server-initiated requests.
		s.pendMu.Lock()
		ch := s.pending[msg.IDKey()]
		delete(s.pending, msg.IDKey())
		s.pendMu.Unlock()
		if ch != nil {
			ch <- msg
		}
		return
	}
	if msg.Kind() != jsonrpc.KindRequest {
		return
	}

	if s.legacy {
		if msg.Method == mcpspec.MethodInitialize {
			s.result(msg.ID, map[string]any{
				"protocolVersion": mcpspec.LegacyProtocolVersion,
				"capabilities": map[string]any{
					"tools":   map[string]any{"listChanged": true},
					"prompts": map[string]any{},
					"logging": map[string]any{},
				},
				"serverInfo":   map[string]any{"name": "fakemcp", "version": "1.0.0"},
				"instructions": "fake server for tests",
			})
			return
		}
		if msg.Method == mcpspec.MethodPing {
			s.result(msg.ID, map[string]any{})
			return
		}
		s.mu.Lock()
		ok := s.initialized
		s.mu.Unlock()
		if !ok {
			s.rpcError(msg.ID, jsonrpc.CodeInvalidRequest, "server not initialized")
			return
		}
	}

	switch msg.Method {
	case mcpspec.MethodDiscover:
		if s.legacy {
			s.rpcError(msg.ID, jsonrpc.CodeMethodNotFound, "unknown method server/discover")
			return
		}
		s.result(msg.ID, map[string]any{
			"resultType":        mcpspec.ResultTypeComplete,
			"supportedVersions": []string{mcpspec.ProtocolVersion},
			"capabilities":      map[string]any{"tools": map[string]any{"listChanged": true}},
			"serverInfo":        map[string]any{"name": "fakemcp", "version": "1.0.0"},
		})
	case "tools/list":
		s.result(msg.ID, map[string]any{
			"resultType": mcpspec.ResultTypeComplete,
			"tools": []map[string]any{
				{"name": "echo", "description": "echoes arguments", "inputSchema": map[string]any{"type": "object"}},
				{"name": "slow", "description": "emits progress then a result", "inputSchema": map[string]any{"type": "object"}},
			},
		})
	case mcpspec.MethodToolsCall:
		s.handleToolCall(msg)
	default:
		s.rpcError(msg.ID, jsonrpc.CodeMethodNotFound, "unknown method "+msg.Method)
	}
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	Meta      struct {
		ProgressToken json.RawMessage `json:"progressToken"`
	} `json:"_meta"`
}

func (s *server) handleToolCall(msg *jsonrpc.Message) {
	var p toolCallParams
	_ = json.Unmarshal(msg.Params, &p)

	switch p.Name {
	case "echo":
		// pid lets tests assert process identity (tenant isolation).
		s.result(msg.ID, map[string]any{
			"resultType": mcpspec.ResultTypeComplete,
			"pid":        os.Getpid(),
			"content":    []map[string]any{{"type": "text", "text": string(p.Arguments)}},
		})
	case "env":
		// Returns one env var's value (arguments: {"name": "VAR"}), so tests
		// can assert credential injection per process.
		var args struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(p.Arguments, &args)
		s.result(msg.ID, map[string]any{
			"resultType": mcpspec.ResultTypeComplete,
			"pid":        os.Getpid(),
			"value":      os.Getenv(args.Name),
		})
	case "slow":
		// Three progress notifications, then the result. Progress requires
		// the caller to have supplied a progressToken.
		if p.Meta.ProgressToken != nil {
			for i := 1; i <= 3; i++ {
				params, _ := json.Marshal(map[string]any{
					"progressToken": json.RawMessage(p.Meta.ProgressToken),
					"progress":      i,
					"total":         3,
				})
				s.send(jsonrpc.NewNotification(mcpspec.NotifProgress, params))
			}
		}
		s.result(msg.ID, map[string]any{
			"resultType": mcpspec.ResultTypeComplete,
			"content":    []map[string]any{{"type": "text", "text": "done"}},
		})
	case "crash":
		os.Exit(3)
	case "huge":
		s.result(msg.ID, map[string]any{
			"resultType": mcpspec.ResultTypeComplete,
			"content": []map[string]any{
				{"type": "text", "text": strings.Repeat("x", 2*1024*1024)},
			},
		})
	case "wedge":
		select {} // never responds; tests the cancellation/timeout path
	case "listen_event":
		// Emits a notification correlated to THIS request via _meta
		// subscriptionId (the 2026-07-28 subscriptions/listen shape), then
		// the result: exercises the mux's subscription routing.
		params, _ := json.Marshal(map[string]any{
			"uri":   "file:///watched",
			"_meta": map[string]any{mcpspec.MetaSubscriptionID: json.RawMessage(msg.ID)},
		})
		s.send(jsonrpc.NewNotification("notifications/resources/updated", params))
		s.result(msg.ID, map[string]any{
			"resultType": mcpspec.ResultTypeComplete,
			"content":    []map[string]any{{"type": "text", "text": "listening"}},
		})
	case "notify_changed":
		// Emits a legacy tools/list_changed notification, then succeeds:
		// exercises the subscriptions/listen synthesis in the legacy bridge.
		s.send(jsonrpc.NewNotification("notifications/tools/list_changed", nil))
		s.result(msg.ID, map[string]any{
			"resultType": mcpspec.ResultTypeComplete,
			"content":    []map[string]any{{"type": "text", "text": "notified"}},
		})
	case "sample":
		// Initiate a server->client request — forbidden on the modern wire —
		// and BLOCK on its answer. The gateway must respond with an error
		// (not silence), which unblocks us; the tool result then reports
		// what happened, proving the process did not wedge.
		s.pendMu.Lock()
		s.seq++
		sampleID, _ := json.Marshal(fmt.Sprintf("srv-%d", s.seq))
		ch := make(chan *jsonrpc.Message, 1)
		s.pending[string(sampleID)] = ch
		s.pendMu.Unlock()
		s.send(&jsonrpc.Message{
			JSONRPC: "2.0",
			ID:      sampleID,
			Method:  "sampling/createMessage",
			Params:  json.RawMessage(`{"messages":[]}`),
		})
		outcome := "no-answer"
		resp := <-ch
		if resp.Error != nil {
			outcome = fmt.Sprintf("rejected:%d", resp.Error.Code)
		} else if resp.Result != nil {
			outcome = "answered"
		}
		s.result(msg.ID, map[string]any{
			"resultType": mcpspec.ResultTypeComplete,
			"content":    []map[string]any{{"type": "text", "text": "sample-" + outcome}},
		})
	default:
		s.rpcError(msg.ID, jsonrpc.CodeInvalidParams, "unknown tool "+p.Name)
	}
}
