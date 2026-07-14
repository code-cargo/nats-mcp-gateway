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

// Package legacy presents a Modern (2026-07-28) face over a Legacy
// (2025-11-25) MCP server — the only schema-aware code on the gateway side.
//
// On connect it performs the legacy initialize handshake exactly once,
// deliberately advertising NO sampling/elicitation/roots client capabilities:
// a conformant legacy server then may not initiate server->client requests,
// which is what keeps the gateway stateless. From the cached
// InitializeResult it synthesizes server/discover without touching the
// subprocess, and it fans legacy */list_changed notifications into open
// subscriptions/listen streams.
package legacy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// handshakeTimeout bounds the one-time initialize exchange on connect.
const handshakeTimeout = 30 * time.Second

// Backend wraps a legacy backend, handshaking on every new connection.
type Backend struct {
	// Inner produces raw connections (stdio subprocess, HTTP).
	Inner backend.Backend
	// DiscoverTTLMs is served in the synthesized DiscoverResult (default 300000).
	DiscoverTTLMs int
	Logger        *slog.Logger
}

// Connect spawns the inner connection and performs the 2025-11-25 handshake.
func (b *Backend) Connect(ctx context.Context) (backend.Conn, error) {
	log := b.Logger
	if log == nil {
		log = slog.Default()
	}
	inner, err := b.Inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	init, err := handshake(ctx, inner)
	if err != nil {
		_ = inner.Close()
		return nil, fmt.Errorf("legacy: handshake: %w", err)
	}
	ttl := b.DiscoverTTLMs
	if ttl <= 0 {
		ttl = 300000
	}
	return &conn{
		inner:     inner,
		init:      init,
		ttlMs:     ttl,
		log:       log,
		synth:     make(chan *jsonrpc.Message, 16),
		listeners: make(map[string]listenFilter),
	}, nil
}

// initResult is the slice of InitializeResult the bridge needs, kept raw so
// nothing is lost in translation.
type initResult struct {
	Capabilities json.RawMessage `json:"capabilities"`
	ServerInfo   json.RawMessage `json:"serverInfo"`
	Instructions json.RawMessage `json:"instructions"`
}

// handshake performs initialize + notifications/initialized. The advertised
// clientCapabilities are EMPTY on purpose — see the package comment.
func handshake(ctx context.Context, c backend.Conn) (*initResult, error) {
	ctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	params, _ := json.Marshal(map[string]any{
		"protocolVersion": mcpspec.LegacyProtocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "natsmcp-gateway", "version": "1.0"},
	})
	req := jsonrpc.NewRequest("natsmcp-init", mcpspec.MethodInitialize, params)
	if err := c.Write(ctx, req); err != nil {
		return nil, err
	}

	for {
		msg, err := c.Read(ctx)
		if err != nil {
			return nil, err
		}
		if msg.Kind() != jsonrpc.KindResponse || msg.IDKey() != req.IDKey() {
			continue // stray notifications before the response are legal
		}
		if msg.Error != nil {
			return nil, fmt.Errorf("initialize rejected: %s", msg.Error)
		}
		var init initResult
		if err := json.Unmarshal(msg.Result, &init); err != nil {
			return nil, fmt.Errorf("bad InitializeResult: %w", err)
		}
		if err := c.Write(ctx, jsonrpc.NewNotification(mcpspec.NotifInitialized, nil)); err != nil {
			return nil, err
		}
		return &init, nil
	}
}

// listenFilter is the slice of SubscriptionFilter v1 honors.
type listenFilter struct {
	Tools     bool `json:"toolsListChanged"`
	Prompts   bool `json:"promptsListChanged"`
	Resources bool `json:"resourcesListChanged"`
}

// conn is the modern-facing connection over a handshaken legacy one.
type conn struct {
	inner backend.Conn
	init  *initResult
	ttlMs int
	log   *slog.Logger

	// synth carries locally-synthesized messages (discover responses,
	// listen notifications) into Read.
	synth chan *jsonrpc.Message

	mu        sync.Mutex
	listeners map[string]listenFilter // listen request id key -> filter
	readCh    chan readResult         // in-flight inner read, if any
}

type readResult struct {
	msg *jsonrpc.Message
	err error
}

func (c *conn) Close() error { return c.inner.Close() }

func (c *conn) Write(ctx context.Context, msg *jsonrpc.Message) error {
	switch {
	case msg.Kind() == jsonrpc.KindRequest && msg.Method == mcpspec.MethodDiscover:
		// Synthesized from the cached InitializeResult; the subprocess never
		// sees it. supportedVersions is the GATEWAY's modern version — from
		// the client's viewpoint the gateway is the modern server.
		result := map[string]any{
			"resultType":        mcpspec.ResultTypeComplete,
			"supportedVersions": []string{mcpspec.ProtocolVersion},
			"capabilities":      orEmpty(c.init.Capabilities),
			"serverInfo":        orEmpty(c.init.ServerInfo),
			"ttlMs":             c.ttlMs,
		}
		if len(c.init.Instructions) > 0 {
			result["instructions"] = c.init.Instructions
		}
		raw, err := json.Marshal(result)
		if err != nil {
			return err
		}
		c.push(ctx, jsonrpc.NewResponse(msg.ID, raw))
		return nil

	case msg.Kind() == jsonrpc.KindRequest && msg.Method == mcpspec.MethodListen:
		// Register the stream; no response is synthesized — a listen stream
		// stays open until cancelled, exactly like a modern backend's would.
		var f listenFilter
		_ = json.Unmarshal(msg.Params, &f)
		c.mu.Lock()
		c.listeners[msg.IDKey()] = f
		c.mu.Unlock()
		return nil

	case msg.Kind() == jsonrpc.KindNotification && msg.Method == mcpspec.NotifCancelled:
		// A cancel for a registered listen stream is ours; everything else
		// forwards (in-flight tool calls).
		var p struct {
			RequestID json.RawMessage `json:"requestId"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		key := string(p.RequestID)
		c.mu.Lock()
		_, isListen := c.listeners[key]
		delete(c.listeners, key)
		c.mu.Unlock()
		if isListen {
			return nil
		}
		return c.inner.Write(ctx, msg)

	default:
		if msg.Kind() == jsonrpc.KindRequest {
			msg = stripModernMeta(msg)
		}
		return c.inner.Write(ctx, msg)
	}
}

func (c *conn) Read(ctx context.Context) (*jsonrpc.Message, error) {
	for {
		select {
		case m := <-c.synth:
			return m, nil
		default:
		}
		// Poll-read the inner connection but stay responsive to synth: a
		// discover answered locally must not wait behind a quiet subprocess.
		msg, err := c.innerReadOrSynth(ctx)
		if err != nil {
			return nil, err
		}
		if msg == nil {
			continue
		}
		if msg.Kind() == jsonrpc.KindNotification {
			if out := c.translateNotification(msg); out != nil {
				return out, nil
			}
			// list_changed fan-out already queued on synth (or dropped).
			continue
		}
		return msg, nil
	}
}

// innerReadOrSynth returns the next inner message, or nil if a synth message
// became available first (the caller loops and picks it up).
func (c *conn) innerReadOrSynth(ctx context.Context) (*jsonrpc.Message, error) {
	// The inner read must survive this call: successive Read calls reuse it.
	c.mu.Lock()
	if c.readCh == nil {
		ch := make(chan readResult, 1)
		c.readCh = ch
		go func() {
			m, err := c.inner.Read(context.Background())
			ch <- readResult{m, err}
		}()
	}
	ch := c.readCh
	c.mu.Unlock()

	select {
	case r := <-ch:
		c.mu.Lock()
		c.readCh = nil
		c.mu.Unlock()
		return r.msg, r.err
	case m := <-c.synth:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// translateNotification handles legacy server notifications. It returns the
// message to surface directly, or nil if handled (fanned out or dropped).
func (c *conn) translateNotification(msg *jsonrpc.Message) *jsonrpc.Message {
	var want func(listenFilter) bool
	switch msg.Method {
	case "notifications/tools/list_changed":
		want = func(f listenFilter) bool { return f.Tools }
	case "notifications/prompts/list_changed":
		want = func(f listenFilter) bool { return f.Prompts }
	case "notifications/resources/list_changed":
		want = func(f listenFilter) bool { return f.Resources }
	default:
		// progress etc. pass through untouched; the mux attributes them.
		return msg
	}

	c.mu.Lock()
	ids := make([]json.RawMessage, 0, len(c.listeners))
	for key, f := range c.listeners {
		if want(f) {
			ids = append(ids, json.RawMessage(key))
		}
	}
	c.mu.Unlock()

	for _, id := range ids {
		params := map[string]json.RawMessage{}
		if len(msg.Params) > 0 {
			_ = json.Unmarshal(msg.Params, &params)
		}
		meta := map[string]json.RawMessage{}
		if raw, ok := params["_meta"]; ok {
			_ = json.Unmarshal(raw, &meta)
		}
		meta[mcpspec.MetaSubscriptionID] = id
		metaRaw, _ := json.Marshal(meta)
		params["_meta"] = metaRaw
		paramsRaw, _ := json.Marshal(params)
		select {
		case c.synth <- jsonrpc.NewNotification(msg.Method, paramsRaw):
		default:
			c.log.Warn("listen fan-out queue full, dropping notification", "method", msg.Method)
		}
	}
	return nil
}

// push queues a synthesized message for Read.
func (c *conn) push(ctx context.Context, m *jsonrpc.Message) {
	select {
	case c.synth <- m:
	case <-ctx.Done():
	}
}

// stripModernMeta removes io.modelcontextprotocol/* keys from params._meta
// before a request reaches a legacy server that never negotiated them.
func stripModernMeta(msg *jsonrpc.Message) *jsonrpc.Message {
	var params map[string]json.RawMessage
	if len(msg.Params) == 0 || json.Unmarshal(msg.Params, &params) != nil {
		return msg
	}
	rawMeta, ok := params["_meta"]
	if !ok {
		return msg
	}
	var meta map[string]json.RawMessage
	if json.Unmarshal(rawMeta, &meta) != nil {
		return msg
	}
	changed := false
	for k := range meta {
		if strings.HasPrefix(k, "io.modelcontextprotocol/") {
			delete(meta, k)
			changed = true
		}
	}
	if !changed {
		return msg
	}
	if len(meta) == 0 {
		delete(params, "_meta")
	} else {
		metaRaw, _ := json.Marshal(meta)
		params["_meta"] = metaRaw
	}
	paramsRaw, _ := json.Marshal(params)
	return &jsonrpc.Message{JSONRPC: "2.0", ID: msg.ID, Method: msg.Method, Params: paramsRaw}
}

func orEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}
