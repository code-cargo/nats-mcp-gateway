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

package backend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
)

// HTTPBackend speaks Streamable HTTP to an MCP server. Hand-rolled on
// purpose: under 2026-07-28 the client is small (POST + required Mcp-Method/
// Mcp-Name headers; response is application/json or an SSE stream), and for
// 2025-11-25 servers only the Mcp-Session-Id dance is added — the legacy
// bridge (pkg/backend/legacy) runs above this Conn exactly as it does for
// stdio. The optional legacy GET stream is deliberately not opened: it
// carries only unsolicited server messages, which the gateway drops anyway.
type HTTPBackend struct {
	URL string
	// Headers are injected on every request (Authorization etc.) — the
	// credential-injection point for HTTP backends.
	Headers map[string]string
	// Legacy enables Mcp-Session-Id capture/echo/DELETE.
	Legacy bool
	Client *http.Client
	Logger *slog.Logger
}

// Connect validates the config; HTTP needs no persistent socket.
func (b *HTTPBackend) Connect(ctx context.Context) (Conn, error) {
	if b.URL == "" {
		return nil, fmt.Errorf("backend: http url required")
	}
	log := b.Logger
	if log == nil {
		log = slog.Default()
	}
	client := b.Client
	if client == nil {
		client = &http.Client{Timeout: 0} // streams are long-lived; per-request ctx bounds them
	}
	c := &httpConn{
		backend: b,
		client:  client,
		log:     log,
		inbox:   make(chan *jsonrpc.Message, 64),
		done:    make(chan struct{}),
	}
	return c, nil
}

type httpConn struct {
	backend *HTTPBackend
	client  *http.Client
	log     *slog.Logger

	inbox chan *jsonrpc.Message

	mu        sync.Mutex
	sessionID string
	inflight  sync.WaitGroup

	done      chan struct{}
	closeOnce sync.Once
}

func (c *httpConn) Read(ctx context.Context) (*jsonrpc.Message, error) {
	select {
	case m := <-c.inbox:
		return m, nil
	case <-c.done:
		// Drain anything already queued before reporting closed.
		select {
		case m := <-c.inbox:
			return m, nil
		default:
			return nil, ErrConnDead
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *httpConn) Write(ctx context.Context, msg *jsonrpc.Message) error {
	select {
	case <-c.done:
		return ErrConnDead
	default:
	}
	body, err := jsonrpc.Encode(msg)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodPost, c.backend.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range c.backend.Headers {
		req.Header.Set(k, v)
	}
	if msg.Kind() == jsonrpc.KindRequest {
		// Required by 2026-07-28; harmless extras for legacy servers.
		req.Header.Set(mcpspec.HeaderMethod, msg.Method)
		if name := paramsName(msg); name != "" {
			req.Header.Set(mcpspec.HeaderName, name)
		}
		if c.backend.Legacy {
			req.Header.Set(mcpspec.HeaderProtocolVersion, mcpspec.LegacyProtocolVersion)
		} else {
			req.Header.Set(mcpspec.HeaderProtocolVersion, mcpspec.ProtocolVersion)
		}
	}
	c.mu.Lock()
	if c.sessionID != "" {
		req.Header.Set("Mcp-Session-Id", c.sessionID)
	}
	c.mu.Unlock()

	// The response may be a long SSE stream: pump it in the background so
	// Write keeps the Conn contract (non-blocking beyond the POST itself).
	c.inflight.Add(1)
	go func() {
		defer c.inflight.Done()
		c.roundTrip(req, msg)
	}()
	return nil
}

func (c *httpConn) roundTrip(req *http.Request, msg *jsonrpc.Message) {
	resp, err := c.client.Do(req)
	if err != nil {
		c.fail(msg, fmt.Sprintf("http request failed: %v", err))
		return
	}
	defer resp.Body.Close()

	if c.backend.Legacy && msg.Method == mcpspec.MethodInitialize {
		if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
			c.mu.Lock()
			c.sessionID = sid
			c.mu.Unlock()
		}
	}

	switch {
	case resp.StatusCode == http.StatusAccepted:
		return // notification/response accepted, no body
	case resp.StatusCode >= 300:
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		c.fail(msg, fmt.Sprintf("http %d: %s", resp.StatusCode, bytes.TrimSpace(data)))
		return
	}

	ct := resp.Header.Get("Content-Type")
	switch {
	case strings.HasPrefix(ct, "application/json"):
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			c.fail(msg, fmt.Sprintf("reading response: %v", err))
			return
		}
		c.deliverBytes(data)
	case strings.HasPrefix(ct, "text/event-stream"):
		c.pumpSSE(resp.Body)
	default:
		c.fail(msg, fmt.Sprintf("unexpected content-type %q", ct))
	}
}

// pumpSSE delivers each SSE data payload as a message.
func (c *httpConn) pumpSSE(body io.Reader) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 64*1024), maxLineBytes)
	var data bytes.Buffer
	flush := func() {
		if data.Len() == 0 {
			return
		}
		c.deliverBytes(append([]byte(nil), data.Bytes()...))
		data.Reset()
	}
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			flush()
		case strings.HasPrefix(line, "data:"):
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		default:
			// id:/event:/retry:/comments — 2026-07-28 removed resumability,
			// so ids carry nothing we need.
		}
	}
	flush()
}

func (c *httpConn) deliverBytes(data []byte) {
	m, err := jsonrpc.Decode(data)
	if err != nil {
		c.log.Warn("dropping undecodable http message", "err", err)
		return
	}
	c.deliver(m)
}

func (c *httpConn) deliver(m *jsonrpc.Message) {
	select {
	case c.inbox <- m:
	case <-c.done:
	}
}

// fail synthesizes an error response for a request whose HTTP exchange
// failed, so the mux's caller gets an answer instead of a timeout.
func (c *httpConn) fail(msg *jsonrpc.Message, detail string) {
	c.log.Warn("http backend error", "method", msg.Method, "detail", detail)
	if msg.Kind() != jsonrpc.KindRequest {
		return
	}
	c.deliver(jsonrpc.NewErrorResponse(msg.ID, jsonrpc.CodeInternalError, detail, nil))
}

func (c *httpConn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		sid := c.sessionID
		c.mu.Unlock()
		if c.backend.Legacy && sid != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.backend.URL, nil)
			if err == nil {
				req.Header.Set("Mcp-Session-Id", sid)
				for k, v := range c.backend.Headers {
					req.Header.Set(k, v)
				}
				if resp, err := c.client.Do(req); err == nil {
					resp.Body.Close() // 405 is a legal "we don't support DELETE"
				}
			}
		}
		close(c.done)
	})
	return nil
}

// paramsName extracts params.name / params.uri for the Mcp-Name header.
func paramsName(msg *jsonrpc.Message) string {
	var p struct {
		Name string `json:"name"`
		URI  string `json:"uri"`
	}
	_ = json.Unmarshal(msg.Params, &p)
	switch msg.Method {
	case mcpspec.MethodToolsCall, mcpspec.MethodPromptsGet:
		return p.Name
	case mcpspec.MethodResourcesRead:
		return p.URI
	}
	return ""
}
