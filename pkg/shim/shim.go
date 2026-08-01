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

// Package shim is the client edge: it bridges an MCP client's stdio to the
// NATS wire. It is a byte pump with two jobs — envelope facts (method, name,
// protocol version) for the wire headers, and per-request stream demux. It
// builds no MCP client and holds no MCP schema beyond pkg/mcpspec constants.
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
	"sync"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

const maxLineBytes = 16 * 1024 * 1024

// errLineTooLong reports a client line past maxLineBytes. The line has been
// consumed either way, so the loop can read on: drained to its newline when
// there was one, or to the end of a stream that stopped mid-line — in which
// case the next read returns the sticky io.EOF and ends the loop.
var errLineTooLong = errors.New("shim: client line exceeds the line cap")

// Config configures a shim.
type Config struct {
	// Server is the MCP server name this shim fronts.
	Server string
	// Logger must write to stderr only: stdout is the protocol pipe.
	Logger *slog.Logger
}

// Shim pumps one MCP client's stdio through the wire.
type Shim struct {
	wc  *wire.Client
	cfg Config
	log *slog.Logger

	outMu sync.Mutex
	out   io.Writer

	streamMu sync.Mutex
	streams  map[string]*wire.Stream // request id key -> in-flight stream

	// legacy is non-nil once a 2025-11-25 client is detected (its first
	// request was initialize). Only touched from the Run loop goroutine.
	legacy *legacyClient

	wg sync.WaitGroup
}

// New builds a shim over an established wire client.
func New(wc *wire.Client, cfg Config) *Shim {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Shim{
		wc:      wc,
		cfg:     cfg,
		log:     log,
		streams: make(map[string]*wire.Stream),
	}
}

// Run pumps until stdin closes or ctx is cancelled.
func (s *Shim) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	s.out = stdout
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	br := bufio.NewReaderSize(stdin, 64*1024)
	var readErr error
	for {
		raw, err := readLine(br, maxLineBytes)
		if errors.Is(err, errLineTooLong) {
			// Dropped like any other line we cannot make sense of. There is no
			// id to answer with — the body it was in never parsed — so the one
			// request pays for it, which is the whole point: under Scanner the
			// session's other requests paid too.
			s.log.Warn("dropping oversize line from client", "err", err, "cap", maxLineBytes)
			continue
		}
		line := bytes.TrimSpace(raw)
		if len(line) > 0 {
			msg, derr := jsonrpc.Decode(line)
			switch {
			case derr != nil:
				s.log.Warn("dropping invalid JSON-RPC line from client", "err", derr)
			case msg.Kind() == jsonrpc.KindRequest:
				body := make([]byte, len(line))
				copy(body, line)
				s.handleRequest(ctx, msg, body)
			case msg.Kind() == jsonrpc.KindNotification:
				s.handleNotification(msg)
			default:
				// Modern servers never initiate requests, so a client response
				// has nothing to answer.
				s.log.Debug("dropping unexpected client message", "kind", "response")
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}
	cancel()    // abandon in-flight streams
	s.wg.Wait() // let pumps finish writing
	return readErr
}

// readLine returns the next line without its terminator. A line past max is
// drained to its newline and reported as errLineTooLong: bufio.Scanner would
// instead end the loop for good, and one unreadable line must not take the
// session's other requests with it.
func readLine(r *bufio.Reader, max int) ([]byte, error) {
	var line []byte
	dropped, n := false, 0
	for {
		chunk, err := r.ReadSlice('\n')
		if err == nil {
			// ReadSlice reports nil only when it found the delimiter, and the
			// cap is on the line rather than on its terminator.
			chunk = chunk[:len(chunk)-1]
		}
		n += len(chunk)
		switch {
		case dropped:
			// Past the cap already: read on only to find the newline.
		case n > max:
			dropped, line = true, nil
		default:
			// ReadSlice hands back the reader's own buffer, so this has to
			// copy — the next read overwrites it.
			line = append(line, chunk...)
		}
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case dropped:
			// A stream that ends mid-line reports the drop first; the next
			// call re-reads the sticky io.EOF and ends the loop. The length is
			// carried along because it is the number whoever is reading the
			// log wants: the cap on its own never says how far past it we went.
			return nil, fmt.Errorf("%w (%d bytes)", errLineTooLong, n)
		default:
			return line, err
		}
	}
}

func (s *Shim) handleRequest(ctx context.Context, msg *jsonrpc.Message, body []byte) {
	// Legacy wing: an initialize flips the shim into legacy mode; thereafter
	// ping and logging/setLevel are answered locally. A REPEAT initialize is
	// answered the same way rather than forwarded — there is no initialize on
	// the 2026-07-28 wire, so forwarding one sends it to a subject no grant is
	// written for, and where a broad grant does cover it the gateway's legacy
	// bridge hands a second handshake to a subprocess that is already
	// initialized. The client is also re-stating its identity, so the answer
	// comes from the params it just sent.
	if msg.Method == mcpspec.MethodInitialize {
		s.engageLegacy(ctx, msg)
		return
	}
	if s.legacy != nil && s.interceptLegacyRequest(msg) {
		return
	}

	// Exact-key, duplicate-rejecting: the subject and headers built below are
	// what the gateway will check this body against, so reading params any
	// differently here only manufactures a -32020 one hop later. Params the
	// shim cannot read unambiguously it must not guess at.
	//
	// Answered with -32600, not the -32020 the proxy answers the same
	// AmbiguousKeyError with, and the difference is not an inconsistency:
	// -32020 means "transport headers disagree with the body", and at this
	// point there are no headers to disagree with — the shim is failing to
	// read the params it would have built them FROM. To the client this is
	// simply a request the shim cannot interpret. The two codes also never
	// reach one observer: a body rejected here never travels, so no caller
	// sees both answers to the same request.
	p, err := mcpspec.DecodeParams(msg.Params)
	if err != nil {
		s.writeError(msg.ID, jsonrpc.CodeInvalidRequest, err.Error())
		return
	}

	// Ensure the required protocolVersion _meta is present: native clients
	// send it; legacy mode injects clientInfo/clientCapabilities too.
	ver, err := p.ProtocolVersion()
	if err != nil {
		s.writeError(msg.ID, jsonrpc.CodeInvalidRequest, err.Error())
		return
	}
	inject := map[string]json.RawMessage{}
	if s.legacy != nil {
		inject = s.legacy.legacyMeta()
		ver = mcpspec.ProtocolVersion
	} else if ver == "" {
		ver = mcpspec.ProtocolVersion
		verRaw, _ := json.Marshal(ver)
		inject[mcpspec.MetaProtocolVersion] = verRaw
	}
	if len(inject) > 0 {
		if newBody, ok := injectMeta(msg, inject); ok {
			body = newBody
		}
	}

	name, _, err := p.Name(msg.Method)
	if err != nil {
		s.writeError(msg.ID, jsonrpc.CodeInvalidRequest, err.Error())
		return
	}

	req := &wire.Request{
		Server:          s.cfg.Server,
		Method:          msg.Method,
		Name:            name,
		ProtocolVersion: ver,
		Body:            body,
	}
	stream, err := s.wc.Do(ctx, req)
	if err != nil {
		s.writeLocalError(msg.ID, err)
		return
	}

	idKey := msg.IDKey()
	s.streamMu.Lock()
	s.streams[idKey] = stream
	s.streamMu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			s.streamMu.Lock()
			// Only if this stream is still the one registered. A client that
			// reuses an id while the first request is in flight replaces the
			// entry, and an unconditional delete would then retire the LIVE
			// request's route on the dead one's way out — leaving a request the
			// client can still see running but can no longer cancel.
			if s.streams[idKey] == stream {
				delete(s.streams, idKey)
			}
			s.streamMu.Unlock()
		}()
		for f := range stream.C {
			switch {
			case f.Err != nil:
				// Locally-detected failure: we own the id, synthesize the
				// JSON-RPC error the client can actually read.
				s.writeError(msg.ID, f.Err.Code, f.Err.Message)
			case f.Kind == wire.FrameEnd && len(f.Body) == 0:
				// Cancelled: a cancelled JSON-RPC request gets no response.
			default:
				s.writeLine(f.Body)
			}
		}
	}()
}

func (s *Shim) handleNotification(msg *jsonrpc.Message) {
	if s.legacy != nil && msg.Method == mcpspec.NotifInitialized {
		return // swallowed: the wire has no handshake
	}
	if msg.Method != mcpspec.NotifCancelled {
		// Modern's only client notification is notifications/cancelled;
		// legacy strays (roots/list_changed) are dropped with a log.
		s.log.Debug("dropping client notification", "method", msg.Method)
		return
	}
	var p struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	_ = json.Unmarshal(msg.Params, &p)
	key := string(bytes.TrimSpace(p.RequestID))
	s.streamMu.Lock()
	stream := s.streams[key]
	s.streamMu.Unlock()
	if stream == nil {
		s.log.Debug("cancel for unknown request", "requestId", key)
		return
	}
	raw, err := jsonrpc.Encode(msg)
	if err != nil {
		return
	}
	if err := stream.Cancel(raw); err != nil {
		s.log.Warn("cancel publish failed", "err", err)
	}
}

func (s *Shim) writeLine(body []byte) {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	_, _ = s.out.Write(body)
	_, _ = s.out.Write([]byte{'\n'})
	if f, ok := s.out.(interface{ Flush() error }); ok {
		_ = f.Flush()
	}
}

func (s *Shim) writeError(id json.RawMessage, code int, message string) {
	body, err := jsonrpc.Encode(jsonrpc.NewErrorResponse(id, code, message, nil))
	if err != nil {
		return
	}
	s.writeLine(body)
}

// writeLocalError maps wire-level Do failures onto JSON-RPC errors.
func (s *Shim) writeLocalError(id json.RawMessage, err error) {
	var werr *wire.Error
	if errors.As(err, &werr) {
		s.writeError(id, werr.Code, werr.Message)
		return
	}
	s.writeError(id, jsonrpc.CodeInternalError, err.Error())
}

// injectMeta merges entries into a request body's params._meta, returning
// the re-encoded bytes.
func injectMeta(msg *jsonrpc.Message, entries map[string]json.RawMessage) ([]byte, bool) {
	var params map[string]json.RawMessage
	if len(msg.Params) > 0 {
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, false
		}
	}
	if params == nil {
		params = map[string]json.RawMessage{}
	}
	var meta map[string]json.RawMessage
	if raw, ok := params["_meta"]; ok {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil, false
		}
	}
	if meta == nil {
		meta = map[string]json.RawMessage{}
	}
	for k, v := range entries {
		meta[k] = v
	}
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		return nil, false
	}
	params["_meta"] = metaRaw
	paramsRaw, err := json.Marshal(params)
	if err != nil {
		return nil, false
	}
	out := &jsonrpc.Message{JSONRPC: "2.0", ID: msg.ID, Method: msg.Method, Params: paramsRaw}
	body, err := jsonrpc.Encode(out)
	if err != nil {
		return nil, false
	}
	msg.Params = paramsRaw
	return body, true
}
