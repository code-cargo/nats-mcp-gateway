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
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// legacyClient is the mirror-image compat wing at the client edge: engaged
// automatically when the client's first request is `initialize` (every
// 2025-11-25 client, Claude Code included). It answers the legacy handshake
// locally from server/discover, absorbs the legacy methods the modern wire
// has no home for (ping, logging/setLevel), and injects the three required
// _meta keys on everything that goes over NATS. This is what upholds
// "2026-07-28 only on the wire".
type legacyClient struct {
	clientInfo json.RawMessage // from initialize params, echoed into _meta
	clientCaps json.RawMessage
}

const discoverTimeout = 30 * time.Second

// engageLegacy handles an initialize request: it engages legacy mode on the
// first one and refreshes the recorded client identity on any repeat, then
// answers from server/discover. Called only from the Run loop goroutine, which
// is what makes the s.legacy assignment safe.
func (s *Shim) engageLegacy(ctx context.Context, msg *jsonrpc.Message) {
	var p struct {
		ClientInfo   json.RawMessage `json:"clientInfo"`
		Capabilities json.RawMessage `json:"capabilities"`
	}
	_ = json.Unmarshal(msg.Params, &p)
	if s.legacy == nil {
		s.log.Info("legacy client detected, bridging initialize to server/discover")
	} else {
		s.log.Info("legacy client re-initialized, answering from server/discover again")
	}
	s.legacy = &legacyClient{clientInfo: p.ClientInfo, clientCaps: p.Capabilities}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.answerInitialize(ctx, msg.ID)
	}()
}

// answerInitialize calls server/discover over the wire and translates its
// DiscoverResult into the InitializeResult the legacy client expects.
func (s *Shim) answerInitialize(ctx context.Context, initID json.RawMessage) {
	ctx, cancel := context.WithTimeout(ctx, discoverTimeout)
	defer cancel()

	params, _ := json.Marshal(map[string]any{
		"_meta": map[string]string{mcpspec.MetaProtocolVersion: mcpspec.ProtocolVersion},
	})
	body, _ := jsonrpc.Encode(jsonrpc.NewRequest("shim-discover", mcpspec.MethodDiscover, params))
	stream, err := s.wc.Do(ctx, &wire.Request{
		Server:          s.cfg.Server,
		Method:          mcpspec.MethodDiscover,
		ProtocolVersion: mcpspec.ProtocolVersion,
		Body:            body,
	})
	if err != nil {
		s.writeLocalError(initID, err)
		return
	}

	var terminal wire.Frame
	for f := range stream.C {
		terminal = f
	}
	switch {
	case terminal.Err != nil:
		s.writeError(initID, terminal.Err.Code, terminal.Err.Message)
		return
	case terminal.Kind != wire.FrameEnd || len(terminal.Body) == 0:
		s.writeError(initID, jsonrpc.CodeInternalError, "server/discover yielded no response")
		return
	}
	resp, err := jsonrpc.Decode(terminal.Body)
	if err != nil || resp.Error != nil {
		if resp != nil && resp.Error != nil {
			s.writeError(initID, resp.Error.Code, resp.Error.Message)
		} else {
			s.writeError(initID, jsonrpc.CodeInternalError, "bad server/discover response")
		}
		return
	}

	var d struct {
		Capabilities json.RawMessage            `json:"capabilities"`
		ServerInfo   json.RawMessage            `json:"serverInfo"`
		Instructions json.RawMessage            `json:"instructions"`
		Meta         map[string]json.RawMessage `json:"_meta"`
	}
	if err := json.Unmarshal(resp.Result, &d); err != nil {
		s.writeError(initID, jsonrpc.CodeInternalError, "bad DiscoverResult")
		return
	}
	// DiscoverResult -> InitializeResult is nearly field-for-field; the
	// protocolVersion is the LEGACY one because that is the face this edge
	// presents.
	init := map[string]any{
		"protocolVersion": mcpspec.LegacyProtocolVersion,
		"capabilities":    orEmptyObject(d.Capabilities),
		"serverInfo":      orEmptyObject(discoverServerInfo(d.Meta, d.ServerInfo)),
	}
	if len(d.Instructions) > 0 {
		init["instructions"] = d.Instructions
	}
	raw, _ := json.Marshal(init)
	respBody, err := jsonrpc.Encode(jsonrpc.NewResponse(initID, raw))
	if err != nil {
		return
	}
	s.writeLine(respBody)
}

// interceptLegacyRequest answers requests the modern wire cannot carry.
// Returns true if the request was fully handled locally.
func (s *Shim) interceptLegacyRequest(msg *jsonrpc.Message) bool {
	switch msg.Method {
	case mcpspec.MethodPing, mcpspec.MethodLoggingSetLevel:
		// ping: removed in 2026-07-28, but legacy clients health-check with
		// it constantly — forwarding would turn every check into
		// method-not-found. setLevel: no modern equivalent.
		body, err := jsonrpc.Encode(jsonrpc.NewResponse(msg.ID, nil))
		if err == nil {
			s.writeLine(body)
		}
		return true
	default:
		return false
	}
}

// legacyMeta returns the _meta entries to inject on every outbound request.
//
// protocolVersion and clientCapabilities are REQUIRED, so both are always
// present — an empty object is a valid capability set, an absent one is not.
// clientInfo is only SHOULD (it was required until 2026-07-16), so it is sent
// when the legacy client identified itself and omitted when it did not, rather
// than fabricated.
func (l *legacyClient) legacyMeta() map[string]json.RawMessage {
	verRaw, _ := json.Marshal(mcpspec.ProtocolVersion)
	caps := l.clientCaps
	if isEmptyJSON(caps) {
		caps = json.RawMessage(`{}`)
	}
	m := map[string]json.RawMessage{
		mcpspec.MetaProtocolVersion:    verRaw,
		mcpspec.MetaClientCapabilities: caps,
	}
	if !isEmptyJSON(l.clientInfo) {
		m[mcpspec.MetaClientInfo] = l.clientInfo
	}
	return m
}

// discoverServerInfo picks the server's identity out of a DiscoverResult.
//
// serverInfo moved out of the result's top level into result _meta on
// 2026-07-16, late in the 2026-07-28 draft. The _meta key is preferred; the
// top-level field is still read so a backend built against the earlier draft
// identifies itself instead of surfacing to the client as an empty object.
// The fallback can go once nothing pre-final is in service.
func discoverServerInfo(meta map[string]json.RawMessage, topLevel json.RawMessage) json.RawMessage {
	if si, ok := meta[mcpspec.MetaServerInfo]; ok && !isEmptyJSON(si) {
		return si
	}
	return topLevel
}

func orEmptyObject(raw json.RawMessage) json.RawMessage {
	if isEmptyJSON(raw) {
		return json.RawMessage(`{}`)
	}
	return raw
}

// isEmptyJSON reports whether raw carries no usable value. A literal `null`
// counts: a legacy client may send "capabilities": null, and forwarding that
// as clientCapabilities would satisfy a length check while giving a strict
// backend neither an object nor an absent field.
func isEmptyJSON(raw json.RawMessage) bool {
	return len(raw) == 0 || string(bytes.TrimSpace(raw)) == "null"
}
