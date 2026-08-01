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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

func runCall(c *CallCmd, g *Globals) error {
	opts := []nats.Option{nats.Name("natsmcp-call")}
	if c.Creds != "" {
		opts = append(opts, nats.UserCredentials(c.Creds))
	}
	nc, err := nats.Connect(c.NatsURL, opts...)
	if err != nil {
		return fmt.Errorf("connect NATS %s: %w", c.NatsURL, err)
	}
	defer nc.Close()

	// Build a spec-correct request: inject the required _meta keys the way
	// the shim would.
	var params map[string]json.RawMessage
	if err := json.Unmarshal([]byte(c.Params), &params); err != nil {
		return fmt.Errorf("--params is not a JSON object: %w", err)
	}
	if params == nil {
		// A JSON null decodes into a map cleanly and leaves it nil, so it is
		// the one non-object --params the check above lets past. Read as "no
		// params", the way the shim's injectMeta reads it.
		params = map[string]json.RawMessage{}
	}
	var meta map[string]json.RawMessage
	if raw, ok := params["_meta"]; ok {
		_ = json.Unmarshal(raw, &meta)
	}
	if meta == nil {
		meta = map[string]json.RawMessage{}
	}
	ver, _ := json.Marshal(mcpspec.ProtocolVersion)
	meta[mcpspec.MetaProtocolVersion] = ver
	metaRaw, _ := json.Marshal(meta)
	params["_meta"] = metaRaw
	paramsRaw, _ := json.Marshal(params)

	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": "cli-1", "method": c.Method, "params": json.RawMessage(paramsRaw),
	})

	name := ""
	var s string
	if raw, ok := params["name"]; ok && json.Unmarshal(raw, &s) == nil {
		name = s
	}
	if raw, ok := params["uri"]; ok && json.Unmarshal(raw, &s) == nil {
		name = s
	}

	cfg := wire.ClientConfig{Tenant: c.Tenant, User: c.User}
	if c.AcceptClaims {
		js, err := jetstream.New(nc)
		if err != nil {
			return fmt.Errorf("accept-claims: %w", err)
		}
		cfg.Claims = &wire.ObjectClaims{JS: js}
	}
	wc, err := wire.NewClient(nc, cfg)
	if err != nil {
		return err
	}
	stream, err := wc.Do(context.Background(), &wire.Request{
		Server:          c.Server,
		Method:          c.Method,
		Name:            name,
		ProtocolVersion: mcpspec.ProtocolVersion,
		Body:            body,
	})
	if err != nil {
		return err
	}

	for f := range stream.C {
		switch {
		case f.Err != nil:
			fmt.Printf("[%s] wire error %d: %s\n", f.Kind, f.Err.Code, f.Err.Message)
		case len(f.Body) == 0:
			fmt.Printf("[%s] (empty body: cancelled)\n", f.Kind)
		default:
			fmt.Printf("[%s] %s\n", f.Kind, f.Body)
		}
	}
	return nil
}
