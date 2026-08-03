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

// buildCallRequest turns the CLI's flags into the wire request the shim would
// have sent for the same call: a spec-correct body with the required _meta
// keys injected, and the name the method's params field carries.
//
// It is separate from the send so it can be held to the property that makes
// this command worth having — that what it publishes is what a real client
// publishes. A debug tool that cannot reproduce a request the shim sends
// correctly turns every session into a hunt for a gateway bug that isn't there.
func buildCallRequest(c *CallCmd) (*wire.Request, error) {
	// Decode the flag through the same reader the gateway uses on a body,
	// rather than into a plain map: a map keeps the LAST of two duplicate
	// keys, so --params '{"name":"a","name":"b"}' would be re-marshalled below
	// into a well-formed call to "b" — a request the operator did not type,
	// built by the one tool whose job is to reproduce requests exactly.
	// DecodeParams refuses it here, at any depth, with the key named.
	//
	// It also lands the one --params that unmarshals into a nil map rather
	// than failing: a JSON null reads as "no params", the way the shim's
	// injectMeta reads it, so nothing below has a nil map to panic on.
	params, err := mcpspec.DecodeParams(json.RawMessage(c.Params))
	if err != nil {
		return nil, fmt.Errorf("--params: %w", err)
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

	// Read the name through the same accessor the shim uses, so the CLI can
	// never derive it from a field the method does not name. Reading it here
	// also catches the case-colliding sibling ("name" beside "Name") locally,
	// with the field named, rather than as a -32020 from the gateway.
	name, _, err := params.Name(c.Method)
	if err != nil {
		return nil, fmt.Errorf("--params: %w", err)
	}

	return &wire.Request{
		Server:          c.Server,
		Method:          c.Method,
		Name:            name,
		ProtocolVersion: mcpspec.ProtocolVersion,
		Body:            body,
	}, nil
}

func runCall(c *CallCmd, g *Globals) error {
	opts := []nats.Option{nats.Name("natsmcp-call")}
	if c.Creds != "" {
		opts = append(opts, nats.UserCredentials(c.Creds))
	}
	if c.InboxPrefix != "" {
		if err := wire.ValidateSubjectPrefix(c.InboxPrefix); err != nil {
			return fmt.Errorf("--inbox-prefix: %w", err)
		}
		opts = append(opts, nats.CustomInboxPrefix(c.InboxPrefix))
	}
	// NewClient checks this too; checking it here as well is what names the
	// flag that is wrong, the same way --inbox-prefix does above.
	if c.SubjectPrefix != "" {
		if err := wire.ValidateSubjectPrefix(c.SubjectPrefix); err != nil {
			return fmt.Errorf("--subject-prefix: %w", err)
		}
	}
	nc, err := nats.Connect(c.NatsURL, opts...)
	if err != nil {
		return connectFailure(c.NatsURL, err)
	}
	defer nc.Close()

	req, err := buildCallRequest(c)
	if err != nil {
		return err
	}

	cfg := wire.ClientConfig{Prefix: c.SubjectPrefix, Tenant: c.Tenant, User: c.User}
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
	stream, err := wc.Do(context.Background(), req)
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
