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
	"encoding/json"
	"fmt"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// The integrity check is what makes NATS subject permissions mean what they
// say. NATS authorized the SUBJECT; the gateway forwards the BODY. Without
// proving they agree, a caller granted only tools.call.get_issue could
// publish a delete_repository body to that subject and we would forward it.
//
// Verified relations:
//
//	subject.method  == header Mcp-Method == body method
//	subject.name    == NameToken(body name)   (per the "_" fallback rules)
//	header Mcp-Name == body params.name/uri   (for the three named methods)
//	header MCP-Protocol-Version == body _meta protocolVersion (and supported)
//
// Any disagreement is a single error code, -32020, mirroring the spec's
// treatment of the same bug at the Streamable HTTP edge.

// CheckError is an integrity failure with its JSON-RPC error code and
// optional data payload.
type CheckError struct {
	Code    int
	Message string
	Data    any
}

func (e *CheckError) Error() string { return e.Message }

func mismatch(format string, args ...any) *CheckError {
	return &CheckError{Code: mcpspec.ErrHeaderMismatch, Message: fmt.Sprintf(format, args...)}
}

// paramsProbe is the only body introspection the gateway ever performs.
type paramsProbe struct {
	Name string                     `json:"name"`
	URI  string                     `json:"uri"`
	Meta map[string]json.RawMessage `json:"_meta"`
}

// Check validates one inbound request against the subject NATS enforced.
func Check(in *wire.Inbound) *CheckError {
	msg := in.Msg
	if msg.Kind() != jsonrpc.KindRequest {
		return &CheckError{
			Code:    jsonrpc.CodeInvalidRequest,
			Message: "wire requests must be JSON-RPC requests (notifications travel on the control subject)",
		}
	}

	// Method: subject == header == body.
	if hm := in.Header.Get(wire.HeaderMethod); hm != msg.Method {
		return mismatch("Mcp-Method header %q does not match body method %q", hm, msg.Method)
	}
	if in.Subject.Method != msg.Method {
		return mismatch("subject method %q does not match body method %q", in.Subject.Method, msg.Method)
	}

	var probe paramsProbe
	if len(msg.Params) > 0 {
		if err := json.Unmarshal(msg.Params, &probe); err != nil {
			return &CheckError{Code: jsonrpc.CodeInvalidRequest, Message: "params is not a JSON object"}
		}
	}

	// Name: which body field is authoritative depends on the method.
	bodyName := ""
	named := false
	switch msg.Method {
	case mcpspec.MethodToolsCall, mcpspec.MethodPromptsGet:
		bodyName, named = probe.Name, true
	case mcpspec.MethodResourcesRead:
		bodyName, named = probe.URI, true
	}

	if named {
		if bodyName == "" {
			return mismatch("method %q requires a name/uri in params", msg.Method)
		}
		if hn := in.Header.Get(wire.HeaderName); hn != bodyName {
			return mismatch("Mcp-Name header %q does not match body name %q", hn, bodyName)
		}
	}

	// Subject name token rules. For tools/call and prompts/get the token is
	// derived from the body name; resources/read URIs are never token-safe
	// by grammar, so they use "_" like every unnamed method.
	wantToken := wire.NameUnset
	if msg.Method == mcpspec.MethodToolsCall || msg.Method == mcpspec.MethodPromptsGet {
		wantToken = wire.NameToken(bodyName)
	}
	if in.Subject.Name != wantToken {
		return mismatch("subject name token %q does not match required token %q for %s %q",
			in.Subject.Name, wantToken, msg.Method, bodyName)
	}

	// Protocol version: header == body _meta, and supported. The three
	// required _meta keys replace the removed initialize handshake, so a
	// missing version is a version error, not a mismatch.
	bodyVer := ""
	if raw, ok := probe.Meta[mcpspec.MetaProtocolVersion]; ok {
		_ = json.Unmarshal(raw, &bodyVer)
	}
	if hv := in.Header.Get(wire.HeaderProtocolVersion); hv != bodyVer {
		return mismatch("MCP-Protocol-Version header %q does not match body _meta %q", hv, bodyVer)
	}
	if bodyVer != mcpspec.ProtocolVersion {
		return &CheckError{
			Code:    mcpspec.ErrUnsupportedProtocolVersion,
			Message: fmt.Sprintf("protocol version %q is not supported", bodyVer),
			Data: map[string]any{
				"supported": []string{mcpspec.ProtocolVersion},
				"requested": bodyVer,
			},
		}
	}
	return nil
}
