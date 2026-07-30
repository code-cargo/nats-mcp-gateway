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
	"errors"
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
// treatment of the same bug at the Streamable HTTP edge. So is a body that
// has no single meaning to begin with: params is read through
// mcpspec.DecodeParams, which matches keys exactly and rejects duplicates,
// because a check that reads the body differently from the backend has
// verified nothing.
//
// What this is NOT is a schema validator. clientCapabilities is required on a
// 2026-07-28 request, but it is mirrored into no header and named by no
// subject token, so there is nothing here for it to disagree with — the
// backend judges it, as it judges every other body field. Adding presence
// checks for individual body fields here would make the gateway a second,
// partial implementation of a schema the wire otherwise moves opaquely.

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

// badParams maps a params decode failure onto its JSON-RPC code. An
// ambiguous key is valid JSON that means two things at once, so the failure
// is not "you sent garbage" but "these headers cannot be proven to agree
// with this body" — which is exactly -32020. Everything else is a shape
// error the caller can see for themselves.
func badParams(err error) *CheckError {
	var ambiguous *mcpspec.AmbiguousKeyError
	if errors.As(err, &ambiguous) {
		return mismatch("params cannot be authorized: %v", err)
	}
	return &CheckError{Code: jsonrpc.CodeInvalidRequest, Message: err.Error()}
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

	// The body is forwarded verbatim, so it is read the way the backend will
	// read it: exact keys, no duplicates, no case-colliding siblings. See
	// pkg/mcpspec/params.go for why a tagged struct here was an authorization
	// bypass rather than a stylistic choice.
	params, err := mcpspec.DecodeParams(msg.Params)
	if err != nil {
		return badParams(err)
	}

	// Name: which body field is authoritative depends on the method.
	bodyName, named, err := params.Name(msg.Method)
	if err != nil {
		return badParams(err)
	}

	if named {
		if bodyName == "" {
			return mismatch("method %q requires a name/uri in params", msg.Method)
		}
		// Decode before comparing: a name that is not header-safe (a resource
		// URI, a tool named in a non-Latin script) arrives base64-sentinel
		// encoded, and a raw comparison would reject a conformant client.
		hn, err := mcpspec.DecodeHeaderValue(in.Header.Get(wire.HeaderName))
		if err != nil {
			return mismatch("Mcp-Name header is malformed: %v", err)
		}
		if hn != bodyName {
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
	bodyVer, err := params.ProtocolVersion()
	if err != nil {
		return badParams(err)
	}
	hv, err := mcpspec.DecodeHeaderValue(in.Header.Get(wire.HeaderProtocolVersion))
	if err != nil {
		return mismatch("MCP-Protocol-Version header is malformed: %v", err)
	}
	if hv != bodyVer {
		return mismatch("MCP-Protocol-Version header %q does not match body _meta %q", hv, bodyVer)
	}
	if !mcpspec.IsSupportedProtocolVersion(bodyVer) {
		return &CheckError{
			Code:    mcpspec.ErrUnsupportedProtocolVersion,
			Message: fmt.Sprintf("protocol version %q is not supported", bodyVer),
			Data: map[string]any{
				"supported": mcpspec.SupportedProtocolVersions,
				"requested": bodyVer,
			},
		}
	}
	return nil
}
