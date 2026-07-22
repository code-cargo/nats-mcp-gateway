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

// Package jsonrpc is a minimal JSON-RPC 2.0 envelope, deliberately shallow:
// params and result stay as raw bytes so the proxy never learns MCP schema.
// IDs are kept as raw bytes too, preserving the string-vs-number distinction
// of the original message exactly.
package jsonrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Standard JSON-RPC 2.0 error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// Message is a JSON-RPC 2.0 request, notification, or response. Exactly one
// Kind applies; use Kind() rather than inspecting fields.
type Message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is a JSON-RPC 2.0 error object.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("jsonrpc: %d %s", e.Code, e.Message)
}

// Kind distinguishes the three JSON-RPC message shapes.
type Kind int

const (
	KindInvalid Kind = iota
	KindRequest
	KindNotification
	KindResponse
)

// Kind reports the message shape. A request has a method and an id; a
// notification has a method and no id; a response has no method and either a
// result or an error.
func (m *Message) Kind() Kind {
	switch {
	case m.Method != "" && m.HasID():
		return KindRequest
	case m.Method != "":
		return KindNotification
	case m.Result != nil || m.Error != nil:
		return KindResponse
	default:
		return KindInvalid
	}
}

// HasID reports whether the message carries an id. A literal null id (used in
// error responses to unparseable requests) counts as present.
func (m *Message) HasID() bool {
	return len(m.ID) > 0
}

// IDKey returns the id as a comparable map key ("" if absent). Raw bytes are
// canonical enough: JSON-RPC forbids fractional ids in practice, and clients
// echo ids verbatim, so byte equality is id equality for correlation.
func (m *Message) IDKey() string {
	return string(bytes.TrimSpace(m.ID))
}

// Decode parses a single JSON-RPC message. It rejects non-objects and wrong
// jsonrpc versions but deliberately validates no deeper: bodies are forwarded
// verbatim, and the far end owns semantic validation.
func Decode(data []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("jsonrpc: decode: %w", err)
	}
	if m.JSONRPC != "2.0" {
		return nil, fmt.Errorf("jsonrpc: unsupported version %q", m.JSONRPC)
	}
	if m.Kind() == KindInvalid {
		return nil, fmt.Errorf("jsonrpc: message is neither request, notification, nor response")
	}
	return &m, nil
}

// Encode serializes a message.
func Encode(m *Message) ([]byte, error) {
	m.JSONRPC = "2.0"
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: encode: %w", err)
	}
	return data, nil
}

// NewRequest builds a request with a string id.
func NewRequest(id, method string, params json.RawMessage) *Message {
	idRaw, _ := json.Marshal(id)
	return &Message{JSONRPC: "2.0", ID: idRaw, Method: method, Params: params}
}

// NewNotification builds a notification.
func NewNotification(method string, params json.RawMessage) *Message {
	return &Message{JSONRPC: "2.0", Method: method, Params: params}
}

// NewResponse builds a success response echoing the given raw id.
func NewResponse(id json.RawMessage, result json.RawMessage) *Message {
	if result == nil {
		result = json.RawMessage(`{}`)
	}
	return &Message{JSONRPC: "2.0", ID: id, Result: result}
}

// NewErrorResponse builds an error response echoing the given raw id. A nil
// id becomes a literal null per JSON-RPC 2.0.
func NewErrorResponse(id json.RawMessage, code int, message string, data any) *Message {
	if id == nil {
		id = json.RawMessage("null")
	}
	var raw json.RawMessage
	if data != nil {
		raw, _ = json.Marshal(data)
	}
	return &Message{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &Error{Code: code, Message: message, Data: raw},
	}
}
