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

package jsonrpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeKinds(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Kind
		wantErr bool
	}{
		{"request string id", `{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{}}`, KindRequest, false},
		{"request number id", `{"jsonrpc":"2.0","id":7,"method":"tools/list"}`, KindRequest, false},
		{"request zero id", `{"jsonrpc":"2.0","id":0,"method":"tools/list"}`, KindRequest, false},
		{"notification", `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"1"}}`, KindNotification, false},
		{"response result", `{"jsonrpc":"2.0","id":"1","result":{"tools":[]}}`, KindResponse, false},
		{"response null result", `{"jsonrpc":"2.0","id":"1","result":null}`, KindResponse, false},
		{"response error", `{"jsonrpc":"2.0","id":"1","error":{"code":-32601,"message":"nope"}}`, KindResponse, false},
		{"error with null id", `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse"}}`, KindResponse, false},
		{"wrong version", `{"jsonrpc":"1.0","id":"1","method":"x"}`, KindInvalid, true},
		{"missing version", `{"id":"1","method":"x"}`, KindInvalid, true},
		{"empty object", `{"jsonrpc":"2.0"}`, KindInvalid, true},
		{"not json", `nope`, KindInvalid, true},
		{"array batch rejected", `[{"jsonrpc":"2.0","id":"1","method":"x"}]`, KindInvalid, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Decode([]byte(tt.input))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, m.Kind())
		})
	}
}

func TestIDPreservation(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{"string", `"abc"`},
		{"number", `42`},
		{"zero", `0`},
		{"null", `null`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := `{"jsonrpc":"2.0","id":` + tt.id + `,"result":{}}`
			m, err := Decode([]byte(input))
			require.NoError(t, err)
			out, err := Encode(m)
			require.NoError(t, err)
			var decoded map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(out, &decoded))
			assert.JSONEq(t, tt.id, string(decoded["id"]), "id must round-trip byte-exact")
		})
	}
}

func TestNotificationHasNoID(t *testing.T) {
	m := NewNotification("notifications/progress", nil)
	out, err := Encode(m)
	require.NoError(t, err)
	assert.NotContains(t, string(out), `"id"`)
}

func TestNewErrorResponseNilID(t *testing.T) {
	m := NewErrorResponse(nil, -32700, "parse error", map[string]int{"size": 9})
	out, err := Encode(m)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"id":null`)
	assert.Contains(t, string(out), `"code":-32700`)
	assert.Contains(t, string(out), `"size":9`)
}

func TestParamsRoundTripVerbatim(t *testing.T) {
	// A params blob with unicode, nested _meta, and unknown fields must
	// survive decode/encode untouched — the proxy forwards bodies verbatim.
	params := `{"name":"crème.brûlée","_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"},"x":[1,2,3]}`
	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` + params + `}`
	m, err := Decode([]byte(input))
	require.NoError(t, err)
	assert.JSONEq(t, params, string(m.Params))
}
