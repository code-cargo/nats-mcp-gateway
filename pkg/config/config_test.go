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

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func write(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoadValidWithEnvExpansion(t *testing.T) {
	t.Setenv("TEST_GH_TOKEN", "tok-123")
	cfg, err := Load(write(t, `{
		"nats": {"url": "nats://x:4222"},
		"servers": {
			"github": {
				"transport": "stdio",
				"command": "gh-mcp",
				"env": {"GITHUB_TOKEN": "${TEST_GH_TOKEN}"}
			},
			"weather": {
				"protocol": "2026-07-28",
				"transport": "http",
				"url": "https://weather/mcp",
				"headers": {"Authorization": "Bearer ${TEST_GH_TOKEN}"}
			}
		}
	}`))
	require.NoError(t, err)
	assert.Equal(t, "tok-123", cfg.Servers["github"].Env["GITHUB_TOKEN"])
	assert.Equal(t, "Bearer tok-123", cfg.Servers["weather"].Headers["Authorization"])
	assert.ElementsMatch(t, []string{"github", "weather"}, cfg.ServerNames())
}

func TestLoadRejects(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantIn  string
	}{
		{"no servers", `{"nats":{}}`, "no servers"},
		{"bad server name", `{"servers":{"bad name":{"command":"x"}}}`, "not subject-token safe"},
		{"bad protocol", `{"servers":{"s":{"command":"x","protocol":"2024-01-01"}}}`, "unknown protocol"},
		{"stdio without command", `{"servers":{"s":{"transport":"stdio"}}}`, "requires command"},
		{"http without url", `{"servers":{"s":{"transport":"http"}}}`, "requires url"},
		{"unknown transport", `{"servers":{"s":{"transport":"grpc"}}}`, "unknown transport"},
		{"unknown field", `{"serverz":{}}`, "unknown field"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(write(t, tt.content))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantIn)
		})
	}
}
