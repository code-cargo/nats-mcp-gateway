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

// Package config loads the gateway's JSON configuration. JSON (not YAML)
// because .mcp.json and claude_desktop_config.json already are, and because
// encoding/json costs no new dependency. ${VAR} references are expanded from
// the gateway's environment at load time.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// Config is the gateway configuration.
type Config struct {
	NATS    NATS              `json:"nats"`
	Servers map[string]Server `json:"servers"`
	Pool    Pool              `json:"pool"`
}

// NATS is the gateway's connection settings.
type NATS struct {
	URL           string `json:"url"`
	CredsFile     string `json:"credsFile"`
	SubjectPrefix string `json:"subjectPrefix"`
	QueueGroup    string `json:"queueGroup"`
}

// Server declares one fronted MCP server.
type Server struct {
	// Protocol the backend speaks: "2025-11-25" (default — that is what
	// exists in the wild) or "2026-07-28".
	Protocol string `json:"protocol"`
	// Transport: "stdio" or "http".
	Transport string `json:"transport"`

	// stdio transport.
	Command string            `json:"command"`
	Args    []string          `json:"args"`
	Env     map[string]string `json:"env"`

	// http transport.
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`

	// DiscoverTTLMs is served in DiscoverResult.ttlMs (default 300000).
	DiscoverTTLMs int `json:"discoverTtlMs"`
}

// Pool mirrors backend.PoolConfig knobs.
type Pool struct {
	MaxConcurrent     int    `json:"maxConcurrent"`
	MaxProcsPerTenant int    `json:"maxProcsPerTenant"`
	IdleTTL           string `json:"idleTtl"`
	MaxLifetime       string `json:"maxLifetime"`
}

// Load reads, env-expands, parses, and validates a config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	expanded := os.Expand(string(raw), func(key string) string {
		return os.Getenv(key)
	})
	var cfg Config
	dec := json.NewDecoder(strings.NewReader(expanded))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if len(c.Servers) == 0 {
		return fmt.Errorf("no servers configured")
	}
	for name, s := range c.Servers {
		if !wire.TokenSafe(name) {
			return fmt.Errorf("server name %q is not subject-token safe (%s)", name, `A-Za-z0-9_-`)
		}
		switch s.Protocol {
		case "", mcpspec.LegacyProtocolVersion, mcpspec.ProtocolVersion:
		default:
			return fmt.Errorf("server %q: unknown protocol %q", name, s.Protocol)
		}
		switch s.Transport {
		case "", "stdio":
			if s.Command == "" {
				return fmt.Errorf("server %q: stdio transport requires command", name)
			}
		case "http":
			if s.URL == "" {
				return fmt.Errorf("server %q: http transport requires url", name)
			}
		default:
			return fmt.Errorf("server %q: unknown transport %q", name, s.Transport)
		}
	}
	return nil
}

// ServerNames returns the configured server names.
func (c *Config) ServerNames() []string {
	names := make([]string, 0, len(c.Servers))
	for name := range c.Servers {
		names = append(names, name)
	}
	return names
}
