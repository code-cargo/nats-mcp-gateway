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
	"time"

	"github.com/alecthomas/kong"
)

// Globals holds flags shared by every subcommand.
type Globals struct {
	LogLevel  string           `help:"Log level (debug|info|warn|error)." default:"info" env:"NATSMCP_LOG_LEVEL"`
	LogFormat string           `help:"Log format (text|json)." default:"text" env:"NATSMCP_LOG_FORMAT"`
	Version   kong.VersionFlag `help:"Print version and exit."`
}

// CLI is the root Kong command tree.
type CLI struct {
	Globals

	Gateway GatewayCmd `cmd:"" help:"Run the gateway: front configured MCP servers over NATS."`
	Shim    ShimCmd    `cmd:"" help:"Run the stdio shim: bridge an MCP client's stdio to NATS."`
	Call    CallCmd    `cmd:"" help:"Send a single MCP request over NATS and print the reply frames (debug)."`
}

// GatewayCmd runs the gateway process. Its config comes from exactly one
// source: a local file (--config) or over NATS request/reply
// (--config-subject). Either way, only the SERVER SET reloads at runtime; the
// NATS connection, subject prefix, and queue group are fixed at boot.
type GatewayCmd struct {
	// File source.
	Config         string        `help:"Path to gateway config JSON (file source; SIGHUP reloads)." type:"existingfile" env:"NATSMCP_CONFIG" xor:"source"`
	ReloadInterval time.Duration `help:"File source: poll interval for change detection (0 disables polling)." default:"10s" env:"NATSMCP_RELOAD_INTERVAL"`

	// NATS fetch source. Connection params come from flags here, because the
	// gateway must connect before it can fetch its config.
	ConfigSubject       string        `help:"NATS subject to request config JSON from (fetch source)." env:"NATSMCP_CONFIG_SUBJECT" xor:"source"`
	ConfigEventsSubject string        `help:"NATS subject that signals a config change (fetch source)." default:"mcp.v1.cfg.changed" env:"NATSMCP_CONFIG_EVENTS_SUBJECT"`
	ConfigRefetch       time.Duration `help:"Fetch source: periodic re-fetch as the missed-event safety net." default:"60s" env:"NATSMCP_CONFIG_REFETCH"`
	NatsURL             string        `help:"NATS URL (fetch source)." default:"nats://127.0.0.1:4222" env:"NATSMCP_NATS_URL"`
	NatsCreds           string        `help:"NATS credentials file (fetch source)." env:"NATSMCP_NATS_CREDS"`
	SubjectPrefix       string        `help:"Wire subject prefix (fetch source)." default:"mcp.v1" env:"NATSMCP_SUBJECT_PREFIX"`
	QueueGroup          string        `help:"Wire queue group (fetch source)." default:"mcpgw" env:"NATSMCP_QUEUE_GROUP"`
	ScopeTenant         string        `help:"Serve only this tenant's subjects (scoped/per-user pod mode; requires --scope-user)." env:"NATSMCP_SCOPE_TENANT"`
	ScopeUser           string        `help:"Serve only this user's subjects (scoped/per-user pod mode; requires --scope-tenant)." env:"NATSMCP_SCOPE_USER"`

	// Version is injected by main.
	Version string `kong:"-"`
}

func (c *GatewayCmd) Run(g *Globals) error {
	return runGateway(c, g, c.Version)
}

// ShimCmd runs the client-side stdio shim. It deliberately has no config
// file: it lives inside .mcp.json entries, so flags and env only.
type ShimCmd struct {
	Server        string `help:"Name of the MCP server to front." required:"" env:"NATSMCP_SERVER"`
	NatsURL       string `help:"NATS server URL." default:"nats://127.0.0.1:4222" env:"NATSMCP_NATS_URL"`
	Creds         string `help:"Path to NATS credentials file." env:"NATSMCP_CREDS"`
	Tenant        string `help:"Tenant subject token." default:"default" env:"NATSMCP_TENANT"`
	User          string `help:"User subject token for attribution ('_' if unset). Must match the token this caller's NATS creds are scoped to under per-user auth." default:"_" env:"NATSMCP_USER"`
	SubjectPrefix string `help:"Wire subject prefix." default:"mcp.v1" env:"NATSMCP_SUBJECT_PREFIX"`
	InboxPrefix   string `help:"Custom NATS inbox prefix (per-tenant inbox isolation)." env:"NATSMCP_INBOX_PREFIX"`
}

func (c *ShimCmd) Run(g *Globals) error {
	return runShim(c, g)
}

// CallCmd sends one request and prints raw reply frames, for debugging the
// wire without an MCP client.
type CallCmd struct {
	Server  string `help:"Name of the MCP server to call." required:""`
	Method  string `help:"MCP method (e.g. tools/list)." required:""`
	Params  string `help:"JSON params." default:"{}"`
	NatsURL string `help:"NATS server URL." default:"nats://127.0.0.1:4222" env:"NATSMCP_NATS_URL"`
	Creds   string `help:"Path to NATS credentials file." env:"NATSMCP_CREDS"`
	Tenant  string `help:"Tenant subject token." default:"default" env:"NATSMCP_TENANT"`
	User    string `help:"User subject token for attribution." default:"_" env:"NATSMCP_USER"`
}

func (c *CallCmd) Run(g *Globals) error {
	return runCall(c, g)
}
