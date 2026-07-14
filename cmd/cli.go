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

// GatewayCmd runs the gateway process.
type GatewayCmd struct {
	Config string `help:"Path to gateway config JSON." required:"" type:"existingfile" env:"NATSMCP_CONFIG"`

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
}

func (c *CallCmd) Run(g *Globals) error {
	return runCall(c, g)
}
