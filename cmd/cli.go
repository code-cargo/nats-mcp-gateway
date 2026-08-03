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
	"os"
	"time"

	"github.com/alecthomas/kong"
)

// credsEnvNames is the credentials env var under each subcommand's own
// documented name, followed by the other subcommand's, which is accepted as
// an alias. Setting the one you had read about on the wrong subcommand used
// not to fail — it was ignored, and the process connected with no credentials
// at all, which against a server permitting anonymous connections means a
// shim that runs and quietly has no identity. Neither name is renamed:
// a documented variable that stops working is a broken deployment.
var (
	gatewayCredsEnv = []string{"NATSMCP_NATS_CREDS", "NATSMCP_CREDS"}
	clientCredsEnv  = []string{"NATSMCP_CREDS", "NATSMCP_NATS_CREDS"}
)

// credsFromEnv resolves the credentials path the tag-level env list could not.
//
// Kong takes the first name in an `env:"A,B"` list that is SET — it calls
// os.LookupEnv and stops on ok, empty value or not. Set-but-empty is a shape
// deployments produce by accident (a Kubernetes env entry sourced from a
// secret key that is not populated yet, a shell exporting an unset variable),
// and under it a blank primary shadows the alias this pair exists for, which
// is the credential-less connect all over again. First NON-EMPTY is what the
// pair was always meant to mean.
func credsFromEnv(current string, names ...string) string {
	if current != "" {
		return current
	}
	for _, name := range names {
		if v := os.Getenv(name); v != "" {
			return v
		}
	}
	return current
}

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
// source: a local file (--config), over NATS request/reply (--config-subject),
// or inline as a document (--config-json). Whichever it is, only the SERVER
// SET reloads at runtime; the NATS connection, subject prefix, queue group and
// scope are fixed at boot.
type GatewayCmd struct {
	// File source. Its document owns the connection/scope/pool/claim-check
	// settings the fetch-source flags below carry, so passing one of those
	// alongside --config is REJECTED unless the document already agrees — the
	// mirror of the inline source's rejected `nats` block, for the same reason:
	// scope injected as pod env must never be dropped on the way to a gateway
	// that then serves every tenant.
	Config         string        `help:"Path to gateway config JSON (file source; SIGHUP reloads)." type:"existingfile" env:"NATSMCP_CONFIG" xor:"source"`
	ReloadInterval time.Duration `help:"File source: poll interval for change detection (0 disables polling)." default:"10s" env:"NATSMCP_RELOAD_INTERVAL"`

	// Inline source: the whole config document as a string, for environments
	// with no file mount and no config responder (the scoped stdio pod). It
	// never reloads; connection params and scope come from the flags below,
	// since the document is plain-only and the NATS URL carries the
	// credential — an inline `nats` block is REJECTED rather than ignored, so
	// a document that asks to be scoped can never silently serve everyone.
	// Its `pool` and `claimCheck` blocks (no credentials) are honored.
	ConfigJSON string `help:"Inline gateway config JSON (config source; same schema as --config, never reloads)." env:"NATSMCP_CONFIG_JSON" xor:"source"`

	// NATS fetch source, plus the connection/wire params the fetch AND inline
	// sources both take from flags — fetch because the gateway must connect
	// before it can fetch its config, inline because its document is
	// plain-only. Only the file source reads these from its `nats` block.
	ConfigSubject       string        `help:"NATS subject to request config JSON from (fetch source)." env:"NATSMCP_CONFIG_SUBJECT" xor:"source"`
	ConfigEventsSubject string        `help:"NATS subject that signals a config change (fetch source)." default:"mcp.v1.cfg.changed" env:"NATSMCP_CONFIG_EVENTS_SUBJECT"`
	ConfigRefetch       time.Duration `help:"Fetch source: periodic re-fetch as the missed-event safety net." default:"60s" env:"NATSMCP_CONFIG_REFETCH"`
	NatsURL             string        `help:"NATS URL (fetch + inline sources)." default:"nats://127.0.0.1:4222" env:"NATSMCP_NATS_URL"`
	NatsCreds           string        `help:"NATS credentials file (fetch + inline sources)." env:"NATSMCP_NATS_CREDS,NATSMCP_CREDS"`
	SubjectPrefix       string        `help:"Wire subject prefix (fetch + inline sources)." default:"mcp.v1" env:"NATSMCP_SUBJECT_PREFIX"`
	InboxPrefix         string        `help:"Custom NATS inbox prefix for this process's own request/reply (fetch + inline sources; the file source uses nats.inboxPrefix). A scoped instance should set it so its identity can be granted a narrow inbox instead of _INBOX.>." env:"NATSMCP_INBOX_PREFIX"`
	QueueGroup          string        `help:"Wire queue group (fetch + inline sources; default mcpgw, or mcpgw.{tenant}[.{user}] for scoped instances)." env:"NATSMCP_QUEUE_GROUP"`
	ScopeTenant         string        `help:"Serve only this tenant's subjects (org deployment; per-user pod when combined with --scope-user)." env:"NATSMCP_SCOPE_TENANT"`
	ScopeUser           string        `help:"Serve only this user's subjects (per-user pod mode; requires --scope-tenant)." env:"NATSMCP_SCOPE_USER"`

	// Backend pool limits. Boot-fixed, so the fetch source can only get them
	// from flags — its config arrives after the pool is built. The file source
	// and an inline document supply them via their `pool` block instead; an
	// inline document that omits the block falls back to these.
	PoolMaxConcurrent     int           `help:"Max concurrent in-flight calls per backend (0 = pool default)." env:"NATSMCP_POOL_MAX_CONCURRENT"`
	PoolMaxProcsPerTenant int           `help:"Max live backends per tenant (0 = pool default, 16). Size to roughly users × stdio servers for a tenant-scoped deployment." env:"NATSMCP_POOL_MAX_PROCS_PER_TENANT"`
	PoolIdleTTL           time.Duration `help:"Reap backends idle this long (0 = pool default, 5m)." env:"NATSMCP_POOL_IDLE_TTL"`
	PoolMaxLifetime       time.Duration `help:"Recycle backends older than this regardless of activity (0 = pool default, 1h)." env:"NATSMCP_POOL_MAX_LIFETIME"`

	// Claim-check (fetch source; the file and inline sources read the
	// claimCheck block from their document).
	ClaimCheck    bool          `help:"Park oversize responses in a JetStream Object Store for claim-accepting clients (fetch source; needs JetStream)." env:"NATSMCP_CLAIM_CHECK"`
	ClaimMaxAge   time.Duration `help:"Claim bucket TTL — the cleanup backstop behind client deletes." default:"5m" env:"NATSMCP_CLAIM_MAX_AGE"`
	ClaimMaxBytes int64         `help:"Per-tenant claim bucket size cap in bytes." default:"1073741824" env:"NATSMCP_CLAIM_MAX_BYTES"`

	// Version is injected by main.
	Version string `kong:"-"`
}

// AfterApply runs inside kong.Parse, and only for the selected command.
func (c *GatewayCmd) AfterApply() error {
	c.NatsCreds = credsFromEnv(c.NatsCreds, gatewayCredsEnv...)
	return nil
}

func (c *GatewayCmd) Run(g *Globals) error {
	return runGateway(c, g, c.Version)
}

// ShimCmd runs the client-side stdio shim. It deliberately has no config
// file: it lives inside .mcp.json entries, so flags and env only.
type ShimCmd struct {
	Server        string `help:"Name of the MCP server to front." required:"" env:"NATSMCP_SERVER"`
	NatsURL       string `help:"NATS server URL." default:"nats://127.0.0.1:4222" env:"NATSMCP_NATS_URL"`
	Creds         string `help:"Path to NATS credentials file." env:"NATSMCP_CREDS,NATSMCP_NATS_CREDS"`
	Tenant        string `help:"Tenant subject token." default:"default" env:"NATSMCP_TENANT"`
	User          string `help:"User subject token for attribution ('_' if unset). Must match the token this caller's NATS creds are scoped to under per-user auth." default:"_" env:"NATSMCP_USER"`
	SubjectPrefix string `help:"Wire subject prefix." default:"mcp.v1" env:"NATSMCP_SUBJECT_PREFIX"`
	InboxPrefix   string `help:"Custom NATS inbox prefix (per-tenant inbox isolation)." env:"NATSMCP_INBOX_PREFIX"`
	AcceptClaims  bool   `help:"Accept claim-checked oversize responses (needs read access to this tenant's claim bucket)." env:"NATSMCP_ACCEPT_CLAIMS"`
}

// AfterApply runs inside kong.Parse, and only for the selected command.
func (c *ShimCmd) AfterApply() error {
	c.Creds = credsFromEnv(c.Creds, clientCredsEnv...)
	return nil
}

func (c *ShimCmd) Run(g *Globals) error {
	return runShim(c, g)
}

// CallCmd sends one request and prints raw reply frames, for debugging the
// wire without an MCP client. It carries the shim's prefix flags because the
// deployments worth pointing a wire debugger at are the awkward ones: against
// a custom --subject-prefix a hardcoded mcp.v1 reports "no gateway" on a
// perfectly healthy fleet, and a fenced identity granted only
// _INBOX_acme_u1.> cannot subscribe to its own replies at all.
type CallCmd struct {
	Server        string `help:"Name of the MCP server to call." required:""`
	Method        string `help:"MCP method (e.g. tools/list)." required:""`
	Params        string `help:"JSON params." default:"{}"`
	NatsURL       string `help:"NATS server URL." default:"nats://127.0.0.1:4222" env:"NATSMCP_NATS_URL"`
	Creds         string `help:"Path to NATS credentials file." env:"NATSMCP_CREDS,NATSMCP_NATS_CREDS"`
	Tenant        string `help:"Tenant subject token." default:"default" env:"NATSMCP_TENANT"`
	User          string `help:"User subject token for attribution." default:"_" env:"NATSMCP_USER"`
	SubjectPrefix string `help:"Wire subject prefix." default:"mcp.v1" env:"NATSMCP_SUBJECT_PREFIX"`
	InboxPrefix   string `help:"Custom NATS inbox prefix (per-tenant inbox isolation)." env:"NATSMCP_INBOX_PREFIX"`
	AcceptClaims  bool   `help:"Accept claim-checked oversize responses." env:"NATSMCP_ACCEPT_CLAIMS"`
}

// AfterApply runs inside kong.Parse, and only for the selected command.
func (c *CallCmd) AfterApply() error {
	c.Creds = credsFromEnv(c.Creds, clientCredsEnv...)
	return nil
}

func (c *CallCmd) Run(g *Globals) error {
	return runCall(c, g)
}
