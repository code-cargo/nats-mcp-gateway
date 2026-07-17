# nats-mcp-gateway

Front MCP servers over a NATS fabric. stdio servers that were never meant to
leave a laptop become network services; NATS accounts and subject permissions
become the MCP authorization plane; the gateway injects backend credentials so
end users never hold them.

```
[MCP client]           [gateway fleet, queue group]       [real MCP servers]
 2025-11-25               2026-07-28 on NATS                 2025-11-25
    │ stdio                       │                               │
    ▼                             ▼                               ▼
┌─────────┐  ═══ NATS ═══>  ┌─────────┐  ── stdio subprocess ──> github-mcp
│  shim   │                 │ gateway │  ── Streamable HTTP ───> weather
└─────────┘                 └─────────┘
 compat wing                 compat wing
```

One binary, `natsmcp`, three subcommands:

- `natsmcp gateway --config gateway.json` — fronts configured MCP servers
  over NATS (a `micro` service; run replicas for HA, they queue-group).
- `natsmcp shim --server NAME` — the client edge; lives inside `.mcp.json`
  and bridges an MCP client's stdio to the wire.
- `natsmcp call --server NAME --method M [--params JSON]` — debug CLI that
  prints raw reply frames.

## The wire (v1)

The NATS wire carries **MCP 2026-07-28 exclusively**. That revision abolishes
server-initiated requests, the initialize handshake, and protocol sessions,
which reduces the transport contract to *one request in → a stream of 0+
notifications → exactly one response* — precisely NATS request/reply with a
streaming reply inbox. Statelessness is what lets gateway replicas run in a
queue group with zero coordination. Today's 2025-11-25 clients and servers
are bridged at the edges (`pkg/shim/legacyclient.go`, `pkg/backend/legacy`),
never on the bus.

```
REQUEST   mcp.v1.req.{tenant}.{server}.{method-tokens}.{name}
REPLY     one inbox per request; frames tagged Mcp-Frame: msg|end|err|ka
CONTROL   {reply}.ctl — cancellation
```

Frames: `msg` carries one JSON-RPC notification; `end` carries the response
(empty body = cancelled, no response exists); `err` carries a
gateway-synthesized error; `ka` is a 15s keepalive so idle streams are
distinguishable from dead ones. A broken stream is reported as error `-32010`
and the client re-issues — exactly what the 2026-07-28 spec mandates.

## The authorization plane is NATS

The gateway contains **no authorization code**. Subject permissions decide
everything:

```
# this user may list tools and call exactly one tool on one server
publish: [
  "mcp.v1.req.acme.github.tools.list._",
  "mcp.v1.req.acme.github.tools.call.get_issue",
  "_INBOX.>"
]
subscribe: ["_INBOX.>"]
```

Two things make that real:

1. **The integrity check** (`pkg/proxy/integrity.go`): the gateway proves the
   subject NATS authorized, the mirrored headers, and the JSON-RPC body all
   agree, and rejects any disagreement with `-32020`. Without it a caller
   could publish a `delete_repo` body to a `get_issue` subject.
2. **The tenant token is NATS-enforced**: a user cannot publish into another
   tenant's subjects, so the gateway trusts the tenant it parses from the
   subject. Backend processes are pooled per `(server, tenant, credentials)`
   and **never shared across tenants**.

Tool names that are not subject-token-safe (`A-Za-z0-9_-`) map to the `_`
token; per-tool grants for those are all-or-nothing, and the integrity check
prevents token-safe names from hiding under `_`.

## Configuration

```json
{
  "nats": { "url": "nats://gw:pw@nats:4222", "credsFile": "" },
  "servers": {
    "github": {
      "protocol": "2025-11-25",
      "transport": "stdio",
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "env": { "GITHUB_TOKEN": "${GITHUB_TOKEN}" }
    },
    "weather": {
      "protocol": "2026-07-28",
      "transport": "http",
      "url": "https://weather.internal/mcp",
      "headers": { "Authorization": "Bearer ${WEATHER_TOKEN}" }
    }
  }
}
```

`protocol` defaults to `2025-11-25` (that is what exists in the wild).
`${VAR}` expands from the gateway's environment — the credential-injection
point; clients never see backend secrets. Set `max_payload: 8MB` on the NATS
server: MCP results carry base64 blobs, and oversize messages fail with a
legible `-32012` instead of a hang.

## Config sources / hot reload

The **server set reloads at runtime** — add, remove, or re-credential a fronted
MCP server without restarting the gateway. (The NATS connection, subject
prefix, and queue group are fixed at boot; only servers reload.) Reloads are
safe: a malformed revision is rejected and the last good config keeps serving,
a removed server stops answering (clients get `-32011` and re-issue), and a
changed server's pooled processes are evicted so the next call spawns from the
new definition. In-flight calls on a removed/changed server fail retryably
(`-32010`); unchanged servers are never disturbed.

Config comes from a **source**, selected by flag:

```
natsmcp gateway --config /etc/natsmcp.json          # file; SIGHUP or poll reloads
natsmcp gateway --config-subject mcp.v1.cfg.request # fetch over NATS request/reply
```

- **File** (`--config`, `--reload-interval` default 10s): re-reads on change and
  on `SIGHUP`. Good for demos and simple deployments. On K8s, a ConfigMap mount
  updates atomically, so polling picks it up.
- **NATS fetch** (`--config-subject`, `--config-events-subject`,
  `--config-refetch`): requests the config JSON over NATS and re-fetches when a
  change event is published (with a periodic re-fetch as the missed-event safety
  net). Secrets stay in the controller and ride only the authenticated NATS
  connection — nothing at rest in a ConfigMap or KV bucket. **Controller
  contract:** respond to the request subject with the same config JSON the file
  source parses, and publish any message to the events subject on change; NATS
  permissions fence both subjects to the controller.

### Reloading from something else

The reload machinery is a small building block, not a fixed pair of modes. The
whole extension surface is one interface in `pkg/configsource`:

```go
type Source interface { Watch(ctx context.Context) <-chan Update }
```

Three ways to hot-reload from a new backend, cheapest first:

1. **Use a built-in** — `configsource.NewFile(...)` or `&configsource.NATS{...}`.
2. **Wrap a fetch function** with `configsource.Poll` — for HTTP, Vault, a KV
   store, S3, a database. You write one fetch; change-detection, the initial
   load, and lifecycle come for free:
   ```go
   src := configsource.Poll(30*time.Second, func(ctx context.Context) (*config.Config, error) {
       raw, err := fetchFromVault(ctx)
       if err != nil { return nil, err }
       return config.Parse(raw)
   })
   ```
3. **Implement `Source`** — for push systems (a Kubernetes informer, a webhook)
   that already know when config changed. Wrap it in `configsource.Dedup` to
   suppress no-op emissions.

`pkg/reconcile.Reconciler` is the source-agnostic engine that applies a config
to the running wire + pool; `configsource.Run` is the consume loop that keeps
the last good config live on error. An embedder can drive `Reconciler.Apply`
from its own control loop and skip `Source` entirely.

Client side (`.mcp.json`):

```json
{
  "mcpServers": {
    "github": {
      "command": "natsmcp",
      "args": ["shim", "--server", "github"],
      "env": { "NATSMCP_NATS_URL": "nats://...", "NATSMCP_TENANT": "acme" }
    }
  }
}
```

## Demo

`make demo` (needs `nats-server` and `npx` on PATH) walks the whole story:
tools/list and a tool call through a real `server-everything` subprocess,
then the same call as a permission-restricted user failing **with no gateway
code running**. See `demo/README-steps` inside `demo/run.sh`.

## Known v1 limits

- Sampling, elicitation, and roots are not bridged: the gateway advertises no
  such capabilities to legacy backends, so conformant servers will not use
  them (all three are deprecated as of 2026-07-28). A server-initiated
  request is answered with `-32601` rather than wedging the subprocess.
- `notifications/message` (logging) has no per-request correlator in a
  multiplexed process; it lands in the gateway's own logs, tagged with
  tenant and server, and is never forwarded.
- Payloads are bounded by NATS `max_payload`; no chunking (planned:
  claim-check via JetStream Object Store).
- `resourceSubscriptions` inside `subscriptions/listen` is not yet bridged
  for legacy backends (`*/list_changed` events are).

## Development

`make ci` = format-check + staticcheck + build + tests. Tests run anywhere Go
does (embedded NATS server; the fake MCP server is the re-exec'd test
binary). `go test ./... -race` is clean.
