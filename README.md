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
│  shim   │        ║        │ gateway │  ── Streamable HTTP ───> weather
└─────────┘        ║        └─────────┘
 compat wing       ║         compat wing
                   ║
                   ║  {tenant}.{user}-scoped subjects
                   ╚═══════> ┌───────────────────────────────────┐
                             │ per-user pod                      │
                             │ gateway (scoped) ── stdio ──> aws │
                             └───────────────────────────────────┘
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
REQUEST   mcp.v1.req.{tenant}.{user}.{server}.{method-tokens}.{name}
REPLY     one inbox per request; frames tagged Mcp-Frame: msg|end|err|ka
CONTROL   {reply}.ctl — cancellation
```

`{user}` carries the caller's identity for attribution (see "User attribution"
below); it is `_` when a deployment does not use per-user auth.

Frames: `msg` carries one JSON-RPC notification; `end` carries the response
(empty body = cancelled, no response exists); `err` carries a
gateway-synthesized error; `ka` is a 15s keepalive so idle streams are
distinguishable from dead ones. A broken stream is reported as error `-32010`
and the client re-issues — exactly what the 2026-07-28 spec mandates.

## Large results (claim-check)

Every message is bounded by the NATS server's `max_payload`, which the
gateway learns from the connection handshake (`nc.MaxPayload()`) — there is
no size config to keep in sync. A response that doesn't fit normally fails
with a legible `-32012`. With **claim-check** enabled, the gateway instead
parks the oversize body in a JetStream Object Store and sends only a
reference (`Mcp-Claim` header on an empty `end` frame); the client fetches,
digest-verifies, and deletes it before the MCP client sees anything:

```
gateway: body > max_payload → put MCP_CLAIMS_{tenant}/{random-id} → end + Mcp-Claim: id
client:  end + Mcp-Claim     → fetch (SHA-256 verified) → delete → deliver inline
```

Both ends opt in — the feature is **off by default** and requires JetStream:

```
natsmcp gateway --config gw.json          # file mode: add "claimCheck": {"maxAge": "5m"}
natsmcp gateway --config-subject … --claim-check   # fetch mode
natsmcp shim --server aws --accept-claims          # client side (also: natsmcp call)
```

The gateway only claims for requests carrying `Mcp-Accept-Claim: 1` (a
claimed frame's empty body would read as "cancelled" to an unaware client),
so mixed fleets are safe. One bucket per tenant (`MCP_CLAIMS_{tenant}`)
keeps the fencing in NATS permissions, same as request subjects — the
callout grants clients read on their own bucket:

```
allow: ["$O.MCP_CLAIMS_acme.>"]   # object-store subjects for tenant acme
```

Objects live until fetched (client deletes eagerly) or until the bucket TTL
(`maxAge`, default 5m) reaps them — a client that dies mid-fetch can
re-issue and still succeed. `maxBytes` (default 1GiB) caps each tenant's
bucket; single objects cap at 64MiB. Every failure degrades to the status
quo: store unreachable at put time → `-32012`; fetch failure → `-32010`,
re-issue. Within a tenant, claim secrecy rests on unguessable random ids and
the privacy of reply inboxes. Cost: a claimed result takes roughly one extra
disk round-trip on the JetStream node (~300ms for 8MB) — keep `max_payload`
at 8MB so claims stay the rare tail, not the steady path.

## The authorization plane is NATS

The gateway contains **no authorization code**. Subject permissions decide
everything:

```
# this user may list tools and call exactly one tool on one server
# ("_" is the user token — see User attribution)
publish: [
  "mcp.v1.req.acme._.github.tools.list._",
  "mcp.v1.req.acme._.github.tools.call.get_issue",
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

## User attribution

The `{user}` subject token records *who* made each request. It is trustworthy
for the same reason `{tenant}` is: core NATS never tells the gateway who
published a message, but it *does* enforce which subjects a connection may
publish to. So attribution rides the subject and is verified by NATS, with no
crypto in the gateway.

The intended setup is **NATS auth callout**: your callout service authenticates
the caller and mints a NATS user JWT scoped to `mcp.v1.req.{tenant}.{user}.>`.
The caller then can't publish under anyone else's `{user}` token, and the
gateway trusts the token it parses — logging it on every request and, because
the scoping is per-user, letting you write per-user tool grants for free:

```
# callout mints, for user u_9f3a in tenant acme:
publish: [
  "mcp.v1.req.acme.u_9f3a.>",          # everything as this user, or finer:
  "mcp.v1.req.acme.u_9f3a.github.tools.call.get_issue",
  "_INBOX_acme_u_9f3a.>"
]
```

Clients pass their user token via `--user` / `NATSMCP_USER` (shim, call). The
user id must be subject-token-safe (`A-Za-z0-9_-`), so map emails/etc. to a
stable handle or UUID in the callout. Deployments without per-user auth use the
`_` placeholder ("unattributed") and the gateway logs `user=_`.

Attribution is isolation only when credentials are per-user: with a static
server the pool key stays `(server, tenant)` and processes are shared per
tenant regardless of user; a server with a per-user `auth` mode (below) keys
its backends per `(server, tenant, user, credential-generation)`, so users
never share a process or a credential.

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

## Per-user backend credentials

By default every server runs with **one shared credential** from its config
`env`/`headers`. For curated third-party servers (AWS, Grafana, GitHub…)
callers should not share an identity: a per-server `auth` block selects how
credentials are **resolved per (tenant, user, server)** and injected — as
`headers` on every HTTP request or as `env` on the spawned subprocess, merged
OVER the config's static values (config keeps non-secrets like `AWS_REGION`;
resolved credentials win on collision). The end user never sees the
credential, and per-user modes keep secrets out of the config entirely.

```json
"grafana": { "transport": "http", "url": "https://grafana.internal/mcp",
             "auth": { "mode": "nats" } },
"jira":    { "transport": "http", "url": "https://jira.internal/mcp",
             "auth": { "mode": "oauth-token-exchange",
                       "tokenUrl": "https://idp/token", "audience": "jira",
                       "clientId": "${OAUTH_CLIENT_ID}", "clientSecret": "${OAUTH_CLIENT_SECRET}",
                       "subjectTokenFile": "/var/run/assertions/{user}.jwt" } },
"aws":     { "transport": "stdio", "command": "uvx", "args": ["awslabs.aws-api-mcp-server"],
             "auth": { "mode": "exec", "command": "aws-cred-helper" } }
```

| mode | grain | resolves via |
|---|---|---|
| `static` (default) | shared | config `env`/`headers` — today's behavior |
| `file` | shared, or per-user when `path` contains `{user}` | a mounted/rotated file (`path`; optional `ttl`, default 1m, applied when the file carries no `expiresAt`) |
| `oauth-client-credentials` | shared | RFC 6749 client_credentials (`tokenUrl`, `clientId`, `clientSecret`; optional `scope`, `audience`) |
| `exec` | per-user | a credential-helper command (`command`; optional `args`, `env` passthrough) — the universal adapter, below |
| `oauth-token-exchange` | per-user | RFC 8693 (`tokenUrl`, `subjectTokenFile`; optional `clientId`, `clientSecret`, `scope`, `audience`, `subjectTokenType` — default `urn:ietf:params:oauth:token-type:access_token`) |
| `oauth-refresh` | per-user | refresh_token grant (`tokenUrl`, `clientId`, `refreshTokenFile`; a rotated refresh token is written back) |
| `nats` | per-user | request/reply to a controller (below; the subject prefix defaults to `{subjectPrefix}.cred` and `subject` overrides it) |

The file-path fields (`path`, `subjectTokenFile`, `refreshTokenFile`) accept
`{tenant}`, `{user}`, `{server}` placeholders, and `file` reads the same
`{"headers"|"env": {...}, "expiresAt": "RFC3339"}` JSON the `exec` helper
prints.

Per-user servers are pooled per `(server, tenant, user, credential
generation)`: two users of one server get separate processes with their own
credentials, a refreshed credential drains the old backend, and the pool
recycles a backend before its credentials expire (an in-flight call at expiry
fails with `-32010` and the client re-issues) — the expiry bounds the process
only when the resolver actually returned `env`/`headers`, so a reply carrying
nothing but an `expiresAt` costs no respawn. HTTP servers get the current
bearer on every request, so a mid-life refresh needs no reconnect; on a `401`
the gateway drops the cached credential, re-resolves, and retries exactly
once — safe for any method, since a `401` rejects the request before the
server executes it. Credential-resolution failures surface as **`-32014`**;
the message says whether to retry (resolver unreachable) or not (the source
refused), and failures are memoized with a short backoff (1s doubling to
30s), so a resolver outage degrades into fast, legible errors instead of
hammering the source at request rate. Mind `pool.maxProcsPerTenant`
(default 16): per-user stdio servers count each `(user, server)` process
against it, so size it to roughly users × stdio servers per tenant.

Adding a credential source has the same three tiers as config reloading
(`pkg/backend/cred`), cheapest first:

1. **Use a built-in** — the modes above, RFC 8693 included.
2. **Wrap a script with `exec`** — the helper runs with a scrubbed
   environment plus `NATSMCP_CRED_{TENANT,USER,SERVER}` and prints
   `{"headers"|"env": {...}, "expiresAt": "RFC3339"}`. Any credential system
   integrates in ~20 lines; e.g. AWS STS with no SDK in the gateway:
   ```sh
   #!/bin/sh
   aws sts assume-role-with-web-identity \
     --role-arn "arn:aws:iam::123456789:role/mcp-${NATSMCP_CRED_USER}" \
     --role-session-name "$NATSMCP_CRED_USER" \
     --web-identity-token "$(cat /var/run/assertions/$NATSMCP_CRED_USER.jwt)" \
     --query 'Credentials' --output json |
   jq '{env: {AWS_ACCESS_KEY_ID: .AccessKeyId, AWS_SECRET_ACCESS_KEY: .SecretAccessKey,
              AWS_SESSION_TOKEN: .SessionToken}, expiresAt: .Expiration}'
   ```
3. **Implement `cred.Resolver`** — one method; wrap it in `cred.Cached` for
   the TTL cache, single-flight, and failure backoff every mode gets.

**Controller contract (`mode: "nats"`):** respond to
`{subjectPrefix}.cred.{tenant}.{user}.{server}` — `mcp.v1.cred.…` by default,
and it follows a custom `--subject-prefix` so one configured prefix governs
both the wire and the cred exchange (`auth.subject` overrides it) — with the same
`{"headers"|"env", "expiresAt"}` JSON — how the controller produced it
(authorization-code + refresh, STS, RFC 8693 token exchange) is invisible to
the gateway and can evolve freely. Refuse with a `Nats-Service-Error` header;
refusals are authoritative and not retried beyond a backoff. NATS permissions
fence the subject to controller ↔ gateway. The gateway always asks with the
NATS-verified caller identity, so it cannot be tricked into fetching another
user's credential. Revocation rides expiry: keep `expiresAt` short (~1h);
there is no push-revoke.

## Local and remote servers

The gateway runs MCP servers in two shapes; the MCP server itself is vanilla
and gateway-unaware in both:

- **Local (in-process):** the shapes above — stdio subprocesses spawned by
  the gateway, HTTP endpoints called by it. Right for single-binary
  deployments and for hosted HTTP servers, which take per-user credentials as
  request headers.
- **Remote scoped instances:** stdio-only servers can't be networked, so to
  run one *outside* the central gateway (its own pod, with its own resources),
  run this same binary in front of it as the container entrypoint, **scoped**
  to a slice of the subject space. The `nats.tenant`/`nats.user` config fields
  (file source) or `--scope-tenant`/`--scope-user` / their env (fetch and
  inline sources) select one of three scopes:

  | scope | binds | serves | use |
  |-------|-------|--------|-----|
  | *unset* | `mcp.v1.req.*.*.{server}.>` | all callers | the central fleet |
  | tenant only | `mcp.v1.req.{tenant}.*.{server}.>` | all of one tenant's users | a per-org deployment |
  | tenant + user | `mcp.v1.req.{tenant}.{user}.{server}.>` | one caller | a per-user pod |

  Setting `user` without `tenant` is invalid — a user token spans every
  tenant. The example below is a per-user pod:

  ```json
  { "nats": { "url": "nats://nats:4222", "tenant": "acme", "user": "u_9f3a" },
    "servers": { "aws": { "transport": "stdio", "command": "uvx",
                           "args": ["awslabs.aws-api-mcp-server"],
                           "env": { "AWS_ACCESS_KEY_ID": "${AWS_ACCESS_KEY_ID}",
                                    "AWS_SECRET_ACCESS_KEY": "${AWS_SECRET_ACCESS_KEY}",
                                    "AWS_SESSION_TOKEN": "${AWS_SESSION_TOKEN}" } } } }
  ```

  It binds `mcp.v1.req.acme.u_9f3a.{server}.>`, so NATS routes exactly that
  user's traffic to the pod — no inbound Service, DNS, or ingress; the pod
  only dials out. Whatever creates the pod injects the user's credentials into
  its environment, and the pod's lifetime bounds the credential's. Dropping
  the `user` field makes it a per-org deployment that fronts the same servers
  for the whole tenant with one shared set of credentials.

  Three rules hold for any scoped instance:

  1. A given server name must be served by **either** the central gateway
     **or** scoped instances, never both — they would compete for the
     overlapping subjects. Scoped instances therefore default to their own
     queue group, `mcpgw.{tenant}` or `mcpgw.{tenant}.{user}`.
  2. For the same reason, **do not run a tenant-scoped instance and a per-user
     instance of the same tenant over the same server name.**
     `{tenant}.*` and `{tenant}.{user}` overlap, and because their default
     queue groups differ, NATS delivers that user's request to *both* — the
     call executes twice against two backends, and only the first reply is
     kept. Pick one granularity per server name.
  3. The pod's NATS identity should be fenced to its own subjects — **including
     its inbox**. A scoped instance should set `--inbox-prefix` /
     `NATSMCP_INBOX_PREFIX` (file source: `nats.inboxPrefix`), which moves every
     request/reply the process issues — the config fetch, the `nats` cred
     resolver, JetStream — under that prefix:

     ```
     # instead of subscribe: ["_INBOX.>"] — the whole account's replies
     publish:   ["mcp.v1.req.acme.u_9f3a.>", "mcp.v1.cfg.request", "mcp.v1.cred.acme.u_9f3a.>"]
     subscribe: ["_INBOX_acme_u_9f3a.>"]
     ```

     Without it the identity needs `_INBOX.>` just to receive its own replies,
     which grants it the entire account-wide reply namespace. That matters most
     for a per-org pod running **third-party MCP server code** beside the
     gateway: a child process that lifts the connection's credential could
     subscribe there and read every other tenant's credential-responder replies.
     The value must be a literal subject prefix — no spaces, no `*`/`>`, no
     trailing dot — and is rejected at boot otherwise.

  The document above is the file-source form. A pod with no file mount carries
  it inline instead — `NATSMCP_CONFIG_JSON` holds a plain-only
  `{"servers":…}` document (a `nats` block there is rejected, so scope can
  never be silently dropped), while the scope and the token-bearing NATS URL
  arrive as env (see [Config sources](#config-sources--hot-reload)).

## Docker image

Releases publish a multi-arch (amd64/arm64) image: a ~20MB static binary on
`gcr.io/distroless/static-debian12:nonroot` — no shell, no package manager,
CA certificates and a writable `/tmp` included (the stdio backend's workdir).
`make image` builds a local single-arch `natsmcp:develop`.

```
docker run --rm -v ./gateway.json:/etc/natsmcp.json:ro \
  ghcr.io/code-cargo/natsmcp:latest gateway --config /etc/natsmcp.json
```

The same image serves as the central gateway, a scoped per-user pod
entrypoint, and the shim/call CLIs. Curated MCP server images (which need
their own Python/Node runtime) should **copy the binary out** rather than
build `FROM` it:

```dockerfile
COPY --from=ghcr.io/code-cargo/natsmcp:v1.2.3 /usr/local/bin/natsmcp /usr/local/bin/natsmcp
ENTRYPOINT ["/usr/local/bin/natsmcp", "gateway", "--config", "/etc/natsmcp.json"]
```

A gateway upgrade is then one tag bump per curated image, and the image's
provenance is attested per release
(`gh attestation verify oci://ghcr.io/code-cargo/natsmcp:vX --repo code-cargo/nats-mcp-gateway`).

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
natsmcp gateway --config /etc/natsmcp.json           # file; SIGHUP or poll reloads
natsmcp gateway --config-subject mcp.v1.cfg.request  # fetch over NATS request/reply
NATSMCP_CONFIG_JSON='{"servers":{…}}' natsmcp gateway # inline; fixed for the process life
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
- **Inline** (`--config-json` / `NATSMCP_CONFIG_JSON`): the whole config
  document as a string, applied once and never reloaded — for pods with no file
  mount and no config responder (the scoped stdio deployment injects its one
  server this way). Connection and scope come from the fetch-source flags/env,
  not the document (which is plain-only, so the NATS URL — not the config —
  carries the credential); a config change is a new pod, not a hot reload.

Note: only the **file** source reads connection settings (URL, prefix, queue
group, inbox prefix, scope) from the document's `nats` block. The fetch and
inline sources take those from flags/env — the document supplies only the
server set.

`--inbox-prefix` / `NATSMCP_INBOX_PREFIX` (file source: `nats.inboxPrefix`)
sets the prefix for this process's own request/reply inboxes — the config
fetch, the `nats` cred resolver, and JetStream all derive their reply subject
from the connection, so one setting covers them. Scoped instances **should**
set it: without one, the gateway replies land under `_INBOX.>` and its NATS
identity has to be granted that whole namespace (see
[Local and remote servers](#local-and-remote-servers)).

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
- Payloads are bounded by NATS `max_payload` unless claim-check is enabled
  (see "Large results"), and even then single results cap at 64MiB.
  Request-direction bodies (large uploads) are always bounded by
  `max_payload` — claim-check covers responses only.
- `resourceSubscriptions` inside `subscriptions/listen` is not yet bridged
  for legacy backends (`*/list_changed` events are).
- Credential-expired backends are recycled on their next use and by the
  reaper, but only when idle: a long-lived in-flight call (a
  `subscriptions/listen` stream) can pin its process past the credentials'
  expiry until it completes.

## Development

`make ci` = format-check + staticcheck + build + tests. Tests run anywhere Go
does (embedded NATS server; the fake MCP server is the re-exec'd test
binary). `go test ./... -race` is clean.
