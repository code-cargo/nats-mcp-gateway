# nats-mcp-gateway

Front MCP servers over a NATS fabric. stdio servers that were never meant to
leave a laptop become network services; NATS accounts and subject permissions
become the MCP authorization plane; the gateway injects backend credentials so
end users never hold them.

> **Status: under active development.** This is not yet a stable release.
> The wire (subjects, framing, headers) is versioned `Mcp-Wire: 1` but may
> still change incompatibly, and MCP 2026-07-28 — the revision the wire
> carries — was itself only finalized on 2026-07-28. Pin a commit, expect
> breaking changes, and read [Upgrading](#upgrading) and the git log before
> moving to a newer one.

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
reference (`Mcp-Claim` header on an empty `end` frame); the client fetches
and digest-verifies it before the MCP client sees anything:

```
gateway: body > max_payload → put MCP_CLAIMS_{tenant}/{random-id} → end + Mcp-Claim: id
client:  end + Mcp-Claim     → fetch (SHA-256 verified) → best-effort delete → inline
```

Both ends opt in — the feature is **off by default** and requires JetStream:

```
natsmcp gateway --config gw.json          # file mode: add "claimCheck": {"maxAge": "5m"}
natsmcp gateway --config-subject … --claim-check   # fetch mode
natsmcp shim --server aws --accept-claims          # client side (also: natsmcp call)
```

The gateway only claims for requests carrying `Mcp-Accept-Claim: 1` (a
claimed frame's empty body would read as "cancelled" to an unaware client),
so mixed fleets are safe. One bucket per tenant (`MCP_CLAIMS_{tenant}`,
backed by the stream `OBJ_MCP_CLAIMS_{tenant}`) keeps the fencing in NATS
permissions, same as request subjects. An object store is *read* through the
JetStream API, so what the callout mints for a client is four publish grants
there — nothing under `$O`:

```
# client (shim, call) for tenant acme — read-only
publish: [
  "$JS.API.STREAM.INFO.OBJ_MCP_CLAIMS_acme",        # bind the bucket
  "$JS.API.DIRECT.GET.OBJ_MCP_CLAIMS_acme.>",       # the object's metadata
  "$JS.API.CONSUMER.CREATE.OBJ_MCP_CLAIMS_acme.>",  # read its chunks
  "$JS.API.CONSUMER.DELETE.OBJ_MCP_CLAIMS_acme.>"   # drop that consumer after
]
```

`$O.MCP_CLAIMS_acme.>` is the opposite of that grant, despite reading like
it: those subjects are the bucket stream's *ingest*, so it is write access
that can read nothing. A client holding it fails its first fetch on
`$JS.API.STREAM.INFO.…` — and can meanwhile publish a rollup metadata
message over `$O.MCP_CLAIMS_acme.M.{base64url(claim-id)}`, a subject
derivable from a claim id alone. That substitution is SILENT rather than a
digest mismatch: a rollup declaring size 0 makes the victim's fetch return an
empty body and no error at all, because the read ends before the digest is
ever compared. It can also fill that tenant's bucket to `maxBytes` with chunk
traffic nobody asked for.

The gateway's own identity needs the write side. `$JS.API.>` would cover it
and grants far too much — every other tenant's bucket, and `STREAM.DELETE`
on anything in the account. Per bucket it needs exactly:

```
# gateway for tenant acme
publish: [
  "$JS.API.STREAM.CREATE.OBJ_MCP_CLAIMS_acme",   # the bucket is made on first claim
  "$JS.API.STREAM.UPDATE.OBJ_MCP_CLAIMS_acme",
  "$JS.API.STREAM.INFO.OBJ_MCP_CLAIMS_acme",
  "$JS.API.STREAM.PURGE.OBJ_MCP_CLAIMS_acme",
  "$JS.API.DIRECT.GET.OBJ_MCP_CLAIMS_acme.>",
  "$O.MCP_CLAIMS_acme.>"                         # chunks and metadata
]
```

The two blocks are disjoint, and neither is a superset of the other: the
gateway cannot READ a claim it wrote, since reading chunks means running a
consumer and only the client grant may create one. That is the intended split
— the gateway only ever puts and deletes.

Both blocks are per tenant and there is no shortening them: NATS wildcards
match whole tokens, so `OBJ_MCP_CLAIMS_*` is a literal token that matches no
bucket at all. A scoped instance has one tenant and this is exact; a central
fleet generates the blocks from the tenant list the callout already keeps.

The client's eager delete is what a read-only grant gives up: deleting needs
a metadata write plus `$JS.API.STREAM.PURGE.…`, and that purge would equally
let any client wipe the tenant's live claims. So the delete is best-effort
by design — objects live until the bucket TTL (`maxAge`, default 5m) reaps
them, which is the same reason a client that dies mid-fetch can re-issue and
still succeed. The fetch itself still returns the body; the client's NATS
library notes the refused delete on stderr, once, which is expected under a
read-only grant rather than a fault. A refused publish is reported to that
handler without ending the request that provoked it, so the delete is bounded
separately from the fetch and stops being attempted after the first refusal —
otherwise every claimed response would wait out the full store timeout with
the body already in hand. Keep `maxAge` short
where you leave the delete ungranted, and size `maxBytes` (default 1GiB, per
tenant) for a whole TTL window of claims; single objects cap at 64MiB. Every
failure degrades to the status quo: store unreachable at put time →
`-32012`; fetch failure → `-32010`, re-issue.
Within a tenant, claim secrecy rests on unguessable random ids and the
privacy of reply inboxes. Cost: a claimed result takes roughly one extra
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

`_INBOX.>` is written here for brevity and is **account-wide in both
directions**: it lets a caller subscribe to every other caller's reply inbox,
and publish to one. Under it a co-tenant reads other users' tool results and
resource contents, and can publish to the `{reply}.ctl` subject a request
listens on. The gateway refuses a control message that is not a
`notifications/cancelled` naming that request, which stops a stray or
misrouted publish from ending a live call — but it cannot stop a caller who
can already read the reply stream from reproducing the id.

Per-user inbox prefixes are what isolate clients from one another. The
auth-callout recipe below mints `_INBOX_{tenant}_{user}.>`, which closes both
directions at the NATS layer, and is the same reasoning the gateway applies to
its own identity with `--inbox-prefix`.

Two things make that real:

1. **The integrity check** (`pkg/proxy/integrity.go`): the gateway proves the
   subject NATS authorized, the mirrored headers, and the JSON-RPC body all
   agree, and rejects any disagreement with `-32020`. Without it a caller
   could publish a `delete_repo` body to a `get_issue` subject. The body is
   forwarded verbatim, so it is read the way the backend will read it: exact
   keys, and no duplicate key at any depth. Keys differing only by case are
   refused across `params`, whose field names the spec fixes — and, inside
   the open extension bags `_meta` and tool `arguments`, on the specific key
   being read rather than the whole object, since a third party's two keys
   there are none of the gateway's business (`pkg/mcpspec/params.go`). A
   check that parses the body differently from the server executing it has
   verified nothing.
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

That grain only holds if `{user}` is an identity, so a per-user `auth` mode
**refuses the `_` placeholder** with `-32014` rather than resolve a credential
for it. Without the refusal, a deployment that granted `mcp.v1.req.{tenant}.>`
instead of `…{tenant}.{user}.>` would quietly collapse every unattributed
caller onto one credential and one process, on exactly the servers configured
so that must not happen. Per-user `auth` modes therefore require per-user NATS
auth; shared and static servers are unaffected and keep serving `_`.

Which modes are per-user is a property of the credential, not of the mode
name. `file`, `oauth-token-exchange` and `oauth-refresh` read their grain from
the path: one containing `{user}` resolves per caller, a fixed one — a
projected service-account token, a single rotated refresh token — is a shared
credential and is treated as one. `exec` and `nats` cannot be read that way,
since the helper or controller decides what the arguments mean, so they are
per-user unless the server says otherwise:

```json
"auth": { "mode": "exec", "command": "/usr/local/bin/creds", "perUser": false }
```

Use it when the helper returns one credential for the whole tenant — it is
handed `NATSMCP_CRED_TENANT` and `NATSMCP_CRED_SERVER` and may ignore the
user. Without it, a deployment whose callers all send `_` gets `-32014` on
every request to that server. The gateway names each per-user server at boot
so this is legible before the first call rather than after it.

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

`protocol` defaults to `2025-11-25` (that is what exists in the wild). Set
`max_payload: 8MB` on the NATS server: MCP results carry base64 blobs, and
oversize messages fail with a legible `-32012` instead of a hang.

`${VAR}` expands from the gateway's environment — the credential-injection
point; clients never see backend secrets. It applies to every string **value**
in a document the operator authored (the file and inline sources; a *fetched*
document is not expanded, see [Config
sources](#config-sources--hot-reload)), and never to a key. Three rules keep a
credential from being quietly mangled on its way to a backend:

- An **undefined variable fails the load**, naming the field and the variable.
  A typo'd `${GITHUB_TOKN}` that defaulted to empty would run the server with no
  credential and surface hours later as an untraceable 401.
- `${VAR}` is the **only** reference form, so a bare `$` is literal — `s$cret`
  is the password you wrote, not `s`.
- `$$` is a literal `$`, which is how a value containing `${` is written:
  `$${TEMPLATE}` reaches the backend as `${TEMPLATE}`.

> **Upgrading:** earlier builds expanded a bare `$VAR` as well, and no longer
> do. That form is now the literal text `$VAR`, and — unlike an undefined
> `${VAR}` — it does **not** fail the load, so a config relying on it ships
> `$GITHUB_TOKEN` to the backend as a credential and comes back as a 401 with
> nothing in the gateway's logs to explain it. The reverse case was quieter
> still: the old form took the longest name it could, so `$HOME_ISH/x`
> resolved to `/x` — a path with its prefix deleted, nothing named
> `HOME_ISH` — where it now stays `$HOME_ISH/x`. Braces are not optional: grep
> your configs for `$` not followed by `{` before upgrading. The bare form
> stays literal on purpose — a `$` belongs to passwords (`s$cret`) and to
> arguments meant for the backend's own shell (`--fmt=$HOME`), and eating
> those would corrupt them just as silently in the other direction.

A server's `url` and its `auth.tokenUrl` must be `https`, because both carry
credentials — the injected `Authorization` header on every call, and the
client secret / subject token / refresh token respectively. Loopback
(`127.0.0.0/8`, `::1`, `localhost`) is exempt, so local MCP servers need no
ceremony. Set `"allowPlaintext": true` on a server when something outside the
gateway's view encrypts the hop — a service mesh sidecar, a tunnel — and it
covers that one server's `url` and `tokenUrl` only.

> **Upgrading:** this rejects the WHOLE config, not the offending server, and
> the fetch source revalidates on every reload — so one un-migrated `http://`
> url takes down every other server on that gateway. Fix the config before
> rolling the binary: add `allowPlaintext` where the hop is genuinely
> encrypted elsewhere, `https` everywhere else. On the **fetch and inline**
> sources an older gateway ignores `allowPlaintext`, so there the config
> change is safe to land first and safe to sit under a rollback. The **file**
> source parses strictly, and an older binary meeting the new key fails with
> `unknown field "allowPlaintext"` and crash-loops — so on a file-source
> gateway the config edit and the binary have to move together, and back
> together.

Redirects are refused unless the hop changes nothing but the path. Go's
default policy is written for a browser: it keeps `Authorization` across an
`https`→`http` downgrade and across a hop to a *subdomain*, never strips the
request body (which for the OAuth client is the refresh token), and hands the
final response back to the caller. A gateway holding someone else's
credential cannot follow those rules, so a hop to a different host, a
different port, or out of `https` fails the request instead. If a backend
sits behind something that redirects to a canonical host, point `url` at
where it lands.

### Caching hints

2026-07-28 requires `ttlMs` and `cacheScope` on every cacheable result
(`server/discover`, the `*/list` methods, `resources/read`). For a legacy
backend the gateway supplies both, since a 2025-11-25 server cannot:

```json
"github": { "discoverTtlMs": 300000, "cacheScope": "private" }
```

`cacheScope` defaults to **`private`** and you should usually leave it there.
Backends are pooled per `(server, tenant[, user, credential-generation])` and
credentials are injected per caller, so a result generally is not safe for a
shared cache to hand to a different authorization context — which is exactly
what `public` licenses. Set `public` only for a server whose listings are
provably identical for every caller. An unrecognized value is rejected at
config load rather than defaulted, so a typo cannot quietly widen it.

### Tool parameters mirrored into headers

A backend MAY annotate tool parameters with `x-mcp-header`, asking that their
values also travel as `Mcp-Param-{Name}` headers. The gateway honors this for
HTTP backends without giving up being schema-blind: it learns annotations from
`tools/list` responses passing through, and if a `tools/call` is rejected with
`-32020` before it has seen one, it re-reads the schema and retries that call
once — the recovery the spec defines for exactly this case. A tool whose
annotations are invalid is dropped from `tools/list` with a warning, so one
malformed definition cannot cost a server its whole toolset.

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
| `exec` | per-user, or shared with `"perUser": false` | a credential-helper command (`command`; optional `args`, `env` passthrough, `waitDelay`) — the universal adapter, below |
| `oauth-token-exchange` | shared, or per-user when `subjectTokenFile` contains `{user}` | RFC 8693 (`tokenUrl`, `subjectTokenFile`; optional `clientId`, `clientSecret`, `scope`, `audience`, `subjectTokenType` — default `urn:ietf:params:oauth:token-type:access_token`) |
| `oauth-refresh` | shared, or per-user when `refreshTokenFile` contains `{user}` | refresh_token grant (`tokenUrl`, `clientId`, `refreshTokenFile`; a rotated refresh token is written back) |
| `nats` | per-user, or shared with `"perUser": false` | request/reply to a controller (below; the subject prefix defaults to `{subjectPrefix}.cred` and `subject` overrides it) |

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
refused). The unattributed-caller refusal above uses the same code — as does
the pool factory's copy of it, which catches a request keyed before a reload
made its server per-user and is the one form of it worth re-issuing. Failures
are memoized with a short backoff (1s doubling to 30s), so a resolver outage
degrades into fast, legible errors instead of
hammering the source at request rate. Mind `pool.maxProcsPerTenant`
(default 16): per-user stdio servers count each `(user, server)` process
against it, so size it to roughly users × stdio servers per tenant. A
tenant-scoped deployment fronts a whole org through one pool, so it is the
shape most likely to need raising. Pool limits are boot-fixed: the file and
inline sources read them from the document's `pool` block, and the fetch
source — whose config arrives after the pool is built — takes them from
`--pool-max-procs-per-tenant`, `--pool-max-concurrent`, `--pool-idle-ttl`
and `--pool-max-lifetime` (or their `NATSMCP_POOL_*` env vars).

Adding a credential source has the same three tiers as config reloading
(`pkg/backend/cred`), cheapest first:

1. **Use a built-in** — the modes above, RFC 8693 included.
2. **Wrap a script with `exec`** — the helper runs with a scrubbed
   environment (`PATH`, `auth.env`, and a private empty `HOME` discarded when
   the run ends) plus `NATSMCP_CRED_{TENANT,USER,SERVER}`, and prints
   `{"headers"|"env": {...}, "expiresAt": "RFC3339"}`. A helper needing a
   populated home — an `aws` or `gcloud` wrapper reading its own config —
   names `HOME` in `auth.env`, which overrides it. Any credential system
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
prefix, queue group, and scope are fixed at boot; only servers reload. Editing
the file's `nats` block and reloading logs a warning naming the fields that
moved — the running connection keeps the boot values until you restart.)
Reloads are safe: a malformed revision is rejected and the last good config
keeps serving, a removed server stops answering (clients get `-32011` and
re-issue), and a changed server's pooled processes are evicted so the next call
spawns from the new definition. In-flight calls on a removed/changed server
fail retryably (`-32010`); unchanged servers are never disturbed.

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
  connection — nothing at rest in a ConfigMap or KV bucket. A fetched document
  is therefore **not** `${VAR}`-expanded, and a reference in one is rejected:
  the controller resolves credentials itself, and expanding here would make the
  gateway pod's own environment a second secret source that whoever answers the
  subject could read back out through a backend argument, `env` entry, or URL.
  **Controller contract:** respond to the request subject with the same config
  JSON the file source parses (with values resolved, not `${VAR}` references),
  and publish any message to the events subject on change; NATS permissions
  fence both subjects to the controller. The reply must carry **no `nats`
  block** — the connection it arrives on is already open, so nothing in one can
  be acted on, and a controller that emits `{"nats":{"tenant":"acme"},…}`
  believing it scoped the fleet would leave every pod serving every tenant. A
  reply carrying one is refused like any other unusable revision, which means
  it depends on whether the pod is already serving: a running gateway keeps its
  last good config and converges once the controller stops sending the block,
  while a pod that has never served retries for the two-minute boot window and
  then exits — so a controller emitting one from the start fails the rollout
  rather than serving the wrong scope.
- **Inline** (`--config-json` / `NATSMCP_CONFIG_JSON`): the whole config
  document as a string, applied once and never reloaded — for pods with no file
  mount and no config responder (the scoped stdio deployment injects its one
  server this way). Connection and scope come from the fetch-source flags/env,
  not the document (which is plain-only, so the NATS URL — not the config —
  carries the credential); a config change is a new pod, not a hot reload.

Note: only the **file** source reads connection settings (URL, prefix, queue
group, inbox prefix, scope) from the document's `nats` block. The fetch and
inline sources take those from flags/env — the document supplies only the
server set. Passing one of those flags (or its `NATSMCP_*` env var) alongside
`--config` is **rejected at boot** unless it agrees with what the document puts
in force — the mirror of the inline source rejecting a `nats` block. A scope
injected as pod env is therefore never silently dropped, in either direction.

Agreement is judged against the *effective* value, not the raw field, so
pinning a flag to a default the document leaves unstated (the `mcpgw.{tenant}`
queue group, the pool's `32`/`16`/`5m`/`1h`) changes nothing and boots.

A flag still sitting at its **own** default is likewise not a conflict, because
it says nothing about what the operator wanted. Only `--nats-url`,
`--subject-prefix`, `--claim-max-age` and `--claim-max-bytes` have defaults, so
only they can be exempt this way — and it matters for the first two, whose
`NATSMCP_NATS_URL` and `NATSMCP_SUBJECT_PREFIX` are shared with `shim` and
`call`: exporting the stock value once per deployment must not break a gateway
it was never aimed at.

Every other setting is checked whenever it is non-empty, which is the whole
point — scope, credentials, queue group and inbox prefix are what isolation
rests on. `NATSMCP_INBOX_PREFIX` is shared with `shim` too but has no default,
so any value it carries is taken as deliberate: a deployment exporting it
fleet-wide **must** also set `nats.inboxPrefix` in the document, or that
gateway will refuse to boot rather than run with the identity fencing silently
off. Unset it for the gateway, or state it in the document — both are a
one-line fix, and the error names the value it expected.

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
   suppress no-op emissions. A source that suppresses them itself should also
   implement `configsource.Retryable`: `Run` calls it when an apply fails, and
   a source that ignores it will mistake the failed revision for one the
   gateway is already running and swallow every redelivery of it. If the
   source's trigger does not repeat on its own, `Retry` must also re-fire it —
   the apply runs on `Run`'s goroutine, so a trigger can arrive and be
   suppressed before `Retry` is ever called. `Watch` each source value once:
   the baseline lives on the source, which is what makes `Retry` possible.

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

## Upgrading

Changes that alter a working deployment's behavior rather than adding to it.
Two more are called out where they are configured: the [`${VAR}` reference
form](#configuration) and [`allowPlaintext`](#configuration).

- **`${VAR}` reaches string values only.** Expansion runs on the decoded
  document now, not on its text, so a reference outside a JSON string no
  longer works by any spelling: unquoted (`"maxConcurrent": ${POOL_MAX}`)
  fails the parse, quoted (`"maxConcurrent": "${POOL_MAX}"`) fails the type
  check. That is every non-string field — `pool.maxConcurrent`,
  `pool.maxProcsPerTenant`, `claimCheck.maxBytes`, `discoverTtlMs`,
  `allowPlaintext`. For the first four the unquoted form used to work,
  because substitution happened before the parse saw it; `allowPlaintext` is
  new and never took one. The duration and size fields that are strings
  (`pool.idleTtl`, `pool.maxLifetime`, `claimCheck.maxAge`) are unaffected.
- **`${VAR}` in a key is rejected**, naming the key. It used to expand, so a
  templated server name (`"servers": {"${POD_SERVER}": …}` — plausible for
  the per-user pod shape in [Local and remote
  servers](#local-and-remote-servers)) or a templated `env`/`headers` key was
  expressible and is not now. Like any parse failure this rejects the whole
  document: the file source crash-loops the pod, the fetch source freezes the
  fleet on its last good config.
- **`$$` in a value changed meaning, silently.** It is now the escape for one
  literal `$`; it used to be read as the shell's `$$` variable and replaced
  with nothing. So `a$$b` was `ab` and is now `a$b`, and a password written
  `pa$$word` was `paword` and is now `pa$word` — neither of which is the
  value on the page. Write `$$$$` for two literal dollars. Nothing errors
  either way; the backend just receives a different secret.
- **The `exec` credential helper gets a private, empty `HOME`** — a scratch
  directory made per resolve and removed after — in place of the gateway's.
  A helper reading `~/.aws/config`, `~/.config/gcloud` or
  `~/.docker/config.json` now finds an empty directory and fails, surfacing
  as a credential-resolution failure on every call to that server, with the
  helper's own stderr in the message. Naming `HOME` in that server's
  `auth.env` overrides it, which is what a wrapper around `aws` or `gcloud`
  wants.
- **`exec` resolution needs a writable temp directory on every resolve**,
  because that scratch `HOME` is created under `TMPDIR` (`/tmp` when unset).
  stdio backends already made their workdir the same way, but a pod with a
  read-only root filesystem fronting only HTTP backends never touched one.
  Mount an `emptyDir`, or point `TMPDIR` at one.
- **A helper's stdout must close within 3s of the helper itself exiting.**
  Anything it left running that inherited that pipe — a daemon it starts on
  demand, an agent — holds it open, and reaching the delay fails the resolve
  and SIGKILLs the helper's process group rather than waiting out `Timeout`.
  The bound is fixed; no config field raises it. Give such a child its own
  stdout (`>/dev/null 2>&1` in the wrapper).
- **Claim-check store operations do not observe the drain.** Parking an
  oversize body, and deleting one whose stream ended before its reference
  went out, each run on the request's own goroutine under a private 30s bound
  rather than the caller's context or the shutdown's. `Shutdown` waits for
  those goroutines, so a burst of oversize replies against a slow or
  unreachable JetStream can hold the drain 30s per store operation and push
  `Shutdown` into its "drain timed out" path. Size the pod's termination
  grace period past that, or keep `max_payload` at 8MB so claims stay rare.
- **The claim-check client grant this file used to give was wrong**, and a
  client still holding it cannot fetch anything while being able to write
  into its tenant's bucket. See [Large results](#large-results-claim-check)
  for what to grant instead.
- **`wire.ClaimStore` gained a `Delete` method** — source-breaking for an
  embedder supplying its own store through `ServerConfig.Claims` or
  `ClientConfig.Claims`. It removes a body whose claim id was never handed
  out, because the stream ended between the put and the frame that would have
  carried the reference. Returning nil and doing nothing is safe; those
  objects then wait for the bucket TTL like any other.

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
  for legacy backends (`*/list_changed` events are). The subscription
  acknowledgment reports only what is honored, so a client that asked for it
  is told up front rather than waiting for updates that never come.
- `x-mcp-header` is honored for **HTTP backends only** — the spec makes it a
  client requirement on Streamable HTTP and lets other transports ignore it,
  and a stdio subprocess has no headers to mirror into.
- Credential-expired backends are recycled on their next use and by the
  reaper, but only when idle: a long-lived in-flight call (a
  `subscriptions/listen` stream) can pin its process past the credentials'
  expiry until it completes.

## Development

`make ci` = format-check + staticcheck + build + tests. Tests run anywhere Go
does (embedded NATS server; the fake MCP server is the re-exec'd test
binary). `make test-race` runs the same suite under the race detector; CI
runs it as its own job on every PR, so "the race detector is clean" is a
checked claim rather than a remembered one. It is deliberately not part of
`make ci` — the run is several times slower — so a full local pass is
`make ci && make test-race`.
