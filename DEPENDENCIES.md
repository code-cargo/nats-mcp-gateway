# External Dependencies

This file lists the dependencies used in this repository.

Note: `github.com/modelcontextprotocol/go-sdk` was evaluated and rejected on
day one — importing even its `jsonrpc` package pulls `segmentio/encoding`,
`segmentio/asm`, and `golang.org/x/sys` into the binary. The JSON-RPC envelope
and the ndjson stdio pump are hand-rolled instead (~200 lines).

## Direct

| Dependency                  | License      | Copyright Owner                          |
|-----------------------------|--------------|------------------------------------------|
| Go                          | BSD-3-Clause | The Go Authors                           |
| github.com/alecthomas/kong  | MIT          | Alec Thomas                              |
| github.com/nats-io/nats.go  | Apache-2.0   | The NATS Authors                         |
| github.com/stretchr/testify | MIT          | Mat Ryer, Tyler Bunnell and contributors |

## Test-only

| Dependency                        | License    | Copyright Owner  |
|-----------------------------------|------------|------------------|
| github.com/nats-io/nats-server/v2 | Apache-2.0 | The NATS Authors |

## Indirect

| Dependency                    | License      | Copyright Owner    |
|-------------------------------|--------------|--------------------|
| github.com/davecgh/go-spew    | ISC          | Dave Collins       |
| github.com/klauspost/compress | Apache-2.0   | Klaus Post et al.  |
| github.com/nats-io/nkeys      | Apache-2.0   | The NATS Authors   |
| github.com/nats-io/nuid       | Apache-2.0   | The NATS Authors   |
| github.com/pmezard/go-difflib | BSD-3-Clause | Patrick Mezard     |
| golang.org/x/crypto           | BSD-3-Clause | The Go Authors     |
| golang.org/x/sys              | BSD-3-Clause | The Go Authors     |
| gopkg.in/yaml.v3              | MIT / Apache-2.0 | Kirill Simonov, Canonical Ltd |
