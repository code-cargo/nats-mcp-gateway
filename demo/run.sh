#!/bin/sh
# Demo runbook — see demo/README.md for the full walkthrough.
set -e
cd "$(dirname "$0")"

if ! command -v nats-server >/dev/null 2>&1; then
  echo "nats-server not found: brew install nats-server (or see nats.io)" >&2
  exit 1
fi

echo "==> starting nats-server (demo config: 8MB payloads, 3 users)"
nats-server -c nats.conf &
NATS_PID=$!
trap 'kill $NATS_PID 2>/dev/null || true' EXIT
sleep 1

echo "==> starting gateway fronting @modelcontextprotocol/server-everything"
../bin/natsmcp gateway --config gateway.json &
GW_PID=$!
# `|| true` matters: the orderly shutdown at the end of the script kills these
# first, so by the time the trap runs there is usually nothing left to kill.
# Under `set -e` that failing kill would become the script's exit status, and
# `make demo` would report failure on every successful run.
trap 'kill $GW_PID $NATS_PID 2>/dev/null || true' EXIT
sleep 2

echo
echo "==> tools/list as admin (full path: NATS -> gateway -> npx server)"
NATSMCP_TENANT=demo ../bin/natsmcp call --server everything --method tools/list \
  --nats-url nats://admin:admin@127.0.0.1:4222 | head -3

echo
echo "==> tools/call echo as admin"
NATSMCP_TENANT=demo ../bin/natsmcp call --server everything --method tools/call \
  --params '{"name":"echo","arguments":{"message":"hello over NATS"}}' \
  --nats-url nats://admin:admin@127.0.0.1:4222

echo
echo "==> tools/list as reader (allowed)"
NATSMCP_TENANT=demo ../bin/natsmcp call --server everything --method tools/list \
  --nats-url nats://reader:reader@127.0.0.1:4222 | head -3

echo
echo "==> tools/call as reader (DENIED by NATS subject permissions —"
echo "    no gateway code runs; this is the auth plane)"
NATSMCP_TENANT=demo ../bin/natsmcp call --server everything --method tools/call \
  --params '{"name":"echo","arguments":{"message":"should fail"}}' \
  --nats-url nats://reader:reader@127.0.0.1:4222 || true

echo
echo "==> done. To try Claude Code: copy demo/mcp.json into your project as"
echo "    .mcp.json (with bin/natsmcp on PATH) and run /mcp."
# `|| true` here too: if the gateway already exited, this kill fails and
# `set -e` would abort the script before the trap ever runs.
kill $GW_PID $NATS_PID 2>/dev/null || true
wait 2>/dev/null || true
