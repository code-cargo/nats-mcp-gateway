#!/bin/sh
# Fire up a local NATS + gateway so Claude Code can use MCP servers THROUGH
# the gateway. Run this, leave it up, then in another terminal run `claude`
# from the repo root and try /mcp — the "everything" and "filesystem" servers
# arrive over the NATS wire via the shim.
#
# Ctrl-C tears everything down.
set -e
cd "$(dirname "$0")/.."
REPO="$(pwd)"

command -v nats-server >/dev/null 2>&1 || {
  echo "nats-server not found: brew install nats-server" >&2
  exit 1
}
command -v npx >/dev/null 2>&1 || {
  echo "npx not found (node required for the demo MCP servers)" >&2
  exit 1
}

make build >/dev/null
mkdir -p /tmp/natsmcp-demo /tmp/natsmcp-npm-cache
echo "hello from the gateway demo" > /tmp/natsmcp-demo/hello.txt

# .mcp.json for Claude Code: each entry is a shim (a thin stdio<->NATS pump)
# connecting as the demo "admin" user in tenant "demo". Absolute binary path
# so it works regardless of PATH. Gitignored — it's machine-local.
cat > "$REPO/.mcp.json" <<EOF
{
  "mcpServers": {
    "everything": {
      "command": "$REPO/bin/natsmcp",
      "args": ["shim", "--server", "everything"],
      "env": {
        "NATSMCP_NATS_URL": "nats://admin:admin@127.0.0.1:4222",
        "NATSMCP_TENANT": "demo"
      }
    },
    "filesystem": {
      "command": "$REPO/bin/natsmcp",
      "args": ["shim", "--server", "filesystem"],
      "env": {
        "NATSMCP_NATS_URL": "nats://admin:admin@127.0.0.1:4222",
        "NATSMCP_TENANT": "demo"
      }
    }
  }
}
EOF

echo "==> starting nats-server (demo users: gateway / admin / reader)"
nats-server -c demo/nats.conf &
NATS_PID=$!
trap 'kill $GW_PID $NATS_PID 2>/dev/null; exit 0' INT TERM
sleep 1

echo "==> starting gateway (servers: everything, filesystem; debug logs)"
NATSMCP_LOG_LEVEL=debug ./bin/natsmcp gateway --config demo/claude-gateway.json &
GW_PID=$!
sleep 1

echo
echo "Ready. In another terminal:"
echo "  cd $REPO && claude"
echo "then run /mcp — both servers ride the NATS wire through the gateway."
echo
echo "Try: 'read hello.txt with the filesystem tool' or 'echo something with"
echo "the everything server'. Watch this terminal for gateway logs."
echo "Ctrl-C stops NATS + gateway."
wait
