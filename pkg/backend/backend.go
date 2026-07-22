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

// Package backend connects the gateway to real MCP servers: stdio
// subprocesses and Streamable HTTP endpoints, pooled per (server, tenant,
// credential) and multiplexed per pool entry.
package backend

import (
	"context"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
)

// Backend creates connections to one configured MCP server.
type Backend interface {
	// Connect establishes a live connection (spawns the subprocess, dials
	// the endpoint). The returned Conn is a raw JSON-RPC message channel.
	Connect(ctx context.Context) (Conn, error)
}

// Conn is a live duplex JSON-RPC message channel to an MCP server. Read
// blocks until a message arrives or the connection dies; Write may be called
// concurrently. Close is idempotent and must unblock Read.
type Conn interface {
	Read(ctx context.Context) (*jsonrpc.Message, error)
	Write(ctx context.Context, msg *jsonrpc.Message) error
	Close() error
}
