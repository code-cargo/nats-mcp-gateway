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

// Package proxy is the gateway's request path: integrity check, then forward
// opaque JSON-RPC bytes to the pooled backend and stream its notifications
// and response back onto the wire.
package proxy

import (
	"context"
	"errors"
	"log/slog"

	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// Proxy routes wire requests to pooled backends.
type Proxy struct {
	pool *backend.Pool
	log  *slog.Logger
}

// New builds a proxy over a pool.
func New(pool *backend.Pool, log *slog.Logger) *Proxy {
	if log == nil {
		log = slog.Default()
	}
	return &Proxy{pool: pool, log: log}
}

// Handler returns the wire.Handler for this proxy.
func (p *Proxy) Handler() wire.Handler {
	return func(ctx context.Context, in *wire.Inbound, w wire.StreamWriter) error {
		if cerr := Check(in); cerr != nil {
			return w.Err(cerr.Code, cerr.Message, cerr.Data)
		}

		key := backend.Key{
			Server: in.Subject.Server,
			Tenant: in.Subject.Tenant,
		}
		mux, release, err := p.pool.Get(ctx, key)
		if err != nil {
			if ctx.Err() != nil {
				return nil // cancelled while waiting: wrapper writes the empty end
			}
			return w.Err(wire.ErrCodeStreamLost, err.Error(), nil)
		}
		defer release()

		notify := func(n *jsonrpc.Message) {
			body, err := jsonrpc.Encode(n)
			if err != nil {
				return
			}
			if err := w.Msg(body); err != nil {
				p.log.Warn("dropping notification", "err", err,
					"server", key.Server, "tenant", key.Tenant)
			}
		}

		resp, err := mux.Call(ctx, in.Msg, notify)
		switch {
		case err == nil:
			body, encErr := jsonrpc.Encode(resp)
			if encErr != nil {
				return w.Err(jsonrpc.CodeInternalError, "response encoding failed", nil)
			}
			return w.End(body)
		case ctx.Err() != nil:
			return nil // cancelled or draining: the wire wrapper terminates
		case errors.Is(err, backend.ErrConnDead):
			return w.Err(wire.ErrCodeStreamLost,
				"backend connection lost mid-request, re-issue the request", nil)
		default:
			return w.Err(wire.ErrCodeStreamLost, err.Error(), nil)
		}
	}
}
