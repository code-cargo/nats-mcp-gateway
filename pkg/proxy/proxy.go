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
	"sync/atomic"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend/cred"
	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// CredLookup returns the credential resolver for a server (nil when its
// credentials are static — the config env/headers path needs no resolution
// here) and whether they are per-user. Per-user servers are pooled per
// caller; shared dynamic servers resolve under wire.UserUnattributed.
type CredLookup func(server string) (resolver *cred.CachedResolver, perUser bool)

// Proxy routes wire requests to pooled backends.
type Proxy struct {
	pool *backend.Pool
	log  *slog.Logger

	// Creds, when set, is consulted per request to resolve backend
	// credentials BEFORE the pool: the resolved generation becomes the pool
	// key's CredVersion, so a refreshed credential yields a new pool entry
	// and the old backend drains. Set it before Handler is first called.
	Creds CredLookup
}

// New builds a proxy over a pool.
func New(pool *backend.Pool, log *slog.Logger) *Proxy {
	if log == nil {
		log = slog.Default()
	}
	return &Proxy{pool: pool, log: log}
}

// Handler returns the wire.Handler for this proxy. Every request logs one
// summary line on completion — the gateway is a policy plane, and a request
// crossing it should be visible.
func (p *Proxy) Handler() wire.Handler {
	return func(ctx context.Context, in *wire.Inbound, w wire.StreamWriter) error {
		start := time.Now()
		var notifications atomic.Int64
		outcome := "ok"
		var errCode int

		reqLog := p.log.With(
			"tenant", in.Subject.Tenant,
			"user", in.Subject.User,
			"server", in.Subject.Server,
			"method", in.Subject.Method,
		)
		if raw := in.Header.Get(wire.HeaderName); raw != "" {
			// Decoded for the log: a name that needed sentinel encoding is
			// precisely the one an operator will struggle to trace, so it
			// must not appear in the audit trail as a base64 blob.
			name, err := mcpspec.DecodeHeaderValue(raw)
			if err != nil {
				name = raw
			}
			reqLog = reqLog.With("name", name)
		}
		defer func() {
			attrs := []any{
				"outcome", outcome,
				"duration", time.Since(start).Round(time.Millisecond).String(),
			}
			if n := notifications.Load(); n > 0 {
				attrs = append(attrs, "notifications", n)
			}
			if errCode != 0 {
				attrs = append(attrs, "code", errCode)
			}
			reqLog.Info("request", attrs...)
		}()
		fail := func(code int, message string, data any) error {
			outcome, errCode = "error", code
			return w.Err(code, message, data)
		}

		if cerr := Check(in); cerr != nil {
			reqLog.Warn("integrity check rejected request", "reason", cerr.Message)
			return fail(cerr.Code, cerr.Message, cerr.Data)
		}

		key := backend.Key{
			Server: in.Subject.Server,
			Tenant: in.Subject.Tenant,
		}
		if p.Creds != nil {
			if resolver, perUser := p.Creds(in.Subject.Server); resolver != nil {
				credUser := wire.UserUnattributed
				if perUser {
					credUser = in.Subject.User
				}
				// The resolve is NATS-verified-identity in, generation out:
				// the caller cannot reach another user's credential because
				// in.Subject.User is enforced by NATS, and the generation in
				// the key drains stale backends on refresh.
				_, gen, cerr := resolver.ResolveGen(ctx, in.Subject.Tenant, credUser, in.Subject.Server)
				if cerr != nil {
					reqLog.Warn("credential resolution failed", "err", cerr, "terminal", cred.IsTerminal(cerr))
					msg := "backend credentials temporarily unavailable, retry later: " + cerr.Error()
					if cred.IsTerminal(cerr) {
						msg = "backend credentials unavailable, do not retry: " + cerr.Error()
					}
					return fail(wire.ErrCodeCredentialUnavailable, msg, nil)
				}
				if perUser {
					key.CredSet = credUser
				}
				key.CredVersion = gen
			}
		}
		mux, release, err := p.pool.Get(ctx, key)
		if err != nil {
			if ctx.Err() != nil {
				outcome = "cancelled"
				return nil // cancelled while waiting: wrapper writes the empty end
			}
			return fail(wire.ErrCodeStreamLost, err.Error(), nil)
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
				return
			}
			notifications.Add(1)
			reqLog.Debug("notification forwarded", "notif_method", n.Method)
		}

		resp, err := mux.Call(ctx, in.Msg, notify)
		switch {
		case err == nil:
			body, encErr := jsonrpc.Encode(resp)
			if encErr != nil {
				return fail(jsonrpc.CodeInternalError, "response encoding failed", nil)
			}
			if resp.Error != nil {
				// The backend answered with a JSON-RPC error: forwarded
				// verbatim, but worth distinguishing in the summary line.
				outcome, errCode = "backend-error", resp.Error.Code
			}
			return w.End(body)
		case ctx.Err() != nil:
			outcome = "cancelled"
			return nil // cancelled or draining: the wire wrapper terminates
		case errors.Is(err, backend.ErrConnDead):
			return fail(wire.ErrCodeStreamLost,
				"backend connection lost mid-request, re-issue the request", nil)
		default:
			return fail(wire.ErrCodeStreamLost, err.Error(), nil)
		}
	}
}
