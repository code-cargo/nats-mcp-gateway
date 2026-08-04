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
	"fmt"
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

		name, cerr := Check(in)
		if cerr != nil {
			// The claimed name, on the rejection line only and under a key
			// that says so. The summary line stays nameless — nothing here is
			// authorized — but an operator reading a rejection still needs to
			// know which tool was attempted, and a refusal for a reason that
			// is not about the name (unsupported version, malformed _meta)
			// carries it nowhere else.
			attrs := []any{"reason", cerr.Message}
			if raw := in.Header.Get(wire.HeaderName); raw != "" {
				claimed, decErr := mcpspec.DecodeHeaderValue(raw)
				if decErr != nil {
					claimed = raw
				}
				attrs = append(attrs, "claimed_name", claimed)
			}
			reqLog.Warn("integrity check rejected request", attrs...)
			return fail(cerr.Code, cerr.Message, cerr.Data)
		}
		if name != "" {
			// Straight from the check, in its decoded form: a name that needed
			// sentinel encoding is precisely the one an operator will struggle
			// to trace, so it must not reach the audit trail as a base64 blob.
			reqLog = reqLog.With("name", name)
		}

		key := backend.Key{
			Server: in.Subject.Server,
			Tenant: in.Subject.Tenant,
		}
		if p.Creds != nil {
			if resolver, perUser := p.Creds(in.Subject.Server); resolver != nil {
				credUser := wire.UserUnattributed
				if perUser {
					// "_" says "this deployment has no per-user auth", which
					// is not an identity and is the one {user} token every
					// caller in the tenant can reach. Since {user} selects the
					// credential AND keys the pool, resolving it here would
					// collapse every unattributed caller onto one credential
					// and one process — on the servers configured so that must
					// not happen — and would do it silently, which is how a
					// grant of mcp.v1.req.{tenant}.> instead of
					// …{tenant}.{user}.> stays undetected. The subject cannot
					// carry the grain the server asked for, so nothing is
					// resolved.
					if in.Subject.User == wire.UserUnattributed {
						return fail(wire.ErrCodeCredentialUnavailable,
							fmt.Sprintf("backend credentials unavailable, do not retry: server %q resolves "+
								"credentials per user and this request is unattributed (subject user token %q) — "+
								"publish under a {user} token minted by per-user NATS auth",
								in.Subject.Server, wire.UserUnattributed), nil)
					}
					credUser = in.Subject.User
				}
				// The resolve is NATS-verified-identity in, generation out:
				// the caller cannot reach another user's credential because
				// in.Subject.User is enforced by NATS, and the generation in
				// the key drains stale backends on refresh.
				_, gen, cerr := resolver.ResolveGen(ctx, in.Subject.Tenant, credUser, in.Subject.Server)
				if cerr != nil {
					ref := cred.FailureRef()
					reqLog.Warn("credential resolution failed", "err", cerr,
						"terminal", cred.IsTerminal(cerr), "ref", ref)
					return fail(wire.ErrCodeCredentialUnavailable, cred.CallerMessage(cerr, ref), nil)
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
			if errors.Is(err, cred.ErrIdentityRequired) {
				// The refusal above, reached through the factory instead: a
				// reload changed the server's grain after this request was
				// keyed. Same condition, so same code — it is a credential
				// failure, not a lost stream, and unlike the request-path
				// refusal it clears on re-issue under the new mode.
				reqLog.Warn("credential grain changed under a live request", "err", err)
				return fail(wire.ErrCodeCredentialUnavailable,
					"backend credentials temporarily unavailable, retry later: "+err.Error(), nil)
			}
			var unavailable *cred.Unavailable
			if errors.As(err, &unavailable) {
				// The factory's resolve failed — the same failure the request
				// path above answers, arriving one layer down because that
				// resolve hit the cache and this one missed it (a TTL boundary,
				// a concurrent 401 Invalidate). The factory has already logged
				// the detail against the ref its message carries.
				return fail(wire.ErrCodeCredentialUnavailable, unavailable.Message, nil)
			}
			// Everything else the pool can fail with: the factory's own errors
			// (which name the backend's executable and argv, a gateway-pod temp
			// path, or a server this instance does not serve) and the pool's
			// limits. None of that is the caller's, so it goes to the log under
			// a ref and the caller gets the category — which is all -32010
			// means to it anyway. cred mints the ref for every site that needs
			// one: an operator holding a screenshot greps for one shape.
			ref := cred.FailureRef()
			reqLog.Warn("backend unavailable", "err", err, "ref", ref)
			return fail(wire.ErrCodeStreamLost,
				"backend unavailable, re-issue the request (gateway ref "+ref+")", nil)
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
			// As above: a mux failure carries the transport's own words (a
			// broken pipe names the pipe, an HTTP write names the endpoint),
			// and the caller acts on the code, not on them.
			ref := cred.FailureRef()
			reqLog.Warn("backend call failed", "err", err, "ref", ref)
			return fail(wire.ErrCodeStreamLost,
				"backend call failed, re-issue the request (gateway ref "+ref+")", nil)
		}
	}
}
