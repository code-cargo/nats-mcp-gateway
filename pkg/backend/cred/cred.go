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

// Package cred resolves per-(tenant, user, server) backend credentials so the
// gateway can inject a caller's own identity into the servers it fronts,
// instead of one shared secret from the config. The whole extension surface
// is one interface:
//
//	type Resolver interface {
//	    Resolve(ctx, tenant, user, server) (*Credentials, error)
//	}
//
// To resolve credentials from a new place you have three options, cheapest
// first (the same tiers as pkg/configsource):
//
//  1. Use a built-in: Static, File, Exec, the OAuth grants (client
//     credentials, RFC 8693 token exchange, refresh), or NATS request/reply.
//  2. Wrap a credential-helper command with Exec — any credential system
//     integrates with a ~20-line script that prints JSON, no gateway code.
//  3. Implement Resolver directly.
//
// Wrap any of them in Cached, which adds the TTL cache, single-flight,
// failure backoff, and the generation counter the backend pool keys on.
package cred

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	rand "math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// Credentials is the injectable material for one (tenant, user, server).
// Injection merges it OVER the server's static config: config env/headers
// keep non-secrets (AWS_REGION, feature flags), credentials override.
type Credentials struct {
	// Headers are added to every HTTP request (Authorization etc.).
	Headers map[string]string
	// Env is added to the stdio subprocess environment (AWS_* etc.;
	// temporary credentials are expected).
	Env map[string]string
	// ExpiresAt bounds the credentials' life; zero means non-expiring. The
	// pool clamps a backend's lifetime to it so a subprocess never outlives
	// its credentials.
	ExpiresAt time.Time
}

// Resolver produces the current credentials for one (tenant, user, server).
// Modes whose credentials do not vary by user are called with
// wire.UserUnattributed ("_") so their cache grain stays per-server.
type Resolver interface {
	Resolve(ctx context.Context, tenant, user, server string) (*Credentials, error)
}

// ResolveFunc adapts a function to a Resolver.
type ResolveFunc func(ctx context.Context, tenant, user, server string) (*Credentials, error)

// Resolve implements Resolver.
func (f ResolveFunc) Resolve(ctx context.Context, tenant, user, server string) (*Credentials, error) {
	return f(ctx, tenant, user, server)
}

// ErrIdentityRequired reports credentials asked for per user without a caller
// identity to ask under. The proxy refuses that on the request path, where it
// can say so precisely; this sentinel is for the pool factory, which sits
// below the layer that owns wire error codes and whose failures would
// otherwise all read as a lost stream. It travels wrapped, so the message
// keeps naming the server and the reload that caused it.
var ErrIdentityRequired = errors.New("per-user credentials require a caller identity")

// terminalError marks a resolve failure as authoritative: the resolver
// reached its source and was told no (unauthorized, unknown user). Retrying
// will not help, unlike a transport failure (source unreachable), and the
// proxy words its client error accordingly.
type terminalError struct{ err error }

func (e *terminalError) Error() string { return e.err.Error() }

func (e *terminalError) Unwrap() error { return e.err }

// Terminal wraps err as authoritative (do-not-retry). Nil stays nil.
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	return &terminalError{err: err}
}

// IsTerminal reports whether err (or anything it wraps) is authoritative.
func IsTerminal(err error) bool {
	var t *terminalError
	return errors.As(err, &t)
}

// FailureRef mints the correlation token for one caller-facing refusal: the
// caller is told the ref, the gateway logs the ref beside the real error, and
// the two are joined by an operator holding a user's screenshot.
//
// It lives here because credential resolution is where withholding the detail
// started, but nothing about it is credential-specific and pkg/proxy uses it
// for the pool and backend failures that withhold their detail for the same
// reason. One generator on purpose: an operator matching a ref from a
// screenshot should not have to know which subsystem minted it.
func FailureRef() string {
	return strconv.FormatUint(rand.Uint64(), 36)
}

// CallerMessage renders a resolve failure for the client that provoked it.
// Dropping the detail is the point. A resolver's error text is whatever its
// source emitted — the helper's stderr, the IdP's response body, the expanded
// path of an assertion file — and the client is the party all of that is
// being kept from; it also chooses when to provoke it, since asking for a
// server whose credentials it cannot have is enough. What survives is the
// category, the only part a client can act on. Log err itself with the same
// ref, or the failure becomes undiagnosable from either end.
func CallerMessage(err error, ref string) string {
	if IsTerminal(err) {
		return "backend credentials unavailable, do not retry (gateway ref " + ref + ")"
	}
	return "backend credentials temporarily unavailable, retry later (gateway ref " + ref + ")"
}

// credJSON is the interchange shape shared by every source that carries
// credentials as bytes (Exec stdout, File contents, the NATS reply):
//
//	{"headers": {...}, "env": {...}, "expiresAt": "RFC3339"}
//
// Unknown fields are ignored — the same forward-compat discipline as the
// config's NATS source: a newer controller may add fields an older gateway
// must not choke on.
type credJSON struct {
	Headers   map[string]string `json:"headers"`
	Env       map[string]string `json:"env"`
	ExpiresAt string            `json:"expiresAt"`
}

// decodeCredentials parses the credJSON interchange shape.
func decodeCredentials(data []byte) (*Credentials, error) {
	var raw credJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("cred: parse: %w", err)
	}
	c := &Credentials{Headers: raw.Headers, Env: raw.Env}
	if raw.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339, raw.ExpiresAt)
		if err != nil {
			return nil, fmt.Errorf("cred: expiresAt %q: %w", raw.ExpiresAt, err)
		}
		c.ExpiresAt = t
	}
	return c, nil
}

// expandPath substitutes {tenant}, {user}, and {server} in a configured path
// (token-store files, subject-token files). The tokens are wire subject-token
// safe (NATS-verified), so no traversal can be smuggled through them.
func expandPath(path, tenant, user, server string) string {
	return strings.NewReplacer(
		"{tenant}", tenant,
		"{user}", user,
		"{server}", server,
	).Replace(path)
}
