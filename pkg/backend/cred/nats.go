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

package cred

import (
	"context"
	"fmt"
	"time"

	nats "github.com/nats-io/nats.go"

	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// NATS fetches credentials over NATS request/reply — the same pattern as
// pkg/configsource: the source of truth (the controller holding the token
// store and doing the actual OAuth/STS exchanges) stays off this process, and
// the credential rides only the authenticated NATS connection.
//
// Controller-side contract (the responder the platform provides):
//   - Respond on {SubjectPrefix}.{tenant}.{user}.{server} with credJSON:
//     {"headers"|"env": {...}, "expiresAt": "RFC3339"}.
//   - Reply with a Nats-Service-Error header to refuse (unknown user,
//     not authorized); refusals are Terminal — the gateway will not retry
//     them beyond the cache backoff.
//
// NATS permissions fence the exchange: only the controller serves the
// subject, and only gateway identities may request it. The gateway asks with
// the NATS-verified caller identity from the wire subject, so it cannot be
// tricked into fetching another user's credential.
type NATS struct {
	// Conn is the gateway's own NATS connection.
	Conn *nats.Conn
	// SubjectPrefix defaults to "mcp.v1.cred".
	SubjectPrefix string
	// RequestTimeout bounds one fetch (default 5s).
	RequestTimeout time.Duration
}

// Resolve implements Resolver.
func (n *NATS) Resolve(ctx context.Context, tenant, user, server string) (*Credentials, error) {
	prefix := n.SubjectPrefix
	if prefix == "" {
		prefix = wire.DefaultPrefix + ".cred"
	}
	timeout := n.RequestTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	subject := prefix + "." + tenant + "." + user + "." + server
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg, err := n.Conn.RequestWithContext(ctx, subject, nil)
	if err != nil {
		return nil, fmt.Errorf("cred: request to %q: %w", subject, err)
	}
	if svcErr := msg.Header.Get("Nats-Service-Error"); svcErr != "" {
		return nil, Terminal(fmt.Errorf("cred: responder refused %q: %s", subject, svcErr))
	}
	creds, err := decodeCredentials(msg.Data)
	if err != nil {
		return nil, fmt.Errorf("cred: from %q: %w", subject, err)
	}
	return creds, nil
}
