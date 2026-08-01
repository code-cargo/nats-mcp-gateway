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

package cmd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// JSON's null unmarshals into a map without complaint and leaves it nil, so
// the _meta injection that follows wrote to a nil map and took the process
// down. Every other non-object --params is rejected on the way in; null was
// the one that got past the check and panicked instead.
func TestCallAcceptsNullParams(t *testing.T) {
	_, url := natstest.Run(t, nil)
	call := func(params string) error {
		// Nothing serves this subject, so the request ends at no-responders —
		// far enough to prove --params was handled.
		return runCall(&CallCmd{
			Server: "fake", Method: "tools/list", Params: params,
			NatsURL: url, Tenant: "acme", User: "_",
		}, &Globals{})
	}

	assert.NotPanics(t, func() {
		require.NoError(t, call("null"), "null params means no params, as it does in the shim")
	})

	// The reason null slipped through in the first place: everything else that
	// is not an object is still refused here.
	for _, params := range []string{"5", `"s"`, "[]", "true"} {
		assert.Error(t, call(params), "--params %s is not an object", params)
	}
}

// A deployment on a custom --subject-prefix was unreachable from the debug
// CLI, which built its client on the default mcp.v1 whatever the fleet used.
// The failure reads as ErrCodeNoGateway — the CLI blames a healthy gateway for
// not being there, which is the worst possible answer from a wire debugger.
func TestCallReachesACustomSubjectPrefix(t *testing.T) {
	nc, url := natstest.Run(t, nil)
	const prefix = "acme.mcp"

	served := make(chan string, 1)
	ws, err := wire.Serve(nc, wire.ServerConfig{Prefix: prefix, Servers: []string{"fake"}},
		func(ctx context.Context, in *wire.Inbound, w wire.StreamWriter) error {
			served <- in.Subject.Method
			return w.End([]byte(`{"jsonrpc":"2.0","id":"cli-1","result":{}}`))
		})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})

	require.NoError(t, runCall(&CallCmd{
		Server: "fake", Method: "tools/list", Params: "{}",
		NatsURL: url, Tenant: "acme", User: "_", SubjectPrefix: prefix,
	}, &Globals{}))

	select {
	case method := <-served:
		assert.Equal(t, "tools/list", method)
	case <-time.After(5 * time.Second):
		t.Fatal("the gateway on the custom prefix never saw the request")
	}
}

// The other half of a fenced deployment: an identity granted subscribe on only
// its own inbox. Without --inbox-prefix the CLI derives replies under _INBOX.,
// which such a grant excludes, so the debug tool is refused by the very
// deployments it exists to debug.
func TestCallWorksUnderAFencedInboxGrant(t *testing.T) {
	const prefix = "_INBOX_acme.u1"

	// The caller may publish its own tenant's requests and subscribe to
	// NOTHING but its prefixed inbox — no _INBOX.>. NoAuthUser binds the
	// unauthenticated connection natstest opens to the unrestricted gateway.
	gwConn, url := natstest.Run(t, &server.Options{
		NoAuthUser: "gateway",
		Users: []*server.User{
			{
				Username: "cli", Password: "pw",
				Permissions: &server.Permissions{
					Publish:   &server.SubjectPermission{Allow: []string{"mcp.v1.req.acme.>"}},
					Subscribe: &server.SubjectPermission{Allow: []string{prefix + ".>"}},
				},
			},
			{Username: "gateway", Password: "gw"}, // unrestricted; serves the replies
		},
	})

	ws, err := wire.Serve(gwConn, wire.ServerConfig{Servers: []string{"fake"}},
		func(ctx context.Context, in *wire.Inbound, w wire.StreamWriter) error {
			return w.End([]byte(`{"jsonrpc":"2.0","id":"cli-1","result":{}}`))
		})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ws.Shutdown(ctx)
	})

	asCLI := strings.Replace(url, "nats://", "nats://cli:pw@", 1)

	// The grant has to actually exclude the default inbox, or reaching the
	// gateway with the prefix would prove nothing about the prefix.
	denied := make(chan error, 1)
	probe, err := nats.Connect(asCLI, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		select {
		case denied <- err:
		default:
		}
	}))
	require.NoError(t, err)
	t.Cleanup(probe.Close)
	_, err = probe.SubscribeSync(probe.NewRespInbox())
	require.NoError(t, err)
	require.NoError(t, probe.Flush())
	select {
	case err := <-denied:
		require.ErrorIs(t, err, nats.ErrPermissionViolation, "the default inbox must be outside this grant")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the default inbox to be denied under this grant")
	}

	// A plain subscriber beside the queue-grouped gateway: it sees the same
	// requests and, unlike a wire handler, the reply subject they carry.
	replies := make(chan string, 1)
	mon, err := gwConn.Subscribe("mcp.v1.req.acme.>", func(m *nats.Msg) {
		select {
		case replies <- m.Reply:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = mon.Unsubscribe() })
	require.NoError(t, gwConn.Flush())

	// Completion is the assertion that bites. A CLI that cannot subscribe to
	// its own replies still publishes a request the gateway answers happily —
	// it just never hears the answer, and sits out the whole inactivity window
	// before giving up.
	done := make(chan error, 1)
	go func() {
		done <- runCall(&CallCmd{
			Server: "fake", Method: "tools/list", Params: "{}",
			NatsURL: asCLI, Tenant: "acme", User: "_", InboxPrefix: prefix,
		}, &Globals{})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the call never completed: the fenced identity never received its reply")
	}

	select {
	case reply := <-replies:
		assert.True(t, strings.HasPrefix(reply, prefix+"."),
			"reply inbox %q must live under the fenced prefix %q", reply, prefix)
	default:
		t.Fatal("the gateway never saw the request at all")
	}
}
