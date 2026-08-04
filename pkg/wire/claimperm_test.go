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

package wire

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
)

// The rest of the suite runs as an unrestricted user, so nothing in it can tell
// a working claim grant from one that is backwards — which is how the README
// came to document the bucket's ingest subjects as a client's read access.
// These tests drive the real ObjectClaims through the grants the README hands
// operators, so the two blocks below are the assertion, and any edit to them
// has to be an edit to the README's claim-check section too.
const (
	claimPermTenant = "acme"
	claimPermBucket = claimBucketPrefix + claimPermTenant // MCP_CLAIMS_acme
	claimPermStream = "OBJ_" + claimPermBucket

	claimUserGateway = "gateway"
	claimUserClient  = "client"
	claimUserIngest  = "ingest"

	// claimPermTimeout is a backstop, not a budget: a call that reaches a
	// refused leg is cut short by runDenied instead of waiting it out.
	claimPermTimeout = 10 * time.Second
)

// Reading a JetStream object store is publish traffic to $JS.API, which is why
// the client block grants nothing under $O — the bucket stream's own subjects
// are its ingest, and holding them is write access that can read nothing.
var (
	clientClaimGrant = []string{
		"$JS.API.STREAM.INFO.OBJ_MCP_CLAIMS_acme",
		"$JS.API.DIRECT.GET.OBJ_MCP_CLAIMS_acme.>",
		"$JS.API.CONSUMER.CREATE.OBJ_MCP_CLAIMS_acme.>",
		"$JS.API.CONSUMER.DELETE.OBJ_MCP_CLAIMS_acme.>",
	}
	gatewayClaimGrant = []string{
		"$JS.API.STREAM.CREATE.OBJ_MCP_CLAIMS_acme",
		"$JS.API.STREAM.UPDATE.OBJ_MCP_CLAIMS_acme",
		"$JS.API.STREAM.INFO.OBJ_MCP_CLAIMS_acme",
		"$JS.API.STREAM.PURGE.OBJ_MCP_CLAIMS_acme",
		"$JS.API.DIRECT.GET.OBJ_MCP_CLAIMS_acme.>",
		"$O.MCP_CLAIMS_acme.>",
	}
	// ingestClaimGrant is the grant the README used to give clients. It reads
	// like bucket access and is its inverse.
	ingestClaimGrant = []string{"$O.MCP_CLAIMS_acme.>"}
)

// claimPermNATS boots embedded JetStream carrying one restricted user per
// claim identity. Subscribe is left unrestricted because the README's blocks
// are publish-only: replies and push-consumer deliveries ride the client's own
// inbox, and every subject that fences claims is a publish. The unauthenticated
// connection is an unrestricted admin, used only to observe bucket state that
// no identity under test is meant to be able to see.
func claimPermNATS(t *testing.T) (admin *nats.Conn, url string) {
	t.Helper()
	users := []*server.User{{Username: "admin", Password: "admin"}}
	for user, grant := range map[string][]string{
		claimUserGateway: gatewayClaimGrant,
		claimUserClient:  clientClaimGrant,
		claimUserIngest:  ingestClaimGrant,
	} {
		users = append(users, &server.User{
			Username:    user,
			Password:    user,
			Permissions: &server.Permissions{Publish: &server.SubjectPermission{Allow: grant}},
		})
	}
	return natstest.Run(t, &server.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Users:      users,
		NoAuthUser: "admin",
	})
}

// dialClaim connects as one restricted user. Denials arrive on the returned
// channel rather than as the caller's error: nats.go routes a refused PUBLISH
// to the async error handler and leaves the request that provoked it waiting,
// so asserting on a denied subject means reading it here.
func dialClaim(t *testing.T, url, user string) (*nats.Conn, *ObjectClaims, <-chan error) {
	t.Helper()
	denied := make(chan error, 8)
	nc, err := nats.Connect(url, nats.UserInfo(user, user),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if errors.Is(err, nats.ErrPermissionViolation) {
				select {
				case denied <- err:
				default:
				}
			}
		}))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return nc, &ObjectClaims{JS: js, MaxAge: time.Minute}, denied
}

func awaitDenial(t *testing.T, denied <-chan error) error {
	t.Helper()
	select {
	case err := <-denied:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the server reported no permission violation")
		return nil
	}
}

// runDenied runs a store call that is expected to have a leg refused, and
// returns the refusal. The context is cancelled as soon as the server reports
// it, because a refused PUBLISH does not end the request that provoked it —
// nats.go hands the violation to the async error handler and the request goes
// on waiting for a reply that will never come, so an uncancelled call here
// would cost its whole timeout.
func runDenied(t *testing.T, denied <-chan error, call func(ctx context.Context)) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), claimPermTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		call(ctx)
	}()
	err := awaitDenial(t, denied)
	cancel()
	<-done
	return err
}

// claimObjectExists reports bucket state over the admin connection, so the
// answer does not depend on the grant under test.
func claimObjectExists(t *testing.T, admin *nats.Conn, id string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), claimPermTimeout)
	defer cancel()
	js, err := jetstream.New(admin)
	require.NoError(t, err)
	obs, err := js.ObjectStore(ctx, claimPermBucket)
	require.NoError(t, err)
	if _, err := obs.GetInfo(ctx, id); err != nil {
		require.ErrorIs(t, err, jetstream.ErrObjectNotFound)
		return false
	}
	return true
}

func TestClaimGrantFetch(t *testing.T) {
	admin, url := claimPermNATS(t)
	_, gateway, _ := dialClaim(t, url, claimUserGateway)
	body := bytes.Repeat([]byte("claimed-response-body"), 15_000) // ~315KB: several chunks

	for _, tc := range []struct {
		name string
		user string
		// wantBody: the fetch returns the object, digest and all.
		wantBody bool
		// wantDenied: the subject the server must refuse this identity.
		wantDenied string
		// wantObject: the object is still in the bucket afterwards.
		wantObject bool
	}{
		{
			name:     "documented client grant fetches",
			user:     claimUserClient,
			wantBody: true,
			// The eager delete needs a metadata write, and the purge behind it
			// would let any client wipe its tenant's live claims. Refusing it
			// is the grant working: TTL is the reaper.
			wantDenied: "$O." + claimPermBucket + ".M.",
			wantObject: true,
		},
		{
			// The two blocks are disjoint on purpose, neither a superset of the
			// other: reading an object means running a consumer over its
			// chunks, and the write side is granted none. The gateway Puts and
			// Deletes, it never fetches.
			name:       "gateway grant cannot fetch what it wrote",
			user:       claimUserGateway,
			wantDenied: "$JS.API.CONSUMER.CREATE." + claimPermStream,
			wantObject: true,
		},
		{
			// The grant the README used to document for clients: the first leg
			// of a fetch is a stream bind it cannot make.
			name:       "bucket-ingest grant cannot fetch",
			user:       claimUserIngest,
			wantDenied: "$JS.API.STREAM.INFO." + claimPermStream,
			wantObject: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), claimPermTimeout)
			defer cancel()
			id, err := gateway.Put(ctx, claimPermTenant, body)
			require.NoError(t, err)

			_, claims, denied := dialClaim(t, url, tc.user)
			var (
				got  []byte
				ferr error
			)
			refusal := runDenied(t, denied, func(ctx context.Context) {
				got, ferr = claims.Fetch(ctx, claimPermTenant, id)
			})
			assert.ErrorContains(t, refusal, tc.wantDenied)

			if tc.wantBody {
				require.NoError(t, ferr)
				// Equality is the digest assertion: the library checks the
				// object's SHA-256 at EOF, which a full read reaches.
				assert.Equal(t, body, got)
			} else {
				require.Error(t, ferr)
			}
			assert.Equal(t, tc.wantObject, claimObjectExists(t, admin, id))
		})
	}
}

func TestClaimGatewayGrantPutDelete(t *testing.T) {
	admin, url := claimPermNATS(t)
	_, gateway, denied := dialClaim(t, url, claimUserGateway)
	ctx, cancel := context.WithTimeout(context.Background(), claimPermTimeout)
	defer cancel()

	// Put creates the bucket on first use, so this leg covers STREAM.CREATE as
	// well as the $O chunk and metadata writes.
	id, err := gateway.Put(ctx, claimPermTenant, bytes.Repeat([]byte("body"), 1000))
	require.NoError(t, err)
	require.True(t, claimObjectExists(t, admin, id))

	// Delete is the claim whose id was never handed out: it needs the metadata
	// write plus STREAM.PURGE.
	require.NoError(t, gateway.Delete(ctx, claimPermTenant, id))
	assert.False(t, claimObjectExists(t, admin, id))
	assert.Empty(t, denied, "the gateway grant must carry put and delete whole")
}

func TestClaimClientGrantCannotWrite(t *testing.T) {
	_, url := claimPermNATS(t)
	_, gateway, _ := dialClaim(t, url, claimUserGateway)
	ctx, cancel := context.WithTimeout(context.Background(), claimPermTimeout)
	defer cancel()
	coTenant := []byte(`{"jsonrpc":"2.0","id":"1","result":{"private":true}}`)
	id, err := gateway.Put(ctx, claimPermTenant, coTenant)
	require.NoError(t, err)

	nc, claims, denied := dialClaim(t, url, claimUserClient)
	for _, tc := range []struct {
		name    string
		subject string
	}{
		{
			// Derivable from a claim id alone, and a rollup here rewrites what
			// a co-tenant's fetch resolves to — see
			// TestClaimIngestGrantCorruptsCoTenantFetch.
			name:    "metadata rollup over a claim id",
			subject: "$O." + claimPermBucket + ".M." + base64.URLEncoding.EncodeToString([]byte(id)),
		},
		{
			// Chunk traffic nobody asked for, up to the bucket's maxBytes.
			name:    "chunks into the tenant bucket",
			subject: "$O." + claimPermBucket + ".C.unsolicited",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, nc.Publish(tc.subject, []byte("x")))
			require.NoError(t, nc.Flush())
			assert.ErrorContains(t, awaitDenial(t, denied), tc.subject)
		})
	}

	t.Run("put through the store", func(t *testing.T) {
		var perr error
		refusal := runDenied(t, denied, func(ctx context.Context) {
			_, perr = claims.Put(ctx, claimPermTenant, []byte("smuggled"))
		})
		require.Error(t, perr)
		// Put creates the bucket on first use, and the library's create-or-
		// update tries UPDATE before CREATE, so that is the leg refused first.
		// The client holds neither, whether or not the bucket exists.
		assert.ErrorContains(t, refusal, "$JS.API.STREAM.UPDATE."+claimPermStream)
	})
}

// TestClaimIngestGrantCorruptsCoTenantFetch is why the client block is four
// $JS.API subjects and not $O.<bucket>.>: the ingest grant lets a holder
// rewrite the metadata of a claim it cannot read, from the claim id alone.
func TestClaimIngestGrantCorruptsCoTenantFetch(t *testing.T) {
	_, url := claimPermNATS(t)
	_, gateway, _ := dialClaim(t, url, claimUserGateway)
	ctx, cancel := context.WithTimeout(context.Background(), claimPermTimeout)
	defer cancel()
	id, err := gateway.Put(ctx, claimPermTenant, []byte(`{"jsonrpc":"2.0","id":"1","result":{"ok":true}}`))
	require.NoError(t, err)

	// The bucket stream takes a rollup on the metadata subject, so one publish
	// replaces the object's metadata with a size and chunk pointer of the
	// attacker's choosing. Nothing here reads the object.
	attacker, _, _ := dialClaim(t, url, claimUserIngest)
	js, err := jetstream.New(attacker)
	require.NoError(t, err)
	meta, err := json.Marshal(jetstream.ObjectInfo{
		ObjectMeta: jetstream.ObjectMeta{Name: id},
		Bucket:     claimPermBucket,
		NUID:       "attacker",
	})
	require.NoError(t, err)
	msg := nats.NewMsg("$O." + claimPermBucket + ".M." + base64.URLEncoding.EncodeToString([]byte(id)))
	msg.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	msg.Data = meta
	_, err = js.PublishMsg(ctx, msg)
	require.NoError(t, err)

	// The victim holds the documented read-only grant and is given no error to
	// notice: a zero-size object never reaches the digest check, so the fetch
	// returns a body the gateway never wrote.
	_, claims, denied := dialClaim(t, url, claimUserClient)
	var (
		got  []byte
		ferr error
	)
	runDenied(t, denied, func(ctx context.Context) {
		got, ferr = claims.Fetch(ctx, claimPermTenant, id)
	})
	require.NoError(t, ferr)
	assert.Empty(t, got, "a rollup from the ingest grant substitutes a co-tenant's claimed result")
}

// The eager delete at the end of Fetch is a PUBLISH, and the documented client
// grant refuses it — which nats.go reports to the async error handler while
// leaving the request waiting for a reply that will never come. Under the
// Fetch-wide budget that was a 30s stall added to every claimed response in
// the deployment this README recommends, with the body already in hand.
//
// So it is bounded separately and latched off after the first failure: refused
// is a permanent property of this identity, and the bucket TTL is the backstop
// the eager delete only optimizes.
func TestEagerDeleteIsBoundedAndLatchesOff(t *testing.T) {
	admin, url := claimPermNATS(t)
	_, gw, _ := dialClaim(t, url, claimUserGateway)
	_, client, denied := dialClaim(t, url, claimUserClient)

	ctx, cancel := context.WithTimeout(context.Background(), claimPermTimeout)
	defer cancel()

	put := func() string {
		t.Helper()
		id, err := gw.Put(ctx, claimPermTenant, []byte("payload"))
		require.NoError(t, err)
		return id
	}

	// First fetch: the delete is attempted and refused, and the fetch still
	// returns the body — the claim survives to its TTL, which is correct.
	first := put()
	body, err := client.Fetch(ctx, claimPermTenant, first)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), body)
	assert.Contains(t, awaitDenial(t, denied).Error(), "$O."+claimPermBucket,
		"the refusal is the metadata write the delete needs")
	assert.True(t, claimObjectExists(t, admin, first))

	// Second fetch: no further attempt, so no further refusal. Asserted on the
	// server's own violation report rather than on elapsed time, which would
	// pin the bound instead of the latch.
	second := put()
	body, err = client.Fetch(ctx, claimPermTenant, second)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), body)
	select {
	case err := <-denied:
		t.Fatalf("a second fetch re-attempted the delete it already knows is refused: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	assert.True(t, claimObjectExists(t, admin, second))
}

// A caller whose context ends is not a statement about whether this identity
// may delete. Latching on it would let one impatient MCP client disable eager
// cleanup for every tenant the process serves, for the life of the process.
//
// Staged deterministically rather than by racing a cancel against a fetch: the
// read-only client grant makes the delete hang to its own bound every time (a
// refused publish is never answered), so a caller deadline shorter than that
// bound always expires inside the delete and never inside the body read.
func TestCallerCancellationDoesNotLatchOffEagerDelete(t *testing.T) {
	_, url := claimPermNATS(t)
	_, gw, _ := dialClaim(t, url, claimUserGateway)
	_, client, _ := dialClaim(t, url, claimUserClient)

	ctx, cancel := context.WithTimeout(context.Background(), claimPermTimeout)
	defer cancel()
	id, err := gw.Put(ctx, claimPermTenant, []byte("payload"))
	require.NoError(t, err)

	short, shortCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer shortCancel()
	body, err := client.Fetch(short, claimPermTenant, id)
	require.NoError(t, err, "the body is read long before the caller's deadline")
	require.Equal(t, []byte("payload"), body)
	require.Error(t, short.Err(), "the caller's deadline must have expired inside the delete")

	assert.False(t, client.noEagerDelete.Load(),
		"a caller's own deadline disabled eager cleanup for every tenant this process serves")
}
