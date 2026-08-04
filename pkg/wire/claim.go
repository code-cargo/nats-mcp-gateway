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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ClaimStore parks reply bodies too large for the NATS wire (the claim-check
// pattern): the gateway Puts the oversize body and sends only a claim id on
// the end frame; the client Fetches it before the consumer sees anything.
// Both sides must opt in — see HeaderAcceptClaim.
type ClaimStore interface {
	// Put stores body under a fresh unguessable id, fenced to tenant.
	Put(ctx context.Context, tenant string, body []byte) (id string, err error)
	// Fetch retrieves (and best-effort deletes) a claimed body.
	Fetch(ctx context.Context, tenant, id string) ([]byte, error)
	// Delete removes a claim whose id was never handed out, so a body nobody
	// can ever fetch does not occupy the tenant's bucket until the TTL.
	Delete(ctx context.Context, tenant, id string) error
}

const (
	// claimBucketPrefix names the per-tenant Object Store buckets. One bucket
	// per tenant so NATS permissions fence claims exactly like request
	// subjects; within a tenant, secrecy rests on the unguessable id and the
	// privacy of the reply inbox it rides.
	claimBucketPrefix = "MCP_CLAIMS_"

	// claimOpTimeout bounds one store operation regardless of caller ctx.
	claimOpTimeout = 30 * time.Second

	// claimEagerDeleteTimeout bounds only the cleanup delete at the end of a
	// Fetch, which the caller is waiting on with the body already in hand.
	//
	// It is separate from claimOpTimeout, and short, because the documented
	// read-only client grant cannot delete: the delete is a publish, and
	// nats.go hands a publish permissions-violation to the async error handler
	// WITHOUT cancelling the request waiting on a reply that will now never
	// come. So the refusal is indistinguishable from silence and costs the
	// full budget. Under claimOpTimeout that was a 30s stall added to every
	// claimed response in the deployment the README recommends.
	claimEagerDeleteTimeout = 2 * time.Second
)

// DefaultClaimMaxAge and DefaultClaimMaxBytes are what a claim bucket gets for
// a limit it is not given — meaning any value <= 0, not just the zero. They
// live here, beside the code that substitutes them, so a caller validating
// configuration cannot drift from the value that will actually be in force —
// see FilledClaimLimits.
const (
	DefaultClaimMaxAge   = 5 * time.Minute
	DefaultClaimMaxBytes = 1 << 30 // 1GiB
)

// FilledClaimLimits returns the bucket limits in force for the given pair,
// substituting the default for anything <= 0. It is the claim-check analogue
// of backend.PoolConfig.Filled, exported for the same reason: a caller
// checking configuration has to compare against what the bucket will actually
// get, not against the zero an operator left behind.
//
// The bound is <= 0 rather than == 0 because a negative limit is not a request
// for a bucket that expires instantly or holds nothing — it is meaningless, and
// meaningless is what the default exists for. A caller checking configuration
// has to fold negatives the same way or it will report a conflict against a
// value the bucket is never going to use: hence the > 0 guards in
// GatewayCmd.checkFileSourceFlags.
func FilledClaimLimits(maxAge time.Duration, maxBytes int64) (time.Duration, int64) {
	if maxAge <= 0 {
		maxAge = DefaultClaimMaxAge
	}
	if maxBytes <= 0 {
		maxBytes = DefaultClaimMaxBytes
	}
	return maxAge, maxBytes
}

// ObjectClaims implements ClaimStore on a JetStream Object Store: one
// lazily-created bucket per tenant, TTL as the cleanup backstop (the client
// deletes eagerly after a successful fetch, but a crashed client must not
// leak multi-MB objects), size-capped so claims cannot fill the disk.
// Objects are chunked, digested (SHA-256, verified on read by the client
// library), and survive until TTL, so a client that dies mid-fetch can
// re-issue the request and still succeed.
type ObjectClaims struct {
	// JS is the JetStream context (jetstream.New(nc)).
	JS jetstream.JetStream
	// MaxAge is the bucket TTL (DefaultClaimMaxAge if <= 0). Gateway-side only.
	MaxAge time.Duration
	// MaxBytes caps each tenant bucket (DefaultClaimMaxBytes if <= 0).
	// Gateway-side only.
	MaxBytes int64

	mu      sync.Mutex
	buckets map[string]jetstream.ObjectStore // tenant -> handle (Put side)

	// noEagerDelete latches once a cleanup delete has failed, so the cost
	// above is paid once per process rather than once per claimed response.
	// A delete that failed is either refused — the documented read-only grant,
	// which is a permanent property of this identity — or the store is
	// unhealthy; the next attempt costs the same and achieves the same
	// nothing. Never unlatched, because the bucket TTL is the backstop by
	// design and the eager delete is an optimization on top of it.
	noEagerDelete atomic.Bool
}

// Put implements ClaimStore. It creates the tenant's bucket on first use.
func (o *ObjectClaims) Put(ctx context.Context, tenant string, body []byte) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, claimOpTimeout)
	defer cancel()

	var idBytes [16]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return "", fmt.Errorf("wire: claim id: %w", err)
	}
	id := hex.EncodeToString(idBytes[:])

	// Object chunks travel as ordinary stream publishes, so the chunk size
	// must fit under max_payload — the library default (128KB) would be
	// rejected outright by a server configured below that.
	meta := jetstream.ObjectMeta{Name: id}
	if max := o.JS.Conn().MaxPayload(); max > 0 && max < 128*1024 {
		// Headroom for the chunk message's subject and headers.
		chunk := max - 1024
		if chunk < 1024 {
			chunk = max
		}
		meta.Opts = &jetstream.ObjectMetaOptions{ChunkSize: uint32(chunk)}
	}

	obs, err := o.bucket(ctx, tenant)
	if err != nil {
		return "", err
	}
	if _, err := obs.Put(ctx, meta, bytes.NewReader(body)); err != nil {
		// Rebuild-and-retry ONLY when the bucket vanished underneath the
		// cached handle (ops cleanup, JS restart). Anything else — quota,
		// timeout, payload — would fail identically on retry, so re-uploading
		// a multi-MB body (and stomping the bucket's settings via
		// CreateOrUpdate) would double the load exactly when JS is under
		// pressure.
		if !bucketMissing(err) {
			return "", fmt.Errorf("wire: claim put: %w", err)
		}
		obs, rerr := o.recreateBucket(ctx, tenant)
		if rerr != nil {
			return "", fmt.Errorf("wire: claim put: %w (bucket rebuild also failed: %w)", err, rerr)
		}
		if _, err := obs.Put(ctx, meta, bytes.NewReader(body)); err != nil {
			return "", fmt.Errorf("wire: claim put: %w", err)
		}
	}
	return id, nil
}

// bucketMissing reports whether a Put failure plausibly means the bucket is
// GONE — the condition where rebuild-and-retry helps. The sentinel set is
// grounded in what nats.go v1.52.0 actually surfaces:
//   - ErrBucketNotFound / ErrStreamNotFound: the lookup legs' explicit
//     not-found remaps.
//   - nats.ErrNoResponders: a deleted bucket reached through a CACHED handle
//     — the object store reads via direct-get subjects served by the stream
//     itself, so a deleted stream answers with no-responders (verified by
//     TestClaimPutRecoversFromDeletedBucketOnly). Also fires when JetStream
//     is down entirely, where the rebuild simply fails fast and the original
//     error is returned — one cheap API call, no re-upload.
//   - jetstream.ErrNoStreamResponse: chunk publishes to a bucket deleted
//     mid-upload (the legacy nats.ErrNoStreamResponse never escapes the
//     jetstream package). Also fires on transient leadership blips; the
//     cost is bounded because the retry is single-shot and the re-upload
//     only happens if the rebuild succeeded, i.e. JS answered again.
func bucketMissing(err error) bool {
	return errors.Is(err, jetstream.ErrBucketNotFound) ||
		errors.Is(err, jetstream.ErrStreamNotFound) ||
		errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, nats.ErrNoResponders)
}

// Fetch implements ClaimStore. It never creates buckets — the fetch side
// (shim) typically has read-only permissions.
func (o *ObjectClaims) Fetch(ctx context.Context, tenant, id string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, claimOpTimeout)
	defer cancel()

	if !TokenSafe(tenant) || !TokenSafe(id) {
		return nil, fmt.Errorf("wire: invalid claim reference %q/%q", tenant, id)
	}
	obs, err := o.JS.ObjectStore(ctx, claimBucketPrefix+tenant)
	if err != nil {
		return nil, fmt.Errorf("wire: claim bucket: %w", err)
	}
	// Streamed under a limit rather than GetBytes'd: how much this process
	// will hold is its own call, never the producer's. claimMaxBody is
	// enforced gateway-side in streamWriter.End, but an end frame only carries
	// a reference — anything able to write the tenant bucket and reach the
	// caller's inbox could point it at an object of any size, and GetBytes
	// would io.ReadAll however much that was.
	r, err := obs.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("wire: claim fetch: %w", err)
	}
	defer func() { _ = r.Close() }()

	// The declared size saves downloading a body that is already too big, but
	// it is the producer's own number: the LimitReader is the real bound. Once
	// it has been checked against the cap it is also safe to size the buffer
	// with, which keeps the read from doubling its way up to 64MiB — the peak
	// allocation is the thing this whole path exists to bound.
	var buf bytes.Buffer
	if info, ierr := r.Info(); ierr == nil {
		if info.Size > claimMaxBody {
			return nil, fmt.Errorf("wire: claim of %d bytes exceeds the %d byte limit", info.Size, claimMaxBody)
		}
		buf.Grow(int(info.Size) + 1)
	}
	// Digest-verified by the client library: a full read ends at the object's
	// own EOF, inside the limit, which is where it checks the SHA-256.
	if _, err := buf.ReadFrom(io.LimitReader(r, claimMaxBody+1)); err != nil {
		return nil, fmt.Errorf("wire: claim fetch: %w", err)
	}
	body := buf.Bytes()
	if len(body) > claimMaxBody {
		return nil, fmt.Errorf("wire: claim exceeds the %d byte limit", claimMaxBody)
	}
	// Eager cleanup; TTL is the backstop when this is denied or we die here.
	if !o.noEagerDelete.Load() {
		dctx, dcancel := context.WithTimeout(ctx, claimEagerDeleteTimeout)
		err := obs.Delete(dctx, id)
		dcancel()
		// Latched only for a failure of OURS. The caller's context ending is
		// the ordinary shape of an MCP client that cancelled or went away
		// microseconds after its body was read, and it says nothing about
		// whether this identity may delete — reading it as a refusal would let
		// one impatient client disable eager cleanup for every tenant this
		// process serves, for the rest of its life.
		if err != nil && ctx.Err() == nil {
			o.noEagerDelete.Store(true)
		}
	}
	return body, nil
}

// Delete implements ClaimStore. It never creates a bucket: a claim can only
// exist in one that already does.
func (o *ObjectClaims) Delete(ctx context.Context, tenant, id string) error {
	ctx, cancel := context.WithTimeout(ctx, claimOpTimeout)
	defer cancel()

	if !TokenSafe(tenant) || !TokenSafe(id) {
		return fmt.Errorf("wire: invalid claim reference %q/%q", tenant, id)
	}
	obs, err := o.JS.ObjectStore(ctx, claimBucketPrefix+tenant)
	if err != nil {
		return fmt.Errorf("wire: claim bucket: %w", err)
	}
	if err := obs.Delete(ctx, id); err != nil {
		return fmt.Errorf("wire: claim delete: %w", err)
	}
	return nil
}

func (o *ObjectClaims) bucket(ctx context.Context, tenant string) (jetstream.ObjectStore, error) {
	o.mu.Lock()
	obs := o.buckets[tenant]
	o.mu.Unlock()
	if obs != nil {
		return obs, nil
	}
	return o.recreateBucket(ctx, tenant)
}

func (o *ObjectClaims) recreateBucket(ctx context.Context, tenant string) (jetstream.ObjectStore, error) {
	if !TokenSafe(tenant) {
		return nil, fmt.Errorf("wire: tenant %q is not token safe", tenant)
	}
	maxAge, maxBytes := FilledClaimLimits(o.MaxAge, o.MaxBytes)
	obs, err := o.JS.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      claimBucketPrefix + tenant,
		Description: "natsmcp claim-check overflow for tenant " + tenant,
		TTL:         maxAge,
		MaxBytes:    maxBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("wire: claim bucket for %q: %w", tenant, err)
	}
	o.mu.Lock()
	if o.buckets == nil {
		o.buckets = make(map[string]jetstream.ObjectStore)
	}
	o.buckets[tenant] = obs
	o.mu.Unlock()
	return obs, nil
}
