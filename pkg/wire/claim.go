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
	"sync"
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
}

const (
	// claimBucketPrefix names the per-tenant Object Store buckets. One bucket
	// per tenant so NATS permissions fence claims exactly like request
	// subjects; within a tenant, secrecy rests on the unguessable id and the
	// privacy of the reply inbox it rides.
	claimBucketPrefix = "MCP_CLAIMS_"

	// claimOpTimeout bounds one store operation regardless of caller ctx.
	claimOpTimeout = 30 * time.Second
)

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
	// MaxAge is the bucket TTL (default 5m). Gateway-side only.
	MaxAge time.Duration
	// MaxBytes caps each tenant bucket (default 1GiB). Gateway-side only.
	MaxBytes int64

	mu      sync.Mutex
	buckets map[string]jetstream.ObjectStore // tenant -> handle (Put side)
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
	body, err := obs.GetBytes(ctx, id) // digest-verified by the client library
	if err != nil {
		return nil, fmt.Errorf("wire: claim fetch: %w", err)
	}
	// Eager cleanup; TTL is the backstop when this is denied or we die here.
	_ = obs.Delete(ctx, id)
	return body, nil
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
	maxAge := o.MaxAge
	if maxAge <= 0 {
		maxAge = 5 * time.Minute
	}
	maxBytes := o.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 30 // 1GiB
	}
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
