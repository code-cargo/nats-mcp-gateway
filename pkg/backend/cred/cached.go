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
	"maps"
	rand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultSkew is how long before ExpiresAt a refresh happens, so
	// credentials are renewed while the old ones still work. A per-entry
	// random jitter up to half the skew is added so a fleet of entries
	// minted together does not refresh in one stampede.
	DefaultSkew = 30 * time.Second

	// failBackoffBase/Cap bound the negative cache: a failed resolve is
	// memoized and retried no sooner than the backoff. Resolution happens on
	// the request path OUTSIDE the pool's circuit breaker (the breaker only
	// counts Connect failures), so this is the guard that keeps a resolver
	// outage from being hammered at request rate.
	failBackoffBase = 1 * time.Second
	failBackoffCap  = 30 * time.Second

	// sweepEvery/idleEvict bound the cache's memory: entries untouched for
	// idleEvict are dropped during a sweep piggybacked on lookups.
	sweepEvery = 5 * time.Minute
	idleEvict  = 30 * time.Minute
)

// globalGen issues credential generations for EVERY CachedResolver in the
// process. Package-global on purpose: the gateway rebuilds a server's
// resolver when its auth config changes, and a per-resolver counter would
// restart at 1 — colliding with pool keys minted by the previous resolver
// and aliasing stale backends. A process-wide monotonic counter can never
// repeat.
var globalGen atomic.Int64

// refreshTimeout bounds one background refresh-ahead resolve (the inner
// resolvers carry their own tighter timeouts).
const refreshTimeout = 45 * time.Second

// CachedResolver wraps a Resolver with the caching every mode needs, so no
// resolver author reimplements it: TTL cache keyed (tenant, user, server),
// single-flight per key, failure backoff, refresh-ahead, and a monotonic
// generation counter (the pool key's CredVersion). The generation advances
// only when the credential MATERIAL changes — a refresh that returns
// identical headers/env keeps its generation, so steadily-renewed identical
// credentials don't churn pooled backends.
type CachedResolver struct {
	inner Resolver
	skew  time.Duration

	mu        sync.Mutex
	entries   map[cacheKey]*cacheEntry
	lastSweep time.Time
}

type cacheKey struct{ tenant, user, server string }

type cacheEntry struct {
	// mu is held across an inner Resolve only when the credentials are
	// absent or actually expired: those callers single-flight behind it.
	// Inside the refresh-ahead window, callers are served the still-valid
	// credentials immediately and one background goroutine refreshes
	// without holding mu across its I/O.
	mu         sync.Mutex
	creds      *Credentials
	gen        int
	refreshAt  time.Time // when refresh-ahead should begin
	refreshing bool
	// refreshDone is closed when the refresh-ahead in flight finishes. A
	// caller whose credentials expired under a slow refresh waits on it
	// instead of resolving alongside it: one source, one conversation.
	refreshDone chan struct{}
	// gone holds what Invalidate dropped, kept for one comparison. Without it
	// the next resolve sees no previous material and advances the generation
	// unconditionally — so a backend that keeps rejecting a credential the
	// source keeps re-issuing gets a brand-new pool key, and a brand-new
	// backend, on every request.
	gone *Credentials
	// epoch counts every mutation of creds (store or Invalidate). A
	// background refresh captures it at launch and discards its result if
	// the entry moved on — otherwise a slow refresh completing after a
	// foreground re-resolve (or a 401 Invalidate) would overwrite fresher
	// credentials with staler ones, last-writer-wins.
	epoch    uint64
	jitter   time.Duration
	fails    int
	lastErr  error
	retryAt  time.Time
	lastUsed time.Time // guarded by CachedResolver.mu, not entry.mu
}

// Cached wraps inner. skew <= 0 uses DefaultSkew.
func Cached(inner Resolver, skew time.Duration) *CachedResolver {
	if skew <= 0 {
		skew = DefaultSkew
	}
	return &CachedResolver{
		inner:   inner,
		skew:    skew,
		entries: make(map[cacheKey]*cacheEntry),
	}
}

// Resolve implements Resolver.
func (c *CachedResolver) Resolve(ctx context.Context, tenant, user, server string) (*Credentials, error) {
	creds, _, err := c.ResolveGen(ctx, tenant, user, server)
	return creds, err
}

// ResolveGen resolves and also returns the credentials' generation — the
// value the proxy threads into the pool key so a CHANGED credential yields a
// new pool entry and the old backend drains. Still-valid credentials are
// served immediately; inside the refresh-ahead window a single background
// refresh runs so callers never block on renewal they don't need.
func (c *CachedResolver) ResolveGen(ctx context.Context, tenant, user, server string) (*Credentials, int, error) {
	for {
		e := c.entry(tenant, user, server)
		e.mu.Lock()

		now := time.Now()
		if e.creds != nil && (e.creds.ExpiresAt.IsZero() || now.Before(e.creds.ExpiresAt)) {
			// Valid. Kick one background refresh once inside the lead window
			// (unless a recent failure's backoff says wait).
			if !e.creds.ExpiresAt.IsZero() && now.After(e.refreshAt) && !e.refreshing && now.After(e.retryAt) {
				e.refreshing = true
				e.refreshDone = make(chan struct{})
				go c.refreshAhead(e, e.epoch, tenant, user, server)
			}
			creds, gen := e.creds, e.gen
			e.mu.Unlock()
			return creds, gen, nil
		}

		// Absent or expired. A refresh slower than the lead window is still
		// running against this key's source, and it is resolving exactly what
		// this caller needs — so wait for it rather than opening a second
		// conversation with that source. The expired path already funnels
		// every foreground caller through one inner.Resolve; the refresh was
		// the one resolve exempt from it, and being exempt is what let two
		// refresh_token grants present the same token and get the user's whole
		// grant revoked.
		if e.refreshing {
			done := e.refreshDone
			e.mu.Unlock()
			select {
			case <-done:
				continue // it stored credentials, or recorded why it could not
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			}
		}

		// Resolve under e.mu so concurrent misses single-flight behind the
		// first caller.
		if now.Before(e.retryAt) {
			err := e.lastErr
			e.mu.Unlock()
			return nil, 0, err
		}
		creds, err := c.inner.Resolve(ctx, tenant, user, server)
		if err == nil {
			err = c.store(e, creds, now)
		}
		if err != nil {
			// A failure caused by the CALLER (its context cancelled or timed
			// out mid-resolve) says nothing about the credential source — do
			// not memoize it, or one impatient client poisons the key for
			// everyone else within the backoff window.
			if ctx.Err() == nil {
				e.fails++
				e.lastErr = err
				e.retryAt = now.Add(failBackoff(e.fails))
			}
			e.mu.Unlock()
			return nil, 0, err
		}
		creds, gen := e.creds, e.gen
		e.mu.Unlock()
		return creds, gen, nil
	}
}

// refreshAhead renews one entry's credentials in the background, resolving
// WITHOUT holding e.mu so valid-credential callers never block behind it.
// startEpoch pins the entry state this refresh was launched against: if the
// entry mutated meanwhile (foreground re-resolve, Invalidate), this result
// is stale and is discarded.
func (c *CachedResolver) refreshAhead(e *cacheEntry, startEpoch uint64, tenant, user, server string) {
	ctx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer cancel()
	creds, err := c.inner.Resolve(ctx, tenant, user, server)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.refreshing = false
	// Released before the epoch check, so a caller parked on this refresh is
	// woken by a superseded one too — it re-examines the entry either way.
	close(e.refreshDone)
	e.refreshDone = nil
	if e.epoch != startEpoch {
		return // superseded while we were resolving; newer state wins
	}
	if err == nil {
		err = c.store(e, creds, time.Now())
	}
	if err != nil {
		// The old credentials keep serving until their real expiry; the
		// backoff throttles further refresh attempts.
		e.fails++
		e.lastErr = err
		e.retryAt = time.Now().Add(failBackoff(e.fails))
	}
}

// store records freshly-resolved credentials (e.mu held), rejecting material
// that is already expired — a stale source (late file rotator, skewed
// controller) must hit the failure backoff, not reset it and be re-resolved
// at request rate. The generation advances only when the injectable material
// changed; the refresh-ahead deadline is clamped so short-TTL credentials
// still cache for at least half their life.
func (c *CachedResolver) store(e *cacheEntry, creds *Credentials, now time.Time) error {
	if creds == nil {
		creds = &Credentials{}
	}
	if !creds.ExpiresAt.IsZero() && !creds.ExpiresAt.After(now) {
		return fmt.Errorf("cred: source returned credentials already expired at %s", creds.ExpiresAt.Format(time.RFC3339))
	}
	// Two questions, two different predecessors. Whether the MATERIAL moved is
	// asked of whatever this entry last held, Invalidate's dropped copy
	// included. Whether a REFRESH was productive is asked only of credentials
	// the entry is still serving: an invalidated entry is not being refreshed,
	// it is being refetched, and reading that refetch as an unproductive
	// refresh would re-arm at a fixed skew cadence for the whole remaining life
	// of a credential that has not even reached its lead window.
	prev := e.creds
	material := prev
	if material == nil {
		material = e.gone
	}
	if material == nil || !maps.Equal(material.Headers, creds.Headers) || !maps.Equal(material.Env, creds.Env) {
		e.gen = int(globalGen.Add(1))
	}
	e.creds, e.gone = creds, nil
	e.epoch++
	e.fails, e.lastErr, e.retryAt = 0, nil, time.Time{}
	if !creds.ExpiresAt.IsZero() {
		lead := c.skew + e.jitter
		if ttl := creds.ExpiresAt.Sub(now); lead > ttl/2 {
			lead = ttl / 2
		}
		e.refreshAt = creds.ExpiresAt.Add(-lead)
		// An unproductive refresh (ExpiresAt did not advance) must not
		// re-arm relative to the shrinking remaining life — that halves the
		// interval every round into an accelerating burst against a source
		// that is just serving the same credential. Hold a fixed cadence
		// instead.
		if prev != nil && !creds.ExpiresAt.After(prev.ExpiresAt) {
			e.refreshAt = now.Add(c.skew)
		}
	}
	return nil
}

// Invalidate drops the cached credentials for one key so the next Resolve
// refetches immediately (any failure backoff is cleared too). Used when a
// backend rejects the credentials mid-life (HTTP 401). The dropped material
// is remembered, unused, until the next resolve decides whether the
// generation moved.
func (c *CachedResolver) Invalidate(tenant, user, server string) {
	c.mu.Lock()
	e := c.entries[cacheKey{tenant, user, server}]
	c.mu.Unlock()
	if e == nil {
		return
	}
	e.mu.Lock()
	if e.creds != nil {
		e.gone = e.creds
	}
	e.creds = nil
	e.retryAt = time.Time{}
	e.epoch++ // any in-flight refresh launched before this is now stale
	e.mu.Unlock()
}

func (c *CachedResolver) entry(tenant, user, server string) *cacheEntry {
	k := cacheKey{tenant, user, server}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if now.Sub(c.lastSweep) > sweepEvery {
		c.lastSweep = now
		for key, e := range c.entries {
			if key != k && now.Sub(e.lastUsed) > idleEvict {
				delete(c.entries, key)
			}
		}
	}
	e := c.entries[k]
	if e == nil {
		e = &cacheEntry{jitter: rand.N(c.skew/2 + 1)}
		c.entries[k] = e
	}
	e.lastUsed = now
	return e
}

func failBackoff(fails int) time.Duration {
	d := failBackoffBase << (fails - 1)
	if d > failBackoffCap || d <= 0 {
		return failBackoffCap
	}
	return d
}
