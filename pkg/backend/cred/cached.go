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
	jitter     time.Duration
	fails      int
	lastErr    error
	retryAt    time.Time
	lastUsed   time.Time // guarded by CachedResolver.mu, not entry.mu
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
	e := c.entry(tenant, user, server)
	e.mu.Lock()

	now := time.Now()
	if e.creds != nil && (e.creds.ExpiresAt.IsZero() || now.Before(e.creds.ExpiresAt)) {
		// Valid. Kick one background refresh once inside the lead window
		// (unless a recent failure's backoff says wait).
		if !e.creds.ExpiresAt.IsZero() && now.After(e.refreshAt) && !e.refreshing && now.After(e.retryAt) {
			e.refreshing = true
			go c.refreshAhead(e, tenant, user, server)
		}
		creds, gen := e.creds, e.gen
		e.mu.Unlock()
		return creds, gen, nil
	}

	// Absent or expired: resolve under e.mu so concurrent misses
	// single-flight behind the first caller.
	if now.Before(e.retryAt) {
		err := e.lastErr
		e.mu.Unlock()
		return nil, 0, err
	}
	creds, err := c.inner.Resolve(ctx, tenant, user, server)
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
	c.store(e, creds, now)
	creds, gen := e.creds, e.gen
	e.mu.Unlock()
	return creds, gen, nil
}

// refreshAhead renews one entry's credentials in the background, resolving
// WITHOUT holding e.mu so valid-credential callers never block behind it.
func (c *CachedResolver) refreshAhead(e *cacheEntry, tenant, user, server string) {
	ctx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer cancel()
	creds, err := c.inner.Resolve(ctx, tenant, user, server)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.refreshing = false
	if err != nil {
		// The old credentials keep serving until their real expiry; the
		// backoff throttles further refresh attempts.
		e.fails++
		e.lastErr = err
		e.retryAt = time.Now().Add(failBackoff(e.fails))
		return
	}
	c.store(e, creds, time.Now())
}

// store records freshly-resolved credentials (e.mu held). The generation
// advances only when the injectable material changed; the refresh-ahead
// deadline is clamped so short-TTL credentials still cache for at least half
// their life instead of resolving on every request.
func (c *CachedResolver) store(e *cacheEntry, creds *Credentials, now time.Time) {
	if creds == nil {
		creds = &Credentials{}
	}
	if e.creds == nil || !maps.Equal(e.creds.Headers, creds.Headers) || !maps.Equal(e.creds.Env, creds.Env) {
		e.gen = int(globalGen.Add(1))
	}
	e.creds = creds
	e.fails, e.lastErr, e.retryAt = 0, nil, time.Time{}
	if !creds.ExpiresAt.IsZero() {
		lead := c.skew + e.jitter
		if ttl := creds.ExpiresAt.Sub(now); lead > ttl/2 {
			lead = ttl / 2
		}
		e.refreshAt = creds.ExpiresAt.Add(-lead)
	}
}

// Invalidate drops the cached credentials for one key so the next Resolve
// refetches immediately (any failure backoff is cleared too). Used when a
// backend rejects the credentials mid-life (HTTP 401).
func (c *CachedResolver) Invalidate(tenant, user, server string) {
	c.mu.Lock()
	e := c.entries[cacheKey{tenant, user, server}]
	c.mu.Unlock()
	if e == nil {
		return
	}
	e.mu.Lock()
	e.creds = nil
	e.retryAt = time.Time{}
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
