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

// CachedResolver wraps a Resolver with the caching every mode needs, so no
// resolver author reimplements it: TTL cache keyed (tenant, user, server),
// single-flight per key, failure backoff, and a monotonic generation counter.
// The generation becomes the pool key's CredVersion — it is resolver-global,
// not per-key, so a swept-and-recreated entry can never repeat a generation
// and alias a stale pooled backend.
type CachedResolver struct {
	inner Resolver
	skew  time.Duration

	gen atomic.Int64

	mu        sync.Mutex
	entries   map[cacheKey]*cacheEntry
	lastSweep time.Time
}

type cacheKey struct{ tenant, user, server string }

type cacheEntry struct {
	// mu is held across an inner Resolve: concurrent misses on one key
	// single-flight behind it (and pick up the winner's result), while other
	// keys proceed independently.
	mu       sync.Mutex
	creds    *Credentials
	gen      int
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
// value the proxy threads into the pool key so a refreshed credential yields
// a new pool entry and the old backend drains.
func (c *CachedResolver) ResolveGen(ctx context.Context, tenant, user, server string) (*Credentials, int, error) {
	e := c.entry(tenant, user, server)
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	if e.creds != nil && (e.creds.ExpiresAt.IsZero() || now.Before(e.creds.ExpiresAt.Add(-c.skew-e.jitter))) {
		return e.creds, e.gen, nil
	}
	if now.Before(e.retryAt) {
		return nil, 0, e.lastErr
	}

	creds, err := c.inner.Resolve(ctx, tenant, user, server)
	if err != nil {
		e.fails++
		e.lastErr = err
		e.retryAt = now.Add(failBackoff(e.fails))
		return nil, 0, err
	}
	if creds == nil {
		creds = &Credentials{}
	}
	e.creds = creds
	e.gen = int(c.gen.Add(1))
	e.fails, e.lastErr, e.retryAt = 0, nil, time.Time{}
	return e.creds, e.gen, nil
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
