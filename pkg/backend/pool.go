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

package backend

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Key identifies one pooled connection. The tenant is in the key because
// tenants must never share a subprocess (credential isolation plus the
// unattributable-notification leak); the credential fields are in it so
// rotation drains old processes naturally.
type Key struct {
	Server      string
	Tenant      string
	CredSet     string
	CredVersion int
}

// PoolConfig bounds the pool.
type PoolConfig struct {
	// MaxConcurrent is the per-connection in-flight cap (default 32): one
	// caller cannot queue-bomb the rest of its tenant.
	MaxConcurrent int
	// MaxProcsPerTenant caps distinct live connections per tenant (default
	// 16): a tenant fan-out cannot OOM the gateway host.
	MaxProcsPerTenant int
	// IdleTTL reaps connections with no in-flight calls (default 5m).
	IdleTTL time.Duration
	// MaxLifetime recycles connections regardless of activity (default 1h),
	// bounding slow leaks in long-lived servers.
	MaxLifetime time.Duration
	// BreakerThreshold is how many consecutive Connect failures open the
	// circuit (default 3); BreakerCooldown is how long it stays open
	// (default 30s).
	BreakerThreshold int
	BreakerCooldown  time.Duration
}

// Filled returns c with every unset field replaced by the value the pool
// would substitute. Exported so a caller validating configuration can compare
// against what will actually be in force rather than against the zero the
// operator left behind.
func (c PoolConfig) Filled() PoolConfig { c.fill(); return c }

func (c *PoolConfig) fill() {
	if c.MaxConcurrent <= 0 {
		c.MaxConcurrent = 32
	}
	if c.MaxProcsPerTenant <= 0 {
		c.MaxProcsPerTenant = 16
	}
	if c.IdleTTL <= 0 {
		c.IdleTTL = 5 * time.Minute
	}
	if c.MaxLifetime <= 0 {
		c.MaxLifetime = time.Hour
	}
	if c.BreakerThreshold <= 0 {
		c.BreakerThreshold = 3
	}
	if c.BreakerCooldown <= 0 {
		c.BreakerCooldown = 30 * time.Second
	}
}

// Factory builds the Backend for a pool key (resolving command, env, and
// credentials for that server+tenant).
type Factory func(key Key) (Backend, error)

// Expiring is optionally implemented by a Backend whose injected credentials
// expire. The pool clamps that entry's lifetime to the expiry (minus a skew)
// so a backend never outlives its credentials — enforced both in Get and by
// the reaper. Caveat, by design: the reaper only takes idle entries, so a
// long-lived in-flight call (subscriptions/listen) can pin a backend past
// expiry; it fails on its next use and the caller re-issues.
type Expiring interface {
	CredExpiresAt() time.Time
}

// credExpirySkew is how long before credential expiry an entry is recycled,
// so the replacement spawns while the old credentials still work.
const credExpirySkew = 30 * time.Second

// Pool lazily creates and reuses one Mux per Key.
type Pool struct {
	cfg     PoolConfig
	factory Factory
	log     *slog.Logger

	mu      sync.Mutex
	entries map[Key]*entry
	broken  map[Key]*breaker
	// orphans are past-deadline entries replaced by Get while they still had
	// in-flight calls: the calls run to completion and the reaper closes the
	// mux once they drain. Closing immediately would kill live work for an
	// age-out — worse than letting it finish.
	orphans []*entry

	stop chan struct{}
	once sync.Once
}

type entry struct {
	// key is retained on the entry (not only as the map key) so orphaned
	// entries stay attributable: EvictServer must find a server's orphans
	// and tenantCountLocked must count them.
	key      Key
	mux      *Mux
	born     time.Time
	deadline time.Time // born + MaxLifetime, clamped to credential expiry
	lastUsed time.Time
	inflight int
	// waiters counts Gets blocked on the sem that have not yet incremented
	// inflight — live-work-in-waiting the close paths must not kill.
	waiters int
	sem     chan struct{}
}

// busyLocked reports whether the entry has live or imminent work (p.mu held).
func (e *entry) busyLocked() bool { return e.inflight > 0 || e.waiters > 0 }

type breaker struct {
	fails     int
	openUntil time.Time
}

// NewPool builds a pool and starts its reaper.
func NewPool(cfg PoolConfig, factory Factory, log *slog.Logger) *Pool {
	cfg.fill()
	if log == nil {
		log = slog.Default()
	}
	p := &Pool{
		cfg:     cfg,
		factory: factory,
		log:     log,
		entries: make(map[Key]*entry),
		broken:  make(map[Key]*breaker),
		stop:    make(chan struct{}),
	}
	go p.reapLoop()
	return p
}

// Get returns the live Mux for the key, creating it if needed, and reserves
// one concurrency slot. The caller MUST call release when its call finishes.
func (p *Pool) Get(ctx context.Context, key Key) (*Mux, func(), error) {
	p.mu.Lock()
	e := p.entries[key]
	// Past-deadline entries are replaced here, not only by the reaper: its
	// 30s tick must never hand a request credentials that expired between
	// ticks. An idle (or dead) entry is closed immediately; a busy one is
	// orphaned so its in-flight calls finish and the reaper collects it once
	// drained — an age-out must never kill live work.
	if e != nil && (e.mux.Dead() || time.Now().After(e.deadline)) {
		if e.mux.Dead() || !e.busyLocked() {
			go func(m *Mux) { _ = m.Close() }(e.mux)
		} else {
			p.orphans = append(p.orphans, e)
		}
		delete(p.entries, key)
		e = nil
	}
	if e == nil {
		if br := p.broken[key]; br != nil && time.Now().Before(br.openUntil) {
			p.mu.Unlock()
			return nil, nil, fmt.Errorf("backend: circuit open for %s/%s until %s (%d consecutive spawn failures)",
				key.Tenant, key.Server, br.openUntil.Format(time.RFC3339), br.fails)
		}
		if p.tenantCountLocked(key.Tenant) >= p.cfg.MaxProcsPerTenant {
			p.mu.Unlock()
			return nil, nil, fmt.Errorf("backend: tenant %q at max live backends (%d)",
				key.Tenant, p.cfg.MaxProcsPerTenant)
		}
		p.mu.Unlock()

		// Connect outside the lock: spawning can take seconds.
		b, err := p.factory(key)
		if err != nil {
			return nil, nil, err
		}
		conn, err := b.Connect(ctx)
		if err != nil {
			p.recordFailure(key)
			return nil, nil, err
		}
		p.mu.Lock()
		delete(p.broken, key)
		// Lost the race with another Get? Keep ours anyway under its key —
		// simplest correct behavior; the reaper collects extras.
		if cur := p.entries[key]; cur != nil && !cur.mux.Dead() {
			e = cur
			go func(c Conn) { _ = c.Close() }(conn)
		} else {
			born := time.Now()
			deadline := born.Add(p.cfg.MaxLifetime)
			if exp, ok := b.(Expiring); ok {
				if t := exp.CredExpiresAt(); !t.IsZero() {
					d := t.Add(-credExpirySkew)
					if d.Before(born) {
						// Credentials already inside the skew window at
						// spawn: a skewed deadline would be born-dead and
						// every Get would tear down and respawn. Live to
						// the literal expiry instead; the cost is the
						// documented in-flight -32010 at expiry.
						d = t
					}
					if d.Before(deadline) {
						deadline = d
					}
				}
			}
			e = &entry{
				key:      key,
				mux:      NewMux(conn, p.log.With("server", key.Server, "tenant", key.Tenant)),
				born:     born,
				deadline: deadline,
				sem:      make(chan struct{}, p.cfg.MaxConcurrent),
			}
			p.entries[key] = e
		}
	}
	e.lastUsed = time.Now()
	e.waiters++ // visible to the close paths while we block on the sem
	sem := e.sem
	mux := e.mux
	p.mu.Unlock()

	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		p.mu.Lock()
		e.waiters--
		p.mu.Unlock()
		return nil, nil, ctx.Err()
	}
	p.mu.Lock()
	e.waiters--
	e.inflight++
	p.mu.Unlock()

	release := func() {
		<-sem
		p.mu.Lock()
		e.inflight--
		e.lastUsed = time.Now()
		p.mu.Unlock()
	}
	return mux, release, nil
}

func (p *Pool) recordFailure(key Key) {
	p.mu.Lock()
	defer p.mu.Unlock()
	br := p.broken[key]
	if br == nil {
		br = &breaker{}
		p.broken[key] = br
	}
	br.fails++
	if br.fails >= p.cfg.BreakerThreshold {
		br.openUntil = time.Now().Add(p.cfg.BreakerCooldown)
		p.log.Warn("backend circuit opened", "server", key.Server, "tenant", key.Tenant,
			"fails", br.fails, "cooldown", p.cfg.BreakerCooldown)
	}
}

func (p *Pool) tenantCountLocked(tenant string) int {
	n := 0
	for k, e := range p.entries {
		if k.Tenant == tenant && !e.mux.Dead() {
			n++
		}
	}
	// Orphans are still live subprocesses; not counting them would let a
	// tenant exceed the OOM guard by exactly the backends it churned.
	for _, e := range p.orphans {
		if e.key.Tenant == tenant && !e.mux.Dead() {
			n++
		}
	}
	return n
}

func (p *Pool) reapLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			p.reap()
		}
	}
}

func (p *Pool) reap() {
	now := time.Now()
	var victims []*Mux
	p.mu.Lock()
	for k, e := range p.entries {
		dead := e.mux.Dead()
		idle := !e.busyLocked() && now.Sub(e.lastUsed) > p.cfg.IdleTTL
		old := !e.busyLocked() && now.After(e.deadline)
		if dead || idle || old {
			p.log.Info("reaping backend connection",
				"server", k.Server, "tenant", k.Tenant,
				"dead", dead, "idle", idle, "over_max_lifetime", old)
			victims = append(victims, e.mux)
			delete(p.entries, k)
		}
	}
	// Orphans (replaced while busy) are closed once their calls drain.
	kept := p.orphans[:0]
	for _, e := range p.orphans {
		if e.mux.Dead() || !e.busyLocked() {
			victims = append(victims, e.mux)
		} else {
			kept = append(kept, e)
		}
	}
	p.orphans = kept
	p.mu.Unlock()
	for _, m := range victims {
		_ = m.Close()
	}
}

// EvictServer closes every pooled connection for the named server (across all
// tenants) and clears its circuit-breaker state. Used on config reload when a
// server is removed or its definition changes: in-flight calls on those muxes
// fail with ErrConnDead (the proxy maps that to ErrCodeStreamLost, and the
// client re-issues), and the next Get spawns a fresh backend from the new
// definition.
func (p *Pool) EvictServer(server string) {
	p.mu.Lock()
	var victims []*Mux
	for k, e := range p.entries {
		if k.Server == server {
			victims = append(victims, e.mux)
			delete(p.entries, k)
		}
	}
	// Eviction deliberately kills busy work (the definition or credentials
	// changed underneath it) — that must include orphans, or a backend
	// replaced moments before the reload would keep serving the removed
	// server with its old credentials.
	kept := p.orphans[:0]
	for _, e := range p.orphans {
		if e.key.Server == server {
			victims = append(victims, e.mux)
		} else {
			kept = append(kept, e)
		}
	}
	p.orphans = kept
	for k := range p.broken {
		if k.Server == server {
			delete(p.broken, k)
		}
	}
	p.mu.Unlock()
	for _, m := range victims {
		_ = m.Close()
	}
}

// Shutdown closes every pooled connection.
func (p *Pool) Shutdown() {
	p.once.Do(func() { close(p.stop) })
	p.mu.Lock()
	victims := make([]*Mux, 0, len(p.entries)+len(p.orphans))
	for k, e := range p.entries {
		victims = append(victims, e.mux)
		delete(p.entries, k)
	}
	for _, e := range p.orphans {
		victims = append(victims, e.mux)
	}
	p.orphans = nil
	p.mu.Unlock()
	for _, m := range victims {
		_ = m.Close()
	}
}
