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
	"errors"
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
	// SpawnTimeout bounds one backend creation (default 60s). It is the
	// POOL's deadline and not the requesting caller's, which is the whole
	// point of it — see runSpawn.
	SpawnTimeout time.Duration
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
	// Longer than the legacy bridge's own 30s handshake timeout, so a stalled
	// initialize is reported by the layer that can say what stalled.
	if c.SpawnTimeout <= 0 {
		c.SpawnTimeout = 60 * time.Second
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
	// pending counts spawns a tenant has been admitted for but has not yet
	// inserted. The lock is dropped for the whole of Connect, so without this
	// the cap is only ever compared against backends that already finished
	// starting — and a burst wide enough to matter is entirely in flight by
	// then.
	pending map[string]int
	// spawning holds the in-flight creation for a key. Concurrent Gets attach
	// to it instead of each starting a subprocess and discarding all but one.
	spawning map[Key]*spawn
	// orphans are past-deadline entries replaced by Get while they still had
	// in-flight calls: the calls run to completion and the reaper closes the
	// mux once they drain. Closing immediately would kill live work for an
	// age-out — worse than letting it finish.
	orphans []*entry
	// closed is set by Shutdown, so a spawn still in flight when it lands
	// closes what it built instead of installing it into a drained pool.
	closed bool

	stop chan struct{}
	// spawnCtx is the lifetime every spawn runs under. Shutdown cancels it,
	// which is the only thing that may interrupt one.
	spawnCtx    context.Context
	spawnCancel context.CancelFunc
	once        sync.Once
}

// spawn is one in-flight backend creation, shared by every Get waiting for
// that key. e and err are written before done is closed and read only after
// receiving from it.
type spawn struct {
	done chan struct{}
	e    *entry
	err  error
	// stale is set (under p.mu) when EvictServer retires the key while this
	// spawn runs: what it produces was built from the superseded definition.
	stale bool
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
		cfg:      cfg,
		factory:  factory,
		log:      log,
		entries:  make(map[Key]*entry),
		broken:   make(map[Key]*breaker),
		pending:  make(map[string]int),
		spawning: make(map[Key]*spawn),
		stop:     make(chan struct{}),
	}
	p.spawnCtx, p.spawnCancel = context.WithCancel(context.Background())
	go p.reapLoop()
	return p
}

// errEntryRetired means the entry a Get had settled on stopped being the one
// to use while that Get was queued for a call slot. Internal to Get's retry;
// it never reaches a caller.
var errEntryRetired = errors.New("backend: pooled entry retired while queued")

// errPoolClosed ends a spawn whose pool shut down while it was starting.
var errPoolClosed = errors.New("backend: pool is shut down")

// getMaxAttempts bounds that retry. Each attempt spawns at most one backend,
// so a pathological case — credentials already expired the moment they are
// resolved — must not become a spawn-and-discard loop; the caller is told to
// re-issue instead.
const getMaxAttempts = 3

// Get returns the live Mux for the key, creating it if needed, and reserves
// one concurrency slot. The caller MUST call release when its call finishes.
func (p *Pool) Get(ctx context.Context, key Key) (*Mux, func(), error) {
	for range getMaxAttempts {
		mux, release, err := p.get(ctx, key)
		if !errors.Is(err, errEntryRetired) {
			return mux, release, err
		}
	}
	return nil, nil, fmt.Errorf("backend: %s/%s could not be given a live backend to run on, re-issue the request",
		key.Tenant, key.Server)
}

// get is one attempt at Get, reporting errEntryRetired when the caller should
// start over.
func (p *Pool) get(ctx context.Context, key Key) (*Mux, func(), error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, errPoolClosed
	}
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
		// Retire what this generation supersedes BEFORE the cap is measured.
		// Those entries can never be dispatched to again, so counting them
		// counts backends that exist only until the reaper notices. Measured
		// with them still in it, a tenant already at MaxProcsPerTenant cannot
		// rotate a credential at all: the new generation is a new key, the
		// predecessor it is replacing still occupies the cap, and every
		// request for that server is refused until the IdleTTL reaps an entry
		// nothing could have reached anyway.
		for _, m := range p.retireSupersededLocked(key) {
			// Off the request path: closing a stdio backend escalates through
			// SIGTERM to SIGKILL and can take seconds, and the caller waiting
			// on this Get has nothing to do with the credentials that rotated.
			go func(m *Mux) { _ = m.Close() }(m)
		}
		if br := p.broken[key]; br != nil && time.Now().Before(br.openUntil) {
			p.mu.Unlock()
			return nil, nil, fmt.Errorf("backend: circuit open for %s/%s until %s (%d consecutive spawn failures)",
				key.Tenant, key.Server, br.openUntil.Format(time.RFC3339), br.fails)
		}
		sp := p.spawning[key]
		if sp == nil {
			if p.tenantCountLocked(key.Tenant) >= p.cfg.MaxProcsPerTenant {
				p.mu.Unlock()
				return nil, nil, fmt.Errorf("backend: tenant %q at max live backends (%d)",
					key.Tenant, p.cfg.MaxProcsPerTenant)
			}
			// Claim the slot while we still hold the lock. The spawn below is
			// the resource the cap is about, and it begins here, not when the
			// entry lands in the map.
			p.pending[key.Tenant]++
			sp = &spawn{done: make(chan struct{})}
			p.spawning[key] = sp
			go p.runSpawn(key, sp)
		}
		p.mu.Unlock()

		select {
		case <-sp.done:
		case <-ctx.Done():
			// Leaving is this caller's business alone. The spawn belongs to
			// the pool and runs on, because a legacy backend handshakes
			// inside Connect and a cold `npx` server routinely takes longer
			// to come up than one client will wait: tearing the subprocess
			// down on the way out would make the next request start that same
			// cold spawn from zero, and a server slower to boot than its
			// clients are patient would never finish coming up at all.
			return nil, nil, ctx.Err()
		}
		if sp.err != nil {
			return nil, nil, sp.err
		}
		p.mu.Lock()
		// The spawn installed this entry, but a Get that waited on it can wake
		// arbitrarily later — long enough for EvictServer, the reaper or a
		// credential rotation to have retired it. Same re-check, for the same
		// reason, as the one after the sem wait below.
		if e = sp.e; p.entries[key] != e {
			p.mu.Unlock()
			return nil, nil, errEntryRetired
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
	// The entry was sampled before the wait, and the wait is unbounded: a full
	// semaphore holds a Get until other calls finish. Everything checked at
	// lookup has to be checked again here, or waiting becomes the way a NEW
	// call starts on a backend past its credential deadline — or on one
	// EvictServer replaced, or one that died meanwhile.
	if p.entries[key] != e || e.mux.Dead() || time.Now().After(e.deadline) {
		p.mu.Unlock()
		<-sem // hand the slot to whoever is behind us; we are not using it
		return nil, nil, errEntryRetired
	}
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

// runSpawn creates one backend for a key and wakes every Get waiting on it.
//
// The context is the POOL's, deliberately, and that is the whole reason this
// runs off the request path. A spawn bound to the caller that happened to
// trigger it dies when that caller loses patience, and since nothing is
// retained the next request starts the same cold subprocess from zero — so a
// backend slower to boot than its clients are patient never comes up, however
// many times it is asked for. The same binding makes the breaker unreadable:
// a caller that gave up and a backend that cannot start both surface as a
// cancelled Connect, so counting cancellations opens the circuit on
// impatience while not counting them leaves a genuinely hung backend
// uncounted. A deadline the pool owns separates the two — no caller's
// cancellation reaches Connect, and every failure that arrives here is the
// backend's.
func (p *Pool) runSpawn(key Key, sp *spawn) {
	ctx, cancel := context.WithTimeout(p.spawnCtx, p.cfg.SpawnTimeout)
	defer cancel()

	e, err := p.spawnEntry(ctx, key, sp)

	// Written before done is closed, so every waiter's receive sees finished
	// values; unmapped under the lock, so a Get arriving a moment too late
	// starts a fresh spawn instead of attaching to this finished one.
	sp.e, sp.err = e, err
	p.mu.Lock()
	if p.spawning[key] == sp {
		delete(p.spawning, key)
	}
	p.mu.Unlock()
	close(sp.done)
}

// spawnEntry resolves the backend for a key, connects it, and installs the
// pooled entry.
func (p *Pool) spawnEntry(ctx context.Context, key Key, sp *spawn) (*entry, error) {
	b, err := p.factory(key)
	if err != nil {
		p.releasePending(key.Tenant)
		return nil, err
	}
	conn, err := b.Connect(ctx)
	if err != nil {
		// Unconditional: ctx is the pool's, so nothing in this count is a
		// caller's impatience — every failure reaching here is the backend
		// failing to start, which is exactly what the breaker is for.
		p.recordFailure(key)
		p.releasePending(key.Tenant)
		return nil, err
	}

	p.mu.Lock()
	// The entry counts for itself from here, whichever branch below wins.
	p.releasePendingLocked(key.Tenant)
	// Shutdown drained the pool, or a reload retired this server, while the
	// subprocess was still starting: what came up was built from a definition
	// nobody wants installed now.
	if p.closed || sp.stale {
		closed := p.closed
		p.mu.Unlock()
		go func() { _ = conn.Close() }()
		if closed {
			return nil, errPoolClosed
		}
		return nil, errEntryRetired
	}
	delete(p.broken, key)
	e := p.entries[key]
	if e != nil && !e.mux.Dead() {
		// Lost the race with a spawn that overlapped ours (evict-and-respawn
		// can start a second one under this key). Adopt the entry that landed
		// first and close the connection we just opened. Keeping both would
		// put a second subprocess under one key with nothing to reach it — the
		// cap counts it, the reaper collects it, and the tenant pays for it in
		// between.
		go func() { _ = conn.Close() }()
	} else {
		born := time.Now()
		deadline := born.Add(p.cfg.MaxLifetime)
		if exp, ok := b.(Expiring); ok {
			if t := exp.CredExpiresAt(); !t.IsZero() {
				d := t.Add(-credExpirySkew)
				if d.Before(born) {
					// Credentials already inside the skew window at spawn: a
					// skewed deadline would be born-dead and every Get would
					// tear down and respawn. Live to the literal expiry
					// instead; the cost is the documented in-flight -32010 at
					// expiry.
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
		// No retireSupersededLocked here: Get does it on the way in, before
		// the cap is measured, because entries this generation supersedes must
		// not be counted against a tenant that is trying to rotate. Retiring
		// again here would be redundant, and doing it ONLY here would put the
		// retirement back after the measurement it has to precede.
	}
	p.mu.Unlock()
	return e, nil
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

// retireSupersededLocked unmaps the idle entries a newly-keyed credential
// generation supersedes, returning their muxes to close (p.mu held). Nothing
// will ever be dispatched to them again: the proxy resolves before it keys,
// so every later request for this (server, tenant, credset) carries at least
// this generation. Waiting for the IdleTTL to notice would leave them
// counting against MaxProcsPerTenant for five minutes — which is how a
// backend that rejects one caller's credentials on every request becomes a
// refusal for every OTHER server the tenant has.
//
// Busy entries are left alone: their in-flight calls were authorized under
// the credentials they hold, and the reaper collects them once drained.
func (p *Pool) retireSupersededLocked(key Key) []*Mux {
	var victims []*Mux
	for k, e := range p.entries {
		if k.Server != key.Server || k.Tenant != key.Tenant || k.CredSet != key.CredSet {
			continue
		}
		// Generations are globally monotonic, so "older" is exactly "lower" —
		// and a Get that raced in with a stale generation must not be read as
		// superseding the newer entry already serving.
		if k.CredVersion >= key.CredVersion || e.busyLocked() {
			continue
		}
		victims = append(victims, e.mux)
		delete(p.entries, k)
	}
	return victims
}

func (p *Pool) releasePending(tenant string) {
	p.mu.Lock()
	p.releasePendingLocked(tenant)
	p.mu.Unlock()
}

func (p *Pool) releasePendingLocked(tenant string) {
	if n := p.pending[tenant] - 1; n > 0 {
		p.pending[tenant] = n
	} else {
		delete(p.pending, tenant)
	}
}

// closeAll closes every mux, concurrently. One close is slow by design — a
// stdio backend escalates through SIGTERM to SIGKILL, an http conn waits out
// the exchanges still on its wire — so closing serially charges that grace
// once per backend to whoever is collecting them: a reaper tick, a config
// reload's eviction, or the whole of Shutdown.
func closeAll(muxes []*Mux) {
	var wg sync.WaitGroup
	for _, m := range muxes {
		wg.Add(1)
		go func(m *Mux) { defer wg.Done(); _ = m.Close() }(m)
	}
	wg.Wait()
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
	// Reserved slots are subprocesses being spawned right now; not counting
	// them lets a burst of concurrent Gets on distinct keys all pass the check
	// and all spawn, overshooting the cap by the width of the burst.
	return n + p.pending[tenant]
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
	closeAll(victims)
}

// EvictServer closes every pooled connection for the named server (across all
// tenants) and clears its circuit-breaker state. Used on config reload when a
// server is removed or its definition changes: in-flight calls on those muxes
// fail with ErrConnDead (the proxy maps that to ErrCodeStreamLost, and the
// client re-issues), and the next Get spawns a fresh backend from the new
// definition.
func (p *Pool) EvictServer(server string) {
	p.evictServer(server, time.Time{})
}

// EvictServerSince is EvictServer restricted to connections spawned after t.
//
// It is for a revision that was rolled back, where the server's live definition
// is the one it already had. Only a backend born inside the failed apply's
// window could have been built from the revision being undone; an older one is
// provably from the definition that is live again, and killing it would fail
// in-flight calls that the rollback is supposed to leave alone.
//
// Circuit-breaker state is cleared for the whole server either way: it carries
// no birth time to filter on, and clearing it only ever permits an earlier
// respawn attempt.
func (p *Pool) EvictServerSince(server string, t time.Time) {
	p.evictServer(server, t)
}

// evictServer drops every connection for the server born after since. The zero
// time matches everything, which is what EvictServer wants.
func (p *Pool) evictServer(server string, since time.Time) {
	p.mu.Lock()
	var victims []*Mux
	for k, e := range p.entries {
		if k.Server == server && e.born.After(since) {
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
		if e.key.Server == server && e.born.After(since) {
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
	// A spawn already running for this server captured the OLD definition.
	// Marking it stale makes it close what it produces rather than install
	// it, so the Get waiting behind it retries onto the new definition.
	for k, sp := range p.spawning {
		if k.Server == server {
			sp.stale = true
		}
	}
	p.mu.Unlock()
	closeAll(victims)
}

// Shutdown closes every pooled connection.
func (p *Pool) Shutdown() {
	// Cancelling spawnCtx is what interrupts a backend still starting; closed
	// is what stops one that finishes anyway from installing itself into a
	// pool that has already been drained.
	p.once.Do(func() {
		close(p.stop)
		p.spawnCancel()
	})
	p.mu.Lock()
	p.closed = true
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
	closeAll(victims)
}
