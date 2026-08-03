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

// Package reconcile applies a config revision to a running gateway: it diffs
// the server set, updates the wire's per-server micro services, and evicts the
// backend pools for servers that were removed or changed. It is
// source-agnostic — drive Apply from any config source (see pkg/configsource)
// or from an embedder's own control loop.
package reconcile

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// Pool is the part of *backend.Pool that reconciliation drives. Narrow because
// eviction is the only thing an apply does to the pool, and a test that has to
// observe exactly which evictions a revision decided on cannot get that from a
// live pool. *backend.Pool satisfies it.
type Pool interface {
	EvictServer(server string)
	EvictServerSince(server string, t time.Time)
}

// Reconciler owns the currently-applied config and drives the wire + pool to
// match a new one.
type Reconciler struct {
	ws   *wire.Server
	pool Pool
	log  *slog.Logger

	cur atomic.Pointer[config.Config]

	// mu serializes an apply's state change — the wire update and the publish
	// through cur — so concurrent reloads can't interleave them.
	mu sync.Mutex

	// evictMu orders the pool work two applies decide on. It is taken under mu
	// and released once that work has run, so the eviction itself runs with mu
	// free: closing a backend waits out its terminate grace, and an apply that
	// paid that under mu would hand its cost to the next reload.
	evictMu sync.Mutex

	// bootNATS is the `nats` block of the FIRST applied revision — the settings
	// the connection and wire server handed to New were built from; bootSeen
	// separates "nothing applied yet" from "applied an empty block", which is
	// what the fetch and inline sources always carry. Guarded by mu, like
	// everything else Apply reads and writes.
	bootNATS config.NATS
	bootSeen bool

	// failed identifies the revision whose apply failed most recently, by
	// content — a source hands out a fresh pointer per delivery, so identity
	// says nothing. failedSeen separates "none has failed" from a revision that
	// could not be identified. Any adopted revision clears both.
	failed     string
	failedSeen bool
}

// New builds a Reconciler over a running wire server and pool. The pool's
// factory should read the live config via Current.
func New(ws *wire.Server, pool Pool, log *slog.Logger) *Reconciler {
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{ws: ws, pool: pool, log: log}
}

// Current returns the last successfully-applied config, or nil before the
// first Apply. The pool factory calls this to resolve a server's definition at
// spawn time, so an evicted server always respawns from the newest config.
//
// Only the server set is authoritative here. The `nats` block is what the
// newest revision SAID, not what this process is running — that is fixed at
// boot, and a revision moving it is warned about and otherwise ignored (see
// warnNATSDrift). Read the running connection and scope from the objects the
// gateway was built with, never from here.
func (r *Reconciler) Current() *config.Config {
	return r.cur.Load()
}

// Delta is the set of server names that changed between two configs.
type Delta struct {
	Added   []string
	Removed []string
	Changed []string
}

// Empty reports whether nothing changed.
func (d Delta) Empty() bool {
	return len(d.Added) == 0 && len(d.Removed) == 0 && len(d.Changed) == 0
}

// Apply reconciles the gateway to next. A revision the wire refuses leaves the
// previous config live and undoes whatever the attempt could have started —
// but only that, so a bad revision can neither leave the gateway half-updated
// nor disturb work the previous config was already serving. On success it
// evicts the pools of removed and changed servers and publishes the new config
// as current.
//
// A nil next is a revision like any other: "every server removed".
//
// The server set is all that reconciles. A revision whose `nats` block has
// moved away from the running gateway's is reported (see warnNATSDrift) and
// otherwise ignored — that connection cannot be rebuilt underneath a serving
// process.
func (r *Reconciler) Apply(next *config.Config) (Delta, error) {
	d, ev, err := r.transition(next)
	// The pool work runs here, with mu released and evictMu still held from
	// inside transition. An eviction closes subprocesses and waits out the
	// terminate grace; that is not a cost to hold the apply lock across, but
	// two applies' pool work still has to run in apply order.
	defer r.evictMu.Unlock()
	r.evict(ev)
	return d, err
}

// eviction is the pool work one apply decided on, deferred out of the critical
// section that decided it.
type eviction struct {
	servers []string
	// since bounds the eviction to backends born after it. The zero time
	// matches every backend, which is what an adopted revision wants; a
	// rollback sets the window in which its rejected revision was reachable.
	since time.Time
	// live is the config the rollback restored, and the one that must still be
	// current for a windowed eviction to mean anything. Nil is a real value —
	// the first apply has no predecessor — so since is what says whether the
	// check applies.
	live *config.Config
}

// evict runs the pool work, with mu released.
func (r *Reconciler) evict(ev eviction) {
	// A windowed eviction stands down if another apply published while this one
	// waited for evictMu. since bounds it from BELOW only, so measured against
	// a newer live config it would drop backends that config legitimately owns
	// — the in-flight kill the bound exists to prevent. The apply that
	// published evicted whatever its own diff called changed; the rest is
	// serving what it should.
	if !ev.since.IsZero() && r.cur.Load() != ev.live {
		return
	}
	for _, name := range ev.servers {
		if ev.since.IsZero() {
			r.pool.EvictServer(name)
			continue
		}
		r.pool.EvictServerSince(name, ev.since)
	}
}

// transition is the half of Apply that has to be serialized against another
// apply: the diff, the wire update and the publish through cur, and nothing
// that waits on a subprocess. It returns with evictMu held, for Apply to
// release once the pool work it returned has run.
func (r *Reconciler) transition(next *config.Config) (Delta, eviction, error) {
	r.mu.Lock()
	defer func() {
		// Taken under mu and released by Apply once the pool work has run: the
		// order two applies decide their evictions in is the order those
		// evictions run in, while the next apply's state change goes ahead
		// rather than waiting out this one's subprocesses.
		r.evictMu.Lock()
		r.mu.Unlock()
	}()

	// A nil revision is a legitimate one — "every server removed" — and the
	// package doc advertises a fetch returning (nil, nil) as the way to write
	// one. Substituting an empty config covers every reader at once: this is
	// the value Current publishes, and the pool factory dereferences that.
	// warnNATSDrift still sees the original, because nil asserts nothing about
	// the `nats` block where an empty document asserts an empty one.
	rev := next
	if rev == nil {
		rev = &config.Config{}
	}

	prev := r.cur.Load()
	d := diffServers(prev, rev)

	if d.Empty() {
		r.adopt(rev, next)
		r.log.Debug("config unchanged")
		return d, eviction{}, nil
	}

	// Publish the new config BEFORE (re)registering endpoints: a newly-added
	// server's micro service starts answering inside SetServers, and its
	// backend factory resolves the definition through Current — which must
	// therefore already be the new config. Roll back on failure so a bad
	// revision leaves the previous config live.
	//
	// windowStart is the instant rev becomes reachable through Current, and
	// so the earliest a backend could be built from it. The rollback below
	// needs it to tell rev's backends from prev's.
	//
	// A revision that already failed its last apply is published nowhere. Run
	// re-delivers a failed revision on the source's next trigger, so a config
	// the wire refuses arrives again every tick for as long as the operator
	// leaves it in place, and every publish opens another window in which a
	// request can pool a backend the rollback then has to kill. The first
	// attempt is worth that: the revision is expected to apply. A repeat is
	// not, and the cost if it does apply is one moment between SetServers
	// binding an added server and the store below, in which that server
	// resolves out of the previous config — an error the caller retries,
	// against backend churn on every tick until the operator fixes the file.
	windowStart := time.Now()
	publish := !r.failedBefore(rev)
	if publish {
		r.cur.Store(rev)
	}
	if err := r.ws.SetServers(rev.ServerNames()); err != nil {
		r.recordFailed(rev)
		if !publish {
			// Nothing became reachable, so there is nothing to undo.
			return Delta{}, eviction{}, err
		}
		r.cur.Store(prev)
		// rev was briefly the live config, so a request racing this apply
		// could have pooled a backend built from a definition that is now
		// rolled back. Nothing downstream would ever collect it: the pool is
		// keyed by (server, tenant), and once prev is live again its
		// definition matches, so no later diff calls the server changed. Drop
		// what rev could have spawned — for a changed server that costs a
		// respawn from prev, and for an added one it is the only chance to
		// notice at all. A removed server needs nothing: it is absent from
		// rev, so the factory refused to build it to begin with.
		//
		// Only what was born in the window, though. prev is live again and its
		// definition never changed, so an older backend is serving exactly what
		// it should; evicting it would fail in-flight calls for a revision that
		// never touched them. A backend born in the window may have come from
		// either config — from prev if its Get read Current just before the
		// store — and dropping one of those costs a respawn, which is the
		// cheaper side of the trade.
		//
		// This narrows the window; it cannot close it. A Get that resolved rev
		// and is still spawning when the eviction runs inserts its backend
		// afterwards, and nothing collects that one. Closing it for real means
		// stamping entries with the revision they were built from and rejecting
		// stale inserts — the pool has no such notion today.
		ev := eviction{
			servers: concat(d.Changed, d.Added),
			since:   windowStart,
			live:    prev,
		}
		// Named separately from the caller's own failure line, which knows only
		// that the apply returned an error: "the last good config keeps
		// serving" is the whole truth about the wire and none of it about the
		// pools, and a revision that reached them is exactly the case an
		// operator reading a repeating failure has to be able to rule out.
		r.log.Warn("config revision refused and rolled back; the previous config is live again, "+
			"and any backend the revision managed to start is dropped",
			"servers", ev.servers, "err", err)
		return Delta{}, ev, err
	}
	r.adopt(rev, next)

	// Removed and changed servers drop their pooled backends: removed so
	// nothing lingers, changed so the next call spawns from the new definition
	// (e.g. rotated credentials).
	ev := eviction{servers: concat(d.Removed, d.Changed)}

	r.log.Info("config reloaded",
		"added", d.Added, "removed", d.Removed, "changed", d.Changed)
	return d, ev, nil
}

// adopt makes rev the live config and takes the bookkeeping every revision the
// gateway accepts needs. next is the caller's own pointer, which warnNATSDrift
// needs in order to tell a nil revision from an empty one.
func (r *Reconciler) adopt(rev, next *config.Config) {
	r.cur.Store(rev)
	r.failed, r.failedSeen = "", false
	r.warnNATSDrift(next)
}

// failedBefore reports whether rev is the revision whose apply failed last.
func (r *Reconciler) failedBefore(rev *config.Config) bool {
	key, ok := revisionKey(rev)
	return ok && r.failedSeen && key == r.failed
}

// recordFailed remembers a revision the wire refused. One that cannot be
// marshalled is never recognised as a repeat: there is no value that could
// stand for a revision nothing can identify, and treating an unidentifiable one
// as "the same again" would suppress a publish that has to happen.
func (r *Reconciler) recordFailed(rev *config.Config) {
	r.failed, r.failedSeen = revisionKey(rev)
}

// revisionKey identifies a revision by content, for the reason serverEqual
// marshals: a field-by-field comparison would rot as config.Config grows.
func revisionKey(c *config.Config) (string, bool) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", false
	}
	return string(b), true
}

func concat(a, b []string) []string {
	if len(a)+len(b) == 0 {
		return nil
	}
	out := make([]string, 0, len(a)+len(b))
	return append(append(out, a...), b...)
}

// warnNATSDrift reports a `nats` block that has moved away from the one the
// running gateway was built on. Apply cannot act on it: New is handed a live
// connection and an already-serving wire server, so the URL, credentials,
// subject prefixes and the tenant/user scope are fixed for the process's life.
//
// Silence is the hazard. The diff covers servers only, so an operator who edits
// nats.tenant in the config file and reloads gets "config unchanged" from a
// gateway that is still serving every tenant. The inline and fetch sources
// refuse such a document outright; the file source cannot, because the servers
// in that same document have to keep reloading — so it reports instead.
//
// Measured against the FIRST applied revision rather than the predecessor: the
// divergence is from what is RUNNING, and a later unrelated edit must not make
// an already-diverged block look settled.
//
// Called only for a revision the gateway ADOPTS, which is what keeps it to one
// line per edit rather than one per poll. Sources emit on content change, so an
// adopted revision arrives once; a REFUSED one arrives every tick, because Run
// hands it back to the source to re-deliver (see configsource.Retryable), and
// warning from the top of an apply would report the same unapplied block for as
// long as the operator left it in the file. The same call site is what makes
// the boot block the first block the gateway actually ran on, rather than
// whatever the first attempt happened to say.
func (r *Reconciler) warnNATSDrift(next *config.Config) {
	if next == nil {
		return // a nil revision is "every server removed"; it asserts nothing
	}
	if !r.bootSeen {
		r.bootNATS, r.bootSeen = next.NATS, true
		return
	}
	if next.NATS == r.bootNATS {
		return
	}
	// Field names, never values: nats.url carries its password in the userinfo
	// form, and this warning goes wherever the gateway's logs go.
	r.log.Warn("the nats block changed but is fixed at boot; the live connection and wire scope are unchanged",
		"fields", natsDriftFields(r.bootNATS, next.NATS),
		"fix", "restart the gateway to apply it")
}

// natsDriftFields names the fields that differ, by their JSON names. Marshalled
// rather than compared field by field for the reason serverEqual gives: a
// hand-written list would rot as config.NATS grows, and this covers a field the
// day it is added.
func natsDriftFields(boot, next config.NATS) []string {
	return driftFields(natsFields(boot), natsFields(next))
}

// driftFields compares two marshalled field maps over the UNION of their keys.
// config.NATS is all plain-tagged strings today, so both sides always carry
// every key — but a field tagged omitempty would drop out of whichever side it
// is empty on, and iterating the boot side alone would then miss it in one
// direction only. That direction is the dangerous one: booted with no tenant,
// reloaded with one set is exactly the narrowing edit this warning exists to
// name, and it would report an empty list.
func driftFields(boot, next map[string]json.RawMessage) []string {
	out := make([]string, 0, len(boot))
	for name, bv := range boot {
		if !bytes.Equal(bv, next[name]) {
			out = append(out, name)
		}
	}
	for name := range next {
		if _, ok := boot[name]; !ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func natsFields(n config.NATS) map[string]json.RawMessage {
	b, err := json.Marshal(n)
	if err != nil {
		return nil // unreachable for a struct of strings; the warning still fires
	}
	var m map[string]json.RawMessage
	_ = json.Unmarshal(b, &m)
	return m
}

// diffServers compares two configs by server name and definition. A nil old
// config (the first apply) makes every server "added". Two servers with the
// same name but differing definitions (command, args, env, url, headers,
// protocol, …) are "changed".
func diffServers(old, next *config.Config) Delta {
	var oldServers, newServers map[string]config.Server
	if old != nil {
		oldServers = old.Servers
	}
	if next != nil {
		newServers = next.Servers
	}

	var d Delta
	for name, ns := range newServers {
		os, ok := oldServers[name]
		switch {
		case !ok:
			d.Added = append(d.Added, name)
		case !serverEqual(os, ns):
			d.Changed = append(d.Changed, name)
		}
	}
	for name := range oldServers {
		if _, ok := newServers[name]; !ok {
			d.Removed = append(d.Removed, name)
		}
	}

	sort.Strings(d.Added)
	sort.Strings(d.Removed)
	sort.Strings(d.Changed)
	return d
}

// serverEqual reports whether two server definitions are identical. JSON
// marshalling gives a stable comparison over the maps and slices without
// hand-writing a field-by-field equality that would rot as config.Server
// grows.
func serverEqual(a, b config.Server) bool {
	ab, err1 := json.Marshal(a)
	bb, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return false // treat unmarshalable as changed; never silently equal
	}
	return string(ab) == string(bb)
}
