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

	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// Reconciler owns the currently-applied config and drives the wire + pool to
// match a new one.
type Reconciler struct {
	ws   *wire.Server
	pool *backend.Pool
	log  *slog.Logger

	cur atomic.Pointer[config.Config]

	// mu serializes Apply so concurrent reloads can't interleave a wire update
	// with a pool eviction.
	mu sync.Mutex

	// bootNATS is the `nats` block of the FIRST applied revision — the settings
	// the connection and wire server handed to New were built from; bootSeen
	// separates "nothing applied yet" from "applied an empty block", which is
	// what the fetch and inline sources always carry. Guarded by mu, like
	// everything else Apply reads and writes.
	bootNATS config.NATS
	bootSeen bool
}

// New builds a Reconciler over a running wire server and pool. The pool's
// factory should read the live config via Current.
func New(ws *wire.Server, pool *backend.Pool, log *slog.Logger) *Reconciler {
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
// The server set is all that reconciles. A revision whose `nats` block has
// moved away from the running gateway's is reported (see warnNATSDrift) and
// otherwise ignored — that connection cannot be rebuilt underneath a serving
// process.
func (r *Reconciler) Apply(next *config.Config) (Delta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.warnNATSDrift(next)

	prev := r.cur.Load()
	d := diffServers(prev, next)

	if d.Empty() {
		r.cur.Store(next)
		r.log.Debug("config unchanged")
		return d, nil
	}

	// Publish the new config BEFORE (re)registering endpoints: a newly-added
	// server's micro service starts answering inside SetServers, and its
	// backend factory resolves the definition through Current — which must
	// therefore already be the new config. Roll back on failure so a bad
	// revision leaves the previous config live.
	//
	// windowStart is the instant next becomes reachable through Current, and
	// so the earliest a backend could be built from it. The rollback below
	// needs it to tell next's backends from prev's.
	windowStart := time.Now()
	r.cur.Store(next)
	if err := r.ws.SetServers(next.ServerNames()); err != nil {
		r.cur.Store(prev)
		// next was briefly the live config, so a request racing this apply
		// could have pooled a backend built from a definition that is now
		// rolled back. Nothing downstream would ever collect it: the pool is
		// keyed by (server, tenant), and once prev is live again its
		// definition matches, so no later diff calls the server changed. Drop
		// what next could have spawned — for a changed server that costs a
		// respawn from prev, and for an added one it is the only chance to
		// notice at all. A removed server needs nothing: it is absent from
		// next, so the factory refused to build it to begin with.
		//
		// Only what was born in the window, though. prev is live again and its
		// definition never changed, so an older backend is serving exactly what
		// it should; evicting it would fail in-flight calls for a revision that
		// never touched them. A backend born in the window may have come from
		// either config — from prev if its Get read Current just before the
		// store — and dropping one of those costs a respawn, which is the
		// cheaper side of the trade.
		for _, name := range d.Changed {
			r.pool.EvictServerSince(name, windowStart)
		}
		for _, name := range d.Added {
			r.pool.EvictServerSince(name, windowStart)
		}
		// This narrows the window; it cannot close it. A Get that resolved
		// next and is still spawning when the loops above run inserts its
		// backend afterwards, and nothing collects that one. Closing it for
		// real means stamping entries with the revision they were built from
		// and rejecting stale inserts — the pool has no such notion today.
		return Delta{}, err
	}

	// Removed and changed servers drop their pooled backends: removed so
	// nothing lingers, changed so the next call spawns from the new definition
	// (e.g. rotated credentials).
	for _, name := range d.Removed {
		r.pool.EvictServer(name)
	}
	for _, name := range d.Changed {
		r.pool.EvictServer(name)
	}

	r.log.Info("config reloaded",
		"added", d.Added, "removed", d.Removed, "changed", d.Changed)
	return d, nil
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
// an already-diverged block look settled. That costs no repetition in practice,
// because sources emit on content change (see pkg/configsource), so this is one
// line per edit rather than one per poll.
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
