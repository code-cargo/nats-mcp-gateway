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
	"encoding/json"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

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

// Apply reconciles the gateway to next. It updates the wire services first; if
// that fails the previous config stays live (nothing is swapped and no pools
// are evicted), so a bad revision can never leave the gateway half-updated.
// On success it evicts the pools of removed and changed servers and publishes
// the new config as current.
func (r *Reconciler) Apply(next *config.Config) (Delta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

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
	r.cur.Store(next)
	if err := r.ws.SetServers(next.ServerNames()); err != nil {
		r.cur.Store(prev)
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
