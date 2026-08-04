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

package configsource

import (
	"context"
	"sync"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
)

// File is a Source that reads a config file, polling for changes and reloading
// on demand via Reload (wire that to SIGHUP in your main; the library never
// installs signal handlers itself). It emits only when the file's parsed
// content changes.
type File struct {
	path         string
	pollInterval time.Duration
	filterSet

	// mu guards watchers: one trigger channel per live Watch, because Reload
	// has to reach ALL of them. A single shared channel delivers one HUP to
	// whichever goroutine happens to receive it, so a second Watch simply
	// never re-reads — and Retry is worse, since the replacement trigger it
	// fires for the Watch whose apply failed can be taken by a Watch that was
	// fine, stranding the first on a revision it never adopted. Poll and NATS
	// build their triggers per Watch already; this is File catching up.
	mu       sync.Mutex
	watchers map[chan struct{}]struct{}
}

// NewFile builds a file source. pollInterval <= 0 disables polling (reload
// then comes only from Reload / the initial read); the default when > 0 is the
// caller's value.
func NewFile(path string, pollInterval time.Duration) *File {
	return &File{path: path, pollInterval: pollInterval}
}

// Reload asks the source to re-read the file now. Non-blocking and coalescing:
// several rapid calls collapse into one reload. Safe to call from a signal
// handler.
func (f *File) Reload() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for w := range f.watchers {
		// Coalescing per watcher, not across them: a watcher already holding
		// an unread trigger will re-read once, which is what several rapid
		// HUPs ask for anyway.
		select {
		case w <- struct{}{}:
		default:
		}
	}
}

// addWatcher registers one Watch's trigger channel and returns its remover.
// Called in Watch rather than in the goroutine, so a Reload arriving the
// instant Watch returns cannot land before the channel exists.
func (f *File) addWatcher() (chan struct{}, func()) {
	w := make(chan struct{}, 1)
	f.mu.Lock()
	if f.watchers == nil {
		f.watchers = make(map[chan struct{}]struct{})
	}
	f.watchers[w] = struct{}{}
	f.mu.Unlock()
	return w, func() {
		f.mu.Lock()
		delete(f.watchers, w)
		f.mu.Unlock()
	}
}

// Retry implements Retryable, re-arming the trigger when one was spent on the
// failed revision.
//
// With pollInterval <= 0 a HUP is the only thing that re-reads the file, and
// Run applies on its own goroutine: a HUP sent while the failing apply is still
// running re-reads, finds content the filter has not yet been told to
// re-deliver, and is swallowed. Clearing the baseline afterwards fixes nothing
// — that HUP is gone, and the next one may be a long way off. So fire one
// ourselves in its place.
//
// This cannot spin. The reload it fires emits (the baseline is clear), which
// clears the swallowed flag, so a second failure re-arms nothing unless another
// trigger was genuinely lost.
func (f *File) Retry() {
	if f.filterSet.retry() {
		f.Reload()
	}
}

// Watch implements Source.
func (f *File) Watch(ctx context.Context) <-chan Update {
	out := make(chan Update)
	filter, release := f.attach()
	reload, unwatch := f.addWatcher()
	go func() {
		defer close(out)
		defer release()
		defer unwatch()
		emit := func() {
			cfg, err := config.Load(f.path)
			if err != nil {
				send(ctx, out, Update{Err: err})
				return
			}
			if !filter.changed(cfg) {
				return
			}
			send(ctx, out, Update{Config: cfg})
		}

		emit() // initial

		var tick <-chan time.Time
		if f.pollInterval > 0 {
			ticker := time.NewTicker(f.pollInterval)
			defer ticker.Stop()
			tick = ticker.C
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-reload:
				emit()
			case <-tick:
				emit()
			}
		}
	}()
	return out
}
