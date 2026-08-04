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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"

	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
)

// changeFilter is the content-hash change detection every built-in source
// shares: it suppresses a revision whose content matches the last one that
// Watch emitted.
//
// One per Watch, never one per source. Watch promises its consumer that the
// first Update is the initial config, and a baseline shared between two Watches
// hands the second one whatever the first had already emitted — so a source
// watched twice tells its second consumer nothing until the config next
// changes. See filterSet, which is what a source holds instead.
//
// The baseline is what the Watch last EMITTED, which is not the same thing as
// what the gateway is running — so Run tells it when an emitted revision failed
// to apply. Without that, a single failed apply would record content the
// gateway never adopted, and every later redelivery of that same content —
// poll tick, change event, --config-refetch, SIGHUP — would be dropped inside
// the source. The gateway would then serve the previous config indefinitely
// with nothing but one log line to say so, and the periodic re-fetch that
// exists as the missed-event safety net would never bring it back.
type changeFilter struct {
	mu sync.Mutex
	// last is the revision the gateway accepted; seen says whether there is
	// one. They are separate because every string value last can hold is a
	// hash a real revision might produce, so no in-band value is free to mean
	// "nothing yet" — and Retry needs to say exactly that.
	last string
	seen bool
	// swallowed records that changed() suppressed a revision since the last
	// one it let through. Run applies on its own goroutine, so a trigger can
	// fire and be suppressed while the revision it would have re-delivered is
	// still failing to apply; by the time Retry drops the baseline that
	// trigger is already spent. retry reports it so a source with no other
	// trigger can fire itself once. See File.Retry.
	swallowed bool
}

// changed reports whether cfg differs from the last emitted revision, adopting
// it as the new baseline when it does.
func (f *changeFilter) changed(cfg *config.Config) bool {
	h, ok := hashConfig(cfg)
	f.mu.Lock()
	defer f.mu.Unlock()
	if ok && f.seen && h == f.last {
		f.swallowed = true
		return false
	}
	f.last, f.seen, f.swallowed = h, ok, false
	return true
}

// retry drops the baseline, returning the filter to the state it had before its
// first emission so identical content is emitted again on the source's next
// trigger. It reports whether a trigger was suppressed in the meantime and so
// needs re-firing.
//
// Run calls this from its own goroutine, so a retry landing beside an
// in-progress emit can cost one redundant emission — a no-op apply, against a
// suppressed retry that strands the gateway.
func (f *changeFilter) retry() (refire bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = false
	refire, f.swallowed = f.swallowed, false
	return refire
}

// filterSet is what a built-in source embeds: the change filters of the Watches
// that are live on it right now.
//
// The split is forced by where each half belongs. A baseline belongs to a
// Watch, because Watch owes its consumer an initial config and a shared
// baseline swallows the second one. Retry belongs to the SOURCE, because
// Retryable is a method set on the source and Run holds nothing finer-grained
// to call it on. So a retry re-arms every live Watch: one whose consumer did
// not fail re-emits content that consumer already has, which costs it a no-op
// apply — against a retry suppressed at the one Watch that did fail, which
// strands a gateway on a config it never adopted.
type filterSet struct {
	mu      sync.Mutex
	filters map[*changeFilter]struct{}
}

// attach returns one Watch's filter and the func that unregisters it. Call it
// in Watch itself rather than in the goroutine, so a Retry arriving the instant
// Watch returns cannot land before the filter exists.
func (s *filterSet) attach() (*changeFilter, func()) {
	f := &changeFilter{}
	s.mu.Lock()
	if s.filters == nil {
		s.filters = make(map[*changeFilter]struct{})
	}
	s.filters[f] = struct{}{}
	s.mu.Unlock()
	return f, func() {
		s.mu.Lock()
		delete(s.filters, f)
		s.mu.Unlock()
	}
}

// retry drops every live Watch's baseline, reporting whether any of them had a
// trigger suppressed in the meantime and so needs one fired in its place.
func (s *filterSet) retry() (refire bool) {
	s.mu.Lock()
	live := make([]*changeFilter, 0, len(s.filters))
	for f := range s.filters {
		live = append(live, f)
	}
	s.mu.Unlock()
	for _, f := range live {
		if f.retry() {
			refire = true
		}
	}
	return refire
}

// Retry implements Retryable. A source whose trigger repeats on its own — a
// ticker — recovers a suppressed trigger on the next tick and needs nothing
// more; one that does not must override this. See File.Retry.
func (s *filterSet) Retry() { s.retry() }

// Dedup wraps a Source so identical consecutive configs are suppressed by
// content hash. Push-based sources (an event fires, re-fetch, emit) can over-
// emit; wrapping in Dedup means the reconciler only sees real changes. Errors
// pass through unchanged. (Poll already dedups internally; Dedup is for
// sources that don't.)
func Dedup(src Source) Source {
	return &dedupSource{src: src}
}

type dedupSource struct {
	src Source
	filterSet
}

// Watch implements Source.
func (d *dedupSource) Watch(ctx context.Context) <-chan Update {
	in := d.src.Watch(ctx)
	out := make(chan Update)
	filter, release := d.attach()
	go func() {
		defer close(out)
		defer release()
		for {
			select {
			case <-ctx.Done():
				return
			case u, ok := <-in:
				if !ok {
					return
				}
				if u.Err == nil && !filter.changed(u.Config) {
					continue
				}
				send(ctx, out, u)
			}
		}
	}()
	return out
}

// Retry implements Retryable, forwarding to the wrapped source as well: it may
// dedup on its own, and a baseline cleared on only one of the two layers still
// swallows the retry at the other.
func (d *dedupSource) Retry() {
	d.filterSet.Retry()
	if r, ok := d.src.(Retryable); ok {
		r.Retry()
	}
}

// hashConfig identifies a revision by content. json.Marshal sorts map keys, so
// the same logical config always hashes identically.
//
// A nil config is hashed rather than special-cased. It is a legitimate revision
// — "every server removed" — and giving it a real hash keeps every in-band
// value distinct from the filter's "nothing accepted yet" state, which is what
// the bool is for. json.Marshal renders it as null.
//
// That bool is false for a revision that cannot be identified at all, which is
// not the same as one that hashes to something unusual: an unhashable revision
// has no value that could be compared against a later one, so it must never be
// suppressed. No config.Config field can fail to marshal today, so this is
// purely how a future one that can degrades — noisily, not silently.
func hashConfig(cfg *config.Config) (string, bool) {
	b, err := json.Marshal(cfg)
	if err != nil {
		return "", false
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), true
}
