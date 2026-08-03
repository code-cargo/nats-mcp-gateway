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

// Package configsource streams gateway configuration over time so the server
// set can be reloaded without a restart. The whole extension surface is one
// interface:
//
//	type Source interface { Watch(ctx) <-chan Update }
//
// To reload from a new place you have three options, cheapest first:
//
//  1. Use a built-in: File or a NATS source (see nats.go).
//  2. Wrap a fetch function with Poll — for HTTP, Vault, a KV store, S3, etc.:
//     configsource.Poll(30*time.Second, func(ctx) (*config.Config, error) { ... })
//  3. Implement Source directly — for push systems (a Kubernetes informer, a
//     webhook) that already know when config changed.
//
// Run consumes a Source and applies each revision, keeping the last good
// config live when a revision fails to parse or apply.
package configsource

import (
	"context"

	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
)

// Update is one config revision, or a non-fatal error the source encountered
// while producing one. Exactly one of Config / Err is set.
type Update struct {
	Config *config.Config
	Err    error
}

// Source streams config revisions until ctx is cancelled, at which point it
// closes the returned channel. The first Update is the initial config — for
// every Watch, including a second one on a source that has been watched
// before. A source owns its own trigger (file poll, NATS event, ticker, …) and
// its own goroutine lifecycle bound to ctx.
//
// A source that dedups keeps its change-detection baseline per Watch, so a
// second consumer starts fresh rather than inheriting what the first had
// already emitted. Retry has no such split — it is a method on the source, and
// Run has nothing finer to call it on — so it re-arms every live Watch. See
// filterSet.
type Source interface {
	Watch(ctx context.Context) <-chan Update
}

// Retryable is a Source that suppresses revisions it has already delivered and
// can be told that one of them did not stick. Run calls Retry after a failed
// apply, so the same content is delivered again on the source's next trigger
// rather than being mistaken for a revision the gateway is already running.
//
// Implement it on any source that dedups; every built-in one does. A source
// that emits everything it sees needs nothing here.
//
// Retry must also account for a trigger spent while the apply was failing: the
// source is free to fire again the moment Run takes a revision off the channel,
// so a trigger can arrive, find unchanged content, and be suppressed before
// Retry is ever called. A source with a repeating trigger recovers on its next
// tick. One without — File with polling off, where SIGHUP is the only trigger —
// must re-fire itself, or the retry waits on an operator who has no way to know
// their signal was eaten.
type Retryable interface {
	Retry()
}

// SourceFunc adapts a plain function to a Source.
type SourceFunc func(ctx context.Context) <-chan Update

// Watch implements Source.
func (f SourceFunc) Watch(ctx context.Context) <-chan Update { return f(ctx) }
