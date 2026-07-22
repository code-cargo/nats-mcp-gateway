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
// closes the returned channel. The first Update is the initial config. A
// source owns its own trigger (file poll, NATS event, ticker, …) and its own
// goroutine lifecycle bound to ctx.
type Source interface {
	Watch(ctx context.Context) <-chan Update
}

// SourceFunc adapts a plain function to a Source.
type SourceFunc func(ctx context.Context) <-chan Update

// Watch implements Source.
func (f SourceFunc) Watch(ctx context.Context) <-chan Update { return f(ctx) }
