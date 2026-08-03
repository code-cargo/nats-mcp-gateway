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
	"fmt"
	"log/slog"

	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
)

// Run consumes a Source and applies each config revision, returning when ctx
// is cancelled or the source's channel closes.
//
// The last good config always keeps serving:
//   - The FIRST update carrying an error is fatal (there is nothing to serve
//     yet) — Run returns it.
//   - After the gateway is serving, a later source error or a failed apply is
//     logged and dropped; the previously-applied config stays live. A failed
//     apply also stays RETRYABLE: Run reports it back to a Retryable source so
//     the revision is re-delivered on the source's next trigger instead of
//     being suppressed as content already seen.
//
// apply is typically reconcile.(*Reconciler).Apply wrapped to drop its Delta.
func Run(ctx context.Context, log *slog.Logger, src Source, apply func(*config.Config) error) error {
	if log == nil {
		log = slog.Default()
	}
	ch := src.Watch(ctx)
	serving := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case u, ok := <-ch:
			if !ok {
				return nil // source closed (usually because ctx was cancelled)
			}
			if u.Err != nil {
				if !serving {
					return fmt.Errorf("configsource: initial config: %w", u.Err)
				}
				log.Error("config source error; keeping last good config", "err", u.Err)
				continue
			}
			if err := apply(u.Config); err != nil {
				if !serving {
					return fmt.Errorf("configsource: applying initial config: %w", err)
				}
				// Not "keeping last good config" on its own: that is true of
				// the wire and says nothing about the backend pools, which the
				// apply may well have touched — and an operator watching this
				// line repeat every tick needs to know whether their traffic is
				// paying for it. What it did is on the apply's own line.
				log.Error("applying config failed; the previous config keeps serving "+
					"and this revision will be retried — see the apply's own line for what it did to the backend pools",
					"err", err)
				// A source that dedups by content has already recorded this
				// revision as delivered. Tell it otherwise, or the next
				// redelivery of the same content — the poll tick, the change
				// event, the operator's SIGHUP — is suppressed upstream of
				// here and the failure becomes permanent for as long as the
				// config keeps saying the same thing.
				if r, ok := src.(Retryable); ok {
					r.Retry()
				}
				continue
			}
			serving = true
		}
	}
}
