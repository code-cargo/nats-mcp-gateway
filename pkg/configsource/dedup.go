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

	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
)

// Dedup wraps a Source so identical consecutive configs are suppressed by
// content hash. Push-based sources (an event fires, re-fetch, emit) can over-
// emit; wrapping in Dedup means the reconciler only sees real changes. Errors
// pass through unchanged. (Poll already dedups internally; Dedup is for
// sources that don't.)
func Dedup(src Source) Source {
	return SourceFunc(func(ctx context.Context) <-chan Update {
		in := src.Watch(ctx)
		out := make(chan Update)
		go func() {
			defer close(out)
			var lastHash string
			for {
				select {
				case <-ctx.Done():
					return
				case u, ok := <-in:
					if !ok {
						return
					}
					if u.Err == nil {
						h := hashConfig(u.Config)
						if h == lastHash {
							continue
						}
						lastHash = h
					}
					send(ctx, out, u)
				}
			}
		}()
		return out
	})
}

// hashConfig returns a stable content hash of a config. json.Marshal sorts map
// keys, so the same logical config always hashes identically.
func hashConfig(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return "" // unhashable: treat as "changed" so we never wrongly suppress
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
