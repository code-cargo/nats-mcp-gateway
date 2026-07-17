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
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
)

// FetchFunc retrieves the current config from somewhere (HTTP, a KV store,
// Vault, a database …). It is called once at startup and then every poll
// interval.
type FetchFunc func(ctx context.Context) (*config.Config, error)

// Poll turns any FetchFunc into a Source that fetches every interval and emits
// only when the config actually changed (by content hash). This is the
// easiest way to add a new config backend: implement one fetch and you get
// change detection, the initial load, and lifecycle for free.
//
// The initial fetch is emitted regardless (even an error, so Run can treat a
// broken initial fetch as fatal). Later fetch errors are emitted as
// Update.Err — Run logs them and keeps the last good config.
func Poll(interval time.Duration, fetch FetchFunc) Source {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return SourceFunc(func(ctx context.Context) <-chan Update {
		out := make(chan Update)
		go func() {
			defer close(out)
			var lastHash string
			emit := func() {
				cfg, err := fetch(ctx)
				if err != nil {
					send(ctx, out, Update{Err: err})
					return
				}
				h := hashConfig(cfg)
				if h == lastHash {
					return // unchanged: stay quiet
				}
				lastHash = h
				send(ctx, out, Update{Config: cfg})
			}

			emit() // initial
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					emit()
				}
			}
		}()
		return out
	})
}

// Static returns a Source that emits cfg once and never changes — useful for
// embedding a fixed config or in tests.
func Static(cfg *config.Config) Source {
	return SourceFunc(func(ctx context.Context) <-chan Update {
		out := make(chan Update, 1)
		out <- Update{Config: cfg}
		close(out)
		return out
	})
}

// send delivers u unless ctx is done first.
func send(ctx context.Context, out chan<- Update, u Update) {
	select {
	case out <- u:
	case <-ctx.Done():
	}
}
