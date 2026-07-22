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

// File is a Source that reads a config file, polling for changes and reloading
// on demand via Reload (wire that to SIGHUP in your main; the library never
// installs signal handlers itself). It emits only when the file's parsed
// content changes.
type File struct {
	path         string
	pollInterval time.Duration
	reload       chan struct{}
}

// NewFile builds a file source. pollInterval <= 0 disables polling (reload
// then comes only from Reload / the initial read); the default when > 0 is the
// caller's value.
func NewFile(path string, pollInterval time.Duration) *File {
	return &File{
		path:         path,
		pollInterval: pollInterval,
		reload:       make(chan struct{}, 1),
	}
}

// Reload asks the source to re-read the file now. Non-blocking and coalescing:
// several rapid calls collapse into one reload. Safe to call from a signal
// handler.
func (f *File) Reload() {
	select {
	case f.reload <- struct{}{}:
	default:
	}
}

// Watch implements Source.
func (f *File) Watch(ctx context.Context) <-chan Update {
	out := make(chan Update)
	go func() {
		defer close(out)
		var lastHash string
		emit := func() {
			cfg, err := config.Load(f.path)
			if err != nil {
				send(ctx, out, Update{Err: err})
				return
			}
			h := hashConfig(cfg)
			if h == lastHash {
				return
			}
			lastHash = h
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
			case <-f.reload:
				emit()
			case <-tick:
				emit()
			}
		}
	}()
	return out
}
