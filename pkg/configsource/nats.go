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
	"time"

	nats "github.com/nats-io/nats.go"

	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
)

// NATS is a Source that fetches config over NATS request/reply and re-fetches
// whenever a change event is published. The config's source of truth stays in
// the controller — secrets ride only the authenticated NATS connection and are
// never at rest in a ConfigMap or KV bucket.
//
// Controller-side contract (the responder the platform provides):
//   - Respond to RequestSubject with the config JSON that config.Parse
//     accepts (the same schema the file source loads).
//   - Publish any message to EventSubject when the config changes.
//
// NATS permissions fence the control plane: only the controller's user may
// serve RequestSubject and publish EventSubject.
type NATS struct {
	// Conn is the NATS connection (the gateway's own).
	Conn *nats.Conn
	// RequestSubject is where the full config JSON is fetched from.
	RequestSubject string
	// EventSubject, if set, triggers a re-fetch on any message.
	EventSubject string
	// Refetch is the periodic re-fetch interval — the safety net for a change
	// event missed on the at-most-once bus. Default 60s.
	Refetch time.Duration
	// RequestTimeout bounds one fetch. Default 5s.
	RequestTimeout time.Duration
	// BootTimeout bounds how long Watch retries the initial fetch before
	// giving up (fatal — the pod crashes and the orchestrator surfaces it).
	// Default 2m; a transient controller restart fits well inside it.
	BootTimeout time.Duration
	// Logger is optional; boot retries log at warn.
	Logger *slog.Logger
}

func (s *NATS) requestTimeout() time.Duration {
	if s.RequestTimeout > 0 {
		return s.RequestTimeout
	}
	return 5 * time.Second
}

func (s *NATS) log() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// fetch performs one request/reply and parses the reply.
func (s *NATS) fetch(ctx context.Context) (*config.Config, error) {
	ctx, cancel := context.WithTimeout(ctx, s.requestTimeout())
	defer cancel()
	msg, err := s.Conn.RequestWithContext(ctx, s.RequestSubject, nil)
	if err != nil {
		return nil, fmt.Errorf("config request to %q: %w", s.RequestSubject, err)
	}
	if svcErr := msg.Header.Get("Nats-Service-Error"); svcErr != "" {
		return nil, fmt.Errorf("config responder error: %s", svcErr)
	}
	cfg, err := config.Parse(msg.Data)
	if err != nil {
		return nil, fmt.Errorf("config from %q: %w", s.RequestSubject, err)
	}
	return cfg, nil
}

// Watch implements Source.
func (s *NATS) Watch(ctx context.Context) <-chan Update {
	out := make(chan Update)
	go func() {
		defer close(out)

		// Subscribe to change events BEFORE the initial fetch so a change
		// racing startup can't be missed; events just coalesce into a pending
		// re-fetch signal.
		events := make(chan struct{}, 1)
		if s.EventSubject != "" {
			sub, err := s.Conn.Subscribe(s.EventSubject, func(*nats.Msg) {
				select {
				case events <- struct{}{}:
				default:
				}
			})
			if err != nil {
				send(ctx, out, Update{Err: fmt.Errorf("subscribe %q: %w", s.EventSubject, err)})
				return
			}
			defer func() { _ = sub.Unsubscribe() }()
		}

		var lastHash string
		emitIfChanged := func(cfg *config.Config) {
			h := hashConfig(cfg)
			if h == lastHash {
				return
			}
			lastHash = h
			send(ctx, out, Update{Config: cfg})
		}

		// Boot: retry the initial fetch with capped backoff until it succeeds
		// or BootTimeout elapses. Failures during boot are retried, not
		// emitted, so a controller that is still starting doesn't crash the
		// gateway before it has ever served.
		bootDeadline := time.Now().Add(bootTimeout(s.BootTimeout))
		backoff := 200 * time.Millisecond
		for {
			cfg, err := s.fetch(ctx)
			if err == nil {
				emitIfChanged(cfg)
				break
			}
			if ctx.Err() != nil {
				return
			}
			if time.Now().After(bootDeadline) {
				send(ctx, out, Update{Err: fmt.Errorf("initial config fetch timed out after %s: %w", bootTimeout(s.BootTimeout), err)})
				return
			}
			s.log().Warn("config fetch failed during boot, retrying", "err", err, "retry_in", backoff)
			if !sleep(ctx, backoff) {
				return
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}

		// Steady state: re-fetch on a change event or the periodic safety net.
		refetch := s.Refetch
		if refetch <= 0 {
			refetch = 60 * time.Second
		}
		ticker := time.NewTicker(refetch)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-events:
			case <-ticker.C:
			}
			cfg, err := s.fetch(ctx)
			if err != nil {
				send(ctx, out, Update{Err: err}) // non-fatal now: Run keeps last good
				continue
			}
			emitIfChanged(cfg)
		}
	}()
	return out
}

func bootTimeout(d time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return 2 * time.Minute
}

// sleep waits for d or ctx cancellation; returns false if ctx was cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
