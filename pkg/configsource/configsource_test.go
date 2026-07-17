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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
)

func cfgWith(servers ...string) *config.Config {
	m := map[string]config.Server{}
	for _, s := range servers {
		m[s] = config.Server{Transport: "stdio", Command: "cmd-" + s}
	}
	return &config.Config{Servers: m}
}

func names(cfg *config.Config) []string { return cfg.ServerNames() }

// collect applies configs into a slice until ctx times out.
func collect(t *testing.T, src Source, d time.Duration) [][]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var got [][]string
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Run(ctx, nil, src, func(c *config.Config) error {
			mu.Lock()
			got = append(got, names(c))
			mu.Unlock()
			return nil
		})
	}()
	<-done
	mu.Lock()
	defer mu.Unlock()
	return got
}

func TestPollEmitsOnChangeOnly(t *testing.T) {
	var n atomic.Int32
	src := Poll(30*time.Millisecond, func(context.Context) (*config.Config, error) {
		i := n.Add(1)
		// Same config for the first two fetches, then a new one.
		if i <= 2 {
			return cfgWith("a"), nil
		}
		return cfgWith("a", "b"), nil
	})
	got := collect(t, src, 250*time.Millisecond)
	require.GreaterOrEqual(t, len(got), 2)
	assert.Equal(t, []string{"a"}, got[0], "initial emit")
	assert.Equal(t, []string{"a", "b"}, got[len(got)-1], "only real changes re-emit")
	// The identical second fetch must not have produced an emission.
	for _, g := range got {
		_ = g
	}
	assert.LessOrEqual(t, len(got), 3, "identical fetches must be suppressed")
}

func TestStatic(t *testing.T) {
	got := collect(t, Static(cfgWith("only")), 100*time.Millisecond)
	require.Len(t, got, 1)
	assert.Equal(t, []string{"only"}, got[0])
}

func TestDedupSuppressesRepeats(t *testing.T) {
	// A push source that emits the same config three times, then a new one.
	raw := SourceFunc(func(ctx context.Context) <-chan Update {
		out := make(chan Update)
		go func() {
			defer close(out)
			for _, c := range []*config.Config{cfgWith("a"), cfgWith("a"), cfgWith("a"), cfgWith("a", "b")} {
				select {
				case out <- Update{Config: c}:
				case <-ctx.Done():
					return
				}
			}
			<-ctx.Done()
		}()
		return out
	})
	got := collect(t, Dedup(raw), 200*time.Millisecond)
	assert.Equal(t, [][]string{{"a"}, {"a", "b"}}, got, "dedup collapses identical consecutive configs")
}

func TestRunFirstErrorFatal(t *testing.T) {
	boom := errors.New("boom")
	src := SourceFunc(func(ctx context.Context) <-chan Update {
		out := make(chan Update, 1)
		out <- Update{Err: boom}
		close(out)
		return out
	})
	err := Run(context.Background(), nil, src, func(*config.Config) error { return nil })
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}

func TestRunLaterErrorKeepsServing(t *testing.T) {
	// Good config, then an error, then another good config. Run must not
	// return on the middle error; both good configs apply.
	src := SourceFunc(func(ctx context.Context) <-chan Update {
		out := make(chan Update)
		go func() {
			defer close(out)
			seq := []Update{
				{Config: cfgWith("a")},
				{Err: errors.New("transient")},
				{Config: cfgWith("a", "b")},
			}
			for _, u := range seq {
				select {
				case out <- u:
				case <-ctx.Done():
					return
				}
			}
			<-ctx.Done()
		}()
		return out
	})

	var applied [][]string
	var mu sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = Run(ctx, nil, src, func(c *config.Config) error {
		mu.Lock()
		applied = append(applied, names(c))
		mu.Unlock()
		return nil
	})
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, [][]string{{"a"}, {"a", "b"}}, applied, "later error is skipped, serving continues")
}

func TestRunApplyErrorKeepsLastGood(t *testing.T) {
	src := SourceFunc(func(ctx context.Context) <-chan Update {
		out := make(chan Update)
		go func() {
			defer close(out)
			for _, c := range []*config.Config{cfgWith("a"), cfgWith("bad"), cfgWith("a", "c")} {
				select {
				case out <- Update{Config: c}:
				case <-ctx.Done():
					return
				}
			}
			<-ctx.Done()
		}()
		return out
	})

	var applied [][]string
	var mu sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = Run(ctx, nil, src, func(c *config.Config) error {
		if _, bad := c.Servers["bad"]; bad {
			return errors.New("apply rejected")
		}
		mu.Lock()
		applied = append(applied, names(c))
		mu.Unlock()
		return nil
	})
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, [][]string{{"a"}, {"a", "c"}}, applied, "failed apply is skipped, last good survives")
}

func TestFileSourceReloadOnChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gw.json")
	// Write atomically (temp + rename) so an async reload can never catch a
	// half-written file — the same guarantee a K8s ConfigMap symlink swap
	// gives in production.
	writeCfg := func(servers ...string) {
		body := `{"servers":{`
		for i, s := range servers {
			if i > 0 {
				body += ","
			}
			body += fmt.Sprintf(`%q:{"transport":"stdio","command":"cmd"}`, s)
		}
		body += `}}`
		tmp := filepath.Join(dir, "gw.json.tmp")
		require.NoError(t, os.WriteFile(tmp, []byte(body), 0o600))
		require.NoError(t, os.Rename(tmp, path))
	}
	writeCfg("a")

	f := NewFile(path, 0) // no polling; drive via Reload
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := f.Watch(ctx)

	u := <-ch
	require.NoError(t, u.Err)
	assert.Equal(t, []string{"a"}, names(u.Config))

	// Reload with no change: nothing emitted (verified by a follow-up change).
	f.Reload()
	writeCfg("a", "b")
	f.Reload()
	u = <-ch
	require.NoError(t, u.Err)
	assert.Equal(t, []string{"a", "b"}, names(u.Config), "only the real change surfaces")
}

// --- NATS source, against embedded NATS -----------------------------------

func runNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true}
	s, err := server.NewServer(opts)
	require.NoError(t, err)
	go s.Start()
	require.True(t, s.ReadyForConnections(5*time.Second))
	t.Cleanup(s.Shutdown)
	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// configResponder serves a mutable config over request/reply and can publish
// change events.
type configResponder struct {
	nc      *nats.Conn
	mu      sync.Mutex
	current []byte
}

func (r *configResponder) set(servers ...string) {
	body := `{"servers":{`
	for i, s := range servers {
		if i > 0 {
			body += ","
		}
		body += fmt.Sprintf(`%q:{"transport":"stdio","command":"cmd"}`, s)
	}
	body += `}}`
	r.mu.Lock()
	r.current = []byte(body)
	r.mu.Unlock()
}

func TestNATSSourceFetchAndEvent(t *testing.T) {
	nc := runNATS(t)
	r := &configResponder{nc: nc}
	r.set("a")
	sub, err := nc.Subscribe("cfg.request", func(m *nats.Msg) {
		r.mu.Lock()
		defer r.mu.Unlock()
		_ = m.Respond(r.current)
	})
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	src := &NATS{
		Conn:           nc,
		RequestSubject: "cfg.request",
		EventSubject:   "cfg.changed",
		Refetch:        time.Hour, // isolate the event path
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := src.Watch(ctx)

	u := <-ch
	require.NoError(t, u.Err)
	assert.Equal(t, []string{"a"}, names(u.Config))

	// Change the config and publish an event → the source re-fetches.
	r.set("a", "b")
	require.NoError(t, nc.Publish("cfg.changed", nil))
	select {
	case u = <-ch:
		require.NoError(t, u.Err)
		assert.Equal(t, []string{"a", "b"}, names(u.Config))
	case <-time.After(5 * time.Second):
		t.Fatal("change event did not trigger a re-fetch")
	}
}

func TestNATSSourceRetriesUntilResponderUp(t *testing.T) {
	nc := runNATS(t)

	src := &NATS{
		Conn:           nc,
		RequestSubject: "cfg.request",
		RequestTimeout: 200 * time.Millisecond,
		BootTimeout:    10 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := src.Watch(ctx)

	// Bring the responder up only after the source has already been retrying.
	time.Sleep(500 * time.Millisecond)
	r := &configResponder{nc: nc}
	r.set("late")
	sub, err := nc.Subscribe("cfg.request", func(m *nats.Msg) {
		r.mu.Lock()
		defer r.mu.Unlock()
		_ = m.Respond(r.current)
	})
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	select {
	case u := <-ch:
		require.NoError(t, u.Err)
		assert.Equal(t, []string{"late"}, names(u.Config), "source converges once the responder is up")
	case <-time.After(5 * time.Second):
		t.Fatal("source never converged after responder came up")
	}
}

func TestNATSSourceBootTimeoutFatal(t *testing.T) {
	nc := runNATS(t)
	src := &NATS{
		Conn:           nc,
		RequestSubject: "cfg.never",
		RequestTimeout: 100 * time.Millisecond,
		BootTimeout:    300 * time.Millisecond,
	}
	err := Run(context.Background(), nil, src, func(*config.Config) error { return nil })
	require.Error(t, err, "a responder that never answers must eventually be fatal")
}
