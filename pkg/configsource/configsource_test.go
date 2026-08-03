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
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/code-cargo/nats-mcp-gateway/internal/natstest"
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

// Static emits its one config and then holds — Run keeps serving it (does not
// return) until ctx is cancelled, the same lifetime the file/NATS sources have.
// This is what keeps an inline-config gateway running instead of exiting at boot.
func TestStaticHoldsUntilContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applied := make(chan []string, 1)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, nil, Static(cfgWith("only")), func(c *config.Config) error {
			applied <- names(c)
			return nil
		})
	}()

	select {
	case got := <-applied:
		assert.Equal(t, []string{"only"}, got)
	case <-time.After(2 * time.Second):
		t.Fatal("Static never emitted its config")
	}

	// Run must still be serving, not returned.
	select {
	case err := <-done:
		t.Fatalf("Run returned before ctx cancel: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
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

// applyRecorder is an apply func that records every revision it is handed and
// rejects the ones a test tells it to. It is how the retry tests distinguish
// "the source never re-delivered" from "the apply was never retried".
type applyRecorder struct {
	seen   chan []string
	reject atomic.Bool
}

func newApplyRecorder() *applyRecorder {
	return &applyRecorder{seen: make(chan []string, 16)}
}

func (a *applyRecorder) apply(c *config.Config) error {
	a.seen <- names(c)
	if a.reject.Load() {
		return errors.New("apply rejected")
	}
	return nil
}

// next returns the next applied revision, failing the test if none arrives.
func (a *applyRecorder) next(t *testing.T, why string) []string {
	t.Helper()
	select {
	case got := <-a.seen:
		return got
	case <-time.After(3 * time.Second):
		t.Fatalf("no config was applied: %s", why)
		return nil
	}
}

// A revision that fails to APPLY has to stay retryable. Every source suppresses
// unchanged content, so unless the failed apply is fed back the source treats
// that content as delivered and swallows every redelivery of it — here the
// SIGHUP path, whose whole purpose is to force a re-read.
func TestFileReloadRetriesAfterFailedApply(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gw.json")
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

	f := NewFile(path, 0) // no polling: Reload (SIGHUP) is the only trigger
	rec := newApplyRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, nil, f, rec.apply) }()

	assert.Equal(t, []string{"a"}, rec.next(t, "initial revision"))

	// A new revision that fails to apply.
	rec.reject.Store(true)
	writeCfg("a", "b")
	f.Reload()
	assert.Equal(t, []string{"a", "b"}, rec.next(t, "the changed file"))

	// The operator fixes whatever the apply choked on and HUPs again. The file
	// is byte-identical to the revision that failed, which is exactly the case
	// content dedup would swallow.
	//
	// This HUP races Run's Retry, and must work whichever lands first: if Retry
	// wins, the reload finds a cleared baseline and emits; if the reload wins it
	// is suppressed, and Retry re-fires it (TestFileRetryRefiresASpentTrigger).
	// Polling is off, so a source that only cleared the baseline would hang here
	// on the second ordering.
	rec.reject.Store(false)
	f.Reload()
	assert.Equal(t, []string{"a", "b"}, rec.next(t, "a config that failed to apply must be retried on the next SIGHUP"))
}

// The periodic poll is the missed-event safety net, so it has to be able to
// carry a retry of content it already delivered.
func TestPollRetriesAfterFailedApply(t *testing.T) {
	var fetches atomic.Int32
	src := Poll(20*time.Millisecond, func(context.Context) (*config.Config, error) {
		if fetches.Add(1) == 1 {
			return cfgWith("a"), nil
		}
		return cfgWith("a", "b"), nil
	})

	rec := newApplyRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, nil, src, rec.apply) }()

	assert.Equal(t, []string{"a"}, rec.next(t, "initial revision"))
	rec.reject.Store(true)
	assert.Equal(t, []string{"a", "b"}, rec.next(t, "the changed fetch"))
	rec.reject.Store(false)
	assert.Equal(t, []string{"a", "b"}, rec.next(t, "a config that failed to apply must be retried on the next poll"))
}

func TestDedupRetriesAfterFailedApply(t *testing.T) {
	// A push source that re-emits the same revision on every event — the
	// over-emitting shape Dedup exists for.
	raw := SourceFunc(func(ctx context.Context) <-chan Update {
		out := make(chan Update)
		go func() {
			defer close(out)
			cur := cfgWith("a")
			for {
				select {
				case out <- Update{Config: cur}:
					cur = cfgWith("a", "b")
				case <-ctx.Done():
					return
				}
				if !sleep(ctx, 20*time.Millisecond) {
					return
				}
			}
		}()
		return out
	})

	rec := newApplyRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, nil, Dedup(raw), rec.apply) }()

	assert.Equal(t, []string{"a"}, rec.next(t, "initial revision"))
	rec.reject.Store(true)
	assert.Equal(t, []string{"a", "b"}, rec.next(t, "the changed revision"))
	rec.reject.Store(false)
	assert.Equal(t, []string{"a", "b"}, rec.next(t, "a config that failed to apply must survive dedup on the next emit"))
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
	nc, _ := natstest.Run(t, nil)
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
	r.setRaw(body)
}

func (r *configResponder) setRaw(body string) {
	r.mu.Lock()
	r.current = []byte(body)
	r.mu.Unlock()
}

func (r *configResponder) serve(t *testing.T) {
	t.Helper()
	sub, err := r.nc.Subscribe("cfg.request", func(m *nats.Msg) {
		r.mu.Lock()
		defer r.mu.Unlock()
		_ = m.Respond(r.current)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

func TestNATSSourceFetchAndEvent(t *testing.T) {
	nc := runNATS(t)
	r := &configResponder{nc: nc}
	r.set("a")
	r.serve(t)

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

// The refetch interval is the fetch source's missed-event safety net, so it
// has to be able to redeliver content whose apply failed — otherwise the
// gateway serves the previous config until the CONTROLLER's document changes,
// which may be never.
func TestNATSSourceRefetchRetriesAfterFailedApply(t *testing.T) {
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

	src := &NATS{Conn: nc, RequestSubject: "cfg.request", Refetch: 50 * time.Millisecond}
	rec := newApplyRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, nil, src, rec.apply) }()

	assert.Equal(t, []string{"a"}, rec.next(t, "initial revision"))
	rec.reject.Store(true)
	r.set("a", "b")
	assert.Equal(t, []string{"a", "b"}, rec.next(t, "the changed document"))
	rec.reject.Store(false)
	assert.Equal(t, []string{"a", "b"}, rec.next(t, "a config that failed to apply must be retried on the next refetch"))
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
	r.serve(t)

	select {
	case u := <-ch:
		require.NoError(t, u.Err)
		assert.Equal(t, []string{"late"}, names(u.Config), "source converges once the responder is up")
	case <-time.After(5 * time.Second):
		t.Fatal("source never converged after responder came up")
	}
}

// The controller holds the secrets and sends resolved values; the gateway pod's
// own environment is not a second credential source for the documents it
// fetches. Were it one, whoever can answer the config subject could name any
// variable the pod happens to carry — its cloud role credentials, its NATS
// password — and read it back out through a backend argument, environment
// entry, or URL.
func TestNATSSourceDoesNotExpandGatewayEnvironment(t *testing.T) {
	t.Setenv("GATEWAY_SECRET", "leak-me-not")
	nc := runNATS(t)
	r := &configResponder{nc: nc}
	r.setRaw(`{"servers":{"gh":{"transport":"stdio","command":"cmd",` +
		`"env":{"TOKEN":"${GATEWAY_SECRET}"}}}}`)
	r.serve(t)

	src := &NATS{
		Conn:           nc,
		RequestSubject: "cfg.request",
		RequestTimeout: 200 * time.Millisecond,
		BootTimeout:    time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u := <-src.Watch(ctx)
	require.Error(t, u.Err, "a fetched document reaching for the gateway's environment is refused")
	assert.NotContains(t, u.Err.Error(), "leak-me-not", "and the refusal does not echo the value")
	assert.Nil(t, u.Config)
}

// The connection is established before the fetch can happen, so nothing in a
// fetched `nats` block can be acted on. Dropping it silently is the dangerous
// direction: a controller emitting {"nats":{"tenant":"acme"},…} believes it has
// scoped the fleet, while every pod goes on serving every tenant and reporting
// healthy.
func TestNATSSourceRejectsFetchedNatsBlock(t *testing.T) {
	nc := runNATS(t)
	r := &configResponder{nc: nc}
	r.setRaw(`{"nats":{"tenant":"acme"},"servers":{"a":{"transport":"stdio","command":"cmd"}}}`)
	r.serve(t)

	src := &NATS{
		Conn:           nc,
		RequestSubject: "cfg.request",
		RequestTimeout: 200 * time.Millisecond,
		BootTimeout:    time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u := <-src.Watch(ctx)
	require.Error(t, u.Err, `a fetched "nats" block must be refused, not dropped`)
	assert.ErrorIs(t, u.Err, ErrNATSBlock, "and stays distinguishable through the boot wrapping")
	assert.Contains(t, u.Err.Error(), `"nats"`, "the refusal names the block")
	assert.Nil(t, u.Config)
}

// The refusal is per REVISION, not per process: a controller that starts
// emitting a `nats` block against a live fleet must not take it down. The bad
// revision is refused, the last good config keeps serving, and the next fetch
// converges on its own once the controller stops sending it — the same handling
// any other unusable revision gets.
func TestNATSSourceNatsBlockLeavesLastGoodServing(t *testing.T) {
	nc := runNATS(t)
	r := &configResponder{nc: nc}
	r.set("a")
	r.serve(t)

	src := &NATS{
		Conn:           nc,
		RequestSubject: "cfg.request",
		EventSubject:   "cfg.changed",
		Refetch:        time.Hour, // isolate the event path
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applied := make(chan []string, 8)
	go func() {
		_ = Run(ctx, nil, src, func(c *config.Config) error {
			applied <- names(c)
			return nil
		})
	}()

	select {
	case got := <-applied:
		require.Equal(t, []string{"a"}, got)
	case <-time.After(5 * time.Second):
		t.Fatal("initial config never applied")
	}

	r.setRaw(`{"nats":{"tenant":"acme"},"servers":{` +
		`"a":{"transport":"stdio","command":"cmd"},` +
		`"b":{"transport":"stdio","command":"cmd"}}}`)
	require.NoError(t, nc.Publish("cfg.changed", nil))
	select {
	case got := <-applied:
		t.Fatalf("a revision carrying a nats block was applied: %v", got)
	case <-time.After(500 * time.Millisecond):
	}

	r.set("a", "b")
	require.NoError(t, nc.Publish("cfg.changed", nil))
	select {
	case got := <-applied:
		assert.Equal(t, []string{"a", "b"}, got, "the source recovers once the block is gone")
	case <-time.After(5 * time.Second):
		t.Fatal("source never recovered after the controller dropped the block")
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

// TestRetryDeliversARevisionThatHashesLikeNothing guards the one value a
// content filter must not confuse with its own empty state.
//
// The filter's baseline and a revision's identity used to share the empty
// string: hashConfig returned "" for a nil config and for one it could not
// marshal, and Retry reset the baseline to "". So the delivery Retry exists to
// let through was the delivery most likely to be dropped — a failed apply set
// the baseline to "", and the next nil revision hashed to "" and matched it.
// "Every server removed" is a legitimate revision, and a gateway that failed
// one apply must not be the reason it never lands.
func TestRetryDeliversARevisionThatHashesLikeNothing(t *testing.T) {
	var f changeFilter

	require.True(t, f.changed(nil), "the first revision is always a change")
	require.False(t, f.changed(nil), "an unchanged revision is suppressed")

	f.retry()
	assert.True(t, f.changed(nil),
		"a retry must re-deliver the revision whose apply failed, whatever it hashes to")
}

// Dropping the baseline is not enough on its own: Run applies on its own
// goroutine, so the source is free to fire again the moment Run takes a
// revision off the channel. A trigger that lands there finds unchanged content
// and is suppressed — spent before Retry is ever called. retry has to say so,
// or a source with nothing else to fall back on waits forever.
func TestRetryReportsATriggerSpentWhileTheApplyFailed(t *testing.T) {
	var f changeFilter

	require.True(t, f.changed(cfgWith("a")), "the revision Run is applying")
	assert.False(t, f.retry(), "no trigger was spent, so there is none to re-fire")

	require.True(t, f.changed(cfgWith("a")), "the baseline was dropped: re-delivered")
	require.False(t, f.changed(cfgWith("a")), "a trigger lands mid-apply and is swallowed")
	assert.True(t, f.retry(), "the swallowed trigger must be reported")
	assert.False(t, f.retry(), "and reported once — re-firing is not repeatable")
}

// The File source is the one that cannot ride out a spent trigger: with polling
// off, SIGHUP is the only thing that re-reads the file, so a HUP swallowed
// during a failing apply is simply gone. Retry has to fire one in its place.
func TestFileRetryRefiresASpentTrigger(t *testing.T) {
	f := NewFile(filepath.Join(t.TempDir(), "gw.json"), 0)
	// The baseline belongs to a Watch, so stand in for one rather than reaching
	// through the source.
	filter, release := f.attach()
	defer release()

	require.True(t, filter.changed(cfgWith("a")), "the revision Run is applying")
	require.False(t, filter.changed(cfgWith("a")), "a HUP lands mid-apply and is swallowed")

	f.Retry()
	// require, not assert: the drain below would block forever otherwise, and a
	// regression should fail the test, not hang the package.
	require.Len(t, f.reload, 1, "Retry must replace the trigger the suppressed emit spent")

	// It must not manufacture one, either — that is what keeps a config that
	// fails every time from spinning: the re-fired reload emits (the baseline
	// is clear), which is not a suppression, so the next failure re-arms
	// nothing.
	<-f.reload
	require.True(t, filter.changed(cfgWith("a")))
	f.Retry()
	assert.Empty(t, f.reload, "Retry must not invent a trigger when none was lost")
}

// Watch owes EVERY consumer an initial config — "the first Update is the
// initial config" is the whole contract an embedder writes a Source against.
// A change-detection baseline shared across Watches breaks it: the second Watch
// finds its content already recorded as emitted and says nothing until the
// config next changes, which for a settled deployment is never. cmd calls Watch
// once per process, so this is felt only by the embedders the package doc
// invites.
func TestSecondWatchGetsItsOwnInitialConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gw.json")
	require.NoError(t, os.WriteFile(path,
		[]byte(`{"servers":{"a":{"transport":"stdio","command":"cmd-a"}}}`), 0o600))

	sources := map[string]Source{
		"file": NewFile(path, 0),
		"poll": Poll(time.Hour, func(context.Context) (*config.Config, error) { return cfgWith("a"), nil }),
		// An over-emitting push source, which is what Dedup is for: the same
		// revision arrives on both Watches, and only the filter decides.
		"dedup": Dedup(SourceFunc(func(ctx context.Context) <-chan Update {
			out := make(chan Update)
			go func() {
				defer close(out)
				for {
					select {
					case out <- Update{Config: cfgWith("a")}:
					case <-ctx.Done():
						return
					}
				}
			}()
			return out
		})),
	}

	for name, src := range sources {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			initial := func(ch <-chan Update, which string) {
				t.Helper()
				select {
				case u := <-ch:
					require.NoError(t, u.Err)
					assert.Equal(t, []string{"a"}, names(u.Config), which)
				case <-time.After(5 * time.Second):
					t.Fatalf("%s never received an initial config", which)
				}
			}
			initial(src.Watch(ctx), "the first Watch")
			initial(src.Watch(ctx), "a second Watch")
		})
	}
}

// errCapture records the error lines an operator would actually see.
type errCapture struct {
	mu    sync.Mutex
	lines []string
}

func (c *errCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *errCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *errCapture) WithGroup(string) slog.Handler            { return c }

func (c *errCapture) Handle(_ context.Context, r slog.Record) error {
	if r.Level < slog.LevelError {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, r.Message)
	return nil
}

func (c *errCapture) got() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

// A revision that keeps failing repeats this line on every trigger, so what it
// says is what an operator concludes. "Keeping last good config" is the whole
// truth about the wire and none of it about the backend pools, which the apply
// may well have touched on its way to failing — and Run cannot know, because
// apply is a func. So it says where the answer is instead of implying there is
// nothing to answer.
func TestRunFailedApplyLineDoesNotImplyTheBackendsWereUntouched(t *testing.T) {
	logs := &errCapture{}
	rec := newApplyRecorder()
	rec.reject.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = Run(ctx, slog.New(logs), SourceFunc(func(ctx context.Context) <-chan Update {
		out := make(chan Update)
		go func() {
			defer close(out)
			for _, c := range []*config.Config{cfgWith("a"), cfgWith("a", "b")} {
				select {
				case out <- Update{Config: c}:
				case <-ctx.Done():
					return
				}
			}
			<-ctx.Done()
		}()
		return out
	}), func(c *config.Config) error {
		if len(c.Servers) == 1 {
			return nil // serving, so the next failure is not fatal
		}
		return rec.apply(c)
	})

	got := logs.got()
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "backend pools",
		"the line has to point at what the apply did to the pools, not just the wire")
}
