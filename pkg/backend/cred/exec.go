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

package cred

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// helperWaitDelay is the default bound on how long Resolve waits for the
// helper's stdout to close after the helper itself is gone. Nothing should
// still hold that pipe once the process it belongs to has exited or been
// killed, so this is a grace period and not a budget — the same role
// terminateGrace plays in StdioBackend, and the same length. Exec.WaitDelay
// raises it for a helper that disagrees.
const helperWaitDelay = 3 * time.Second

// killHelperGroup SIGKILLs the helper's process group — the helper and
// everything it forked that is still holding our stdout.
//
// One signal to the whole group, leader included. Killing the leader first and
// the group second would be the same two signals in the order that loses them:
// os/exec is free to reap the leader in between, and the group kill then names
// a pid the kernel has released, so the child this exists to reach survives.
//
// SIGKILL with no SIGTERM first, where StdioBackend escalates: a helper is a
// short-lived script already past its deadline with no session to shut down,
// and every grace period spent here is spent under CachedResolver's per-key
// mutex. Callers must know the group is non-empty; on a group whose last
// member has exited the pgid is free, and a raw kill consults nothing.
func killHelperGroup(pgid int) error {
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// cancelHelper is the Cmd.Cancel for a helper: kill the group, but only once
// os.Process confirms the leader is still ours.
//
// The probe goes through os.Process, not syscall.Kill, because os.Process
// declines to signal a pid it has already reaped — it marks the process done
// under the lock its own Wait takes, and on Linux with pidfd it cannot be
// fooled by reuse at all. Cmd.Wait reaps before it reads the cancel result, so
// this runs after the reap often enough to matter, and the pid being signalled
// is a process GROUP id: aimed at a freed one it is a SIGKILL delivered to
// whatever now holds that pgid, which on this host is as likely as not another
// backend's subprocess tree.
//
// This narrows that window rather than closing it — the group kill below is
// still raw, and the leader can be reaped between the two calls. What closes
// it in practice is that a pgid is reusable only once the whole group is
// empty, and the child this cancel exists to reach is in that group.
//
// ErrProcessDone means the helper finished a hair before the deadline, which
// os/exec reads as "nothing was interrupted" — the answer that keeps such a
// helper from being reported as cancelled.
func cancelHelper(p *os.Process) error {
	if err := p.Signal(syscall.Signal(0)); err != nil {
		return err
	}
	return killHelperGroup(p.Pid)
}

// Exec is the universal adapter: it runs a credential-helper command per
// (tenant, user, server) and parses the credJSON it prints —
// {"headers"|"env": {...}, "expiresAt": "RFC3339"} — so any credential
// system integrates with a short script and no gateway code. It is how AWS
// STS ships batteries-included: a helper wrapping
// `aws sts assume-role-with-web-identity` needs no AWS SDK in the gateway.
type Exec struct {
	Command string
	Args    []string
	// Env is extra environment for the helper. The helper does NOT inherit
	// the gateway's environment (which holds the gateway's own NATS secrets
	// — the same hygiene as StdioBackend): it gets PATH, a private empty
	// HOME, the identity variables NATSMCP_CRED_{TENANT,USER,SERVER}, and
	// these. Naming HOME here overrides the private one, for a helper that
	// genuinely needs a populated home directory.
	Env map[string]string
	// Timeout bounds one helper run (default 30s). It bounds the helper
	// itself; anything the helper spawned that is still holding stdout after
	// the helper exits is bounded by WaitDelay instead, and reaching THAT
	// bound fails the resolve rather than extending this one.
	Timeout time.Duration
	// WaitDelay bounds the wait for stdout to close once the helper is gone
	// (default helperWaitDelay). Reaching it fails the resolve — what was read
	// may be a prefix of the credentials — and reclaims whatever still holds
	// the pipe, so a helper whose child legitimately holds stdout for longer
	// needs this raised past that or it can never succeed. It is bounded at all
	// because the wait is taken under the cache's per-key mutex, which no
	// context can interrupt: an unbounded one wedges that key for the life of
	// the gateway instead of failing and backing off.
	WaitDelay time.Duration
}

// Resolve implements Resolver.
func (e *Exec) Resolve(ctx context.Context, tenant, user, server string) (*Credentials, error) {
	timeout := e.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	waitDelay := e.WaitDelay
	if waitDelay <= 0 {
		waitDelay = helperWaitDelay
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// A scratch HOME for this run, because scrubbing the environment is worth
	// little while HOME still addresses the gateway's own home directory: a
	// helper is third-party code by construction, and the dotfiles it reaches
	// from there include the creds file the gateway authenticates to NATS
	// with. StdioBackend points a spawned server's HOME at its workdir for
	// exactly this reason; a helper handling credentials is no less deserving.
	// Per run rather than per gateway, so nothing one helper leaves behind is
	// readable by the next one resolving for a different user.
	home, err := os.MkdirTemp("", "natsmcp-cred-*")
	if err != nil {
		return nil, fmt.Errorf("cred: helper home: %w", err)
	}
	defer func() { _ = os.RemoveAll(home) }()

	cmd := exec.CommandContext(ctx, e.Command, e.Args...)
	// The same directory again as the working directory, which is the other
	// half of that boundary and the one StdioBackend already draws (cmd.Dir =
	// workDir). Without it the helper runs in the gateway's cwd, so every
	// relative path it writes — a token cache, an SDK's debug log, a scratch
	// file it forgets — lands among the gateway's own files under the
	// gateway's uid.
	cmd.Dir = home
	// The subprocess discipline StdioBackend uses, for the same reason:
	// helpers are wrapper scripts that fork. New process group so the timeout
	// reaches the children (killing the helper alone leaves them running and
	// holding stdout); WaitDelay so a pipe a child still holds cannot outlast
	// the timeout anyway. Both matter more here than there — this wait is
	// taken under CachedResolver's per-key mutex, which no context can
	// interrupt, so an unbounded one wedges that (tenant, user, server) for
	// the life of the gateway instead of failing and backing off.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return cancelHelper(cmd.Process) }
	cmd.WaitDelay = waitDelay
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + home,
		"NATSMCP_CRED_TENANT=" + tenant,
		"NATSMCP_CRED_USER=" + user,
		"NATSMCP_CRED_SERVER=" + server,
	}
	// Appended last, so os/exec's dedup (last occurrence wins) lets a helper
	// that needs a real home be given one by name.
	for k, v := range e.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("cred: helper %s: %w: %s", e.Command, err, bytes.TrimSpace(exitErr.Stderr))
		}
		if errors.Is(err, exec.ErrWaitDelay) {
			// The helper exited but left something holding stdout, so the read
			// was cut short and what we captured may be a prefix.
			//
			// os/exec closes OUR end of that pipe and stops there, so the
			// holder is still running — and still in the group made for it
			// above. Reclaim it, or it outlives the gateway: this path is
			// reached again on every backoff retry, so a helper with the habit
			// leaks a process per attempt. Safe to signal raw here in a way it
			// would not be on the paths above: reaching WaitDelay is itself
			// the evidence that the group still has a member.
			//
			// This does kill a daemon a helper backgrounded on purpose. That
			// is the same contract violation the error names — a helper with
			// something to leave running owes it a stdout of its own.
			if cmd.Process != nil {
				_ = killHelperGroup(cmd.Process.Pid)
			}
			// Say which helper habit caused it: os/exec's own wording names the
			// mechanism and gives an operator nothing to fix.
			return nil, fmt.Errorf("cred: helper %s left a process holding its stdout open: %w", e.Command, err)
		}
		return nil, fmt.Errorf("cred: helper %s: %w", e.Command, err)
	}
	return decodeCredentials(out)
}
