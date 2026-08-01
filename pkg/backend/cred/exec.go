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

// helperWaitDelay bounds how long Resolve waits for the helper's stdout to
// close after the helper itself is gone. Nothing should still hold that pipe
// once the process it belongs to has exited or been killed, so this is a
// grace period and not a budget — the same role terminateGrace plays in
// StdioBackend, and the same length.
const helperWaitDelay = 3 * time.Second

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
	// — the same hygiene as StdioBackend): it gets PATH and HOME, the
	// identity variables NATSMCP_CRED_{TENANT,USER,SERVER}, and these.
	Env map[string]string
	// Timeout bounds one helper run (default 30s), plus helperWaitDelay when
	// something the helper spawned is still holding its stdout open.
	Timeout time.Duration
}

// Resolve implements Resolver.
func (e *Exec) Resolve(ctx context.Context, tenant, user, server string) (*Credentials, error) {
	timeout := e.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, e.Command, e.Args...)
	// The subprocess discipline StdioBackend uses, for the same reason:
	// helpers are wrapper scripts that fork. New process group so the timeout
	// reaches the children (killing the helper alone leaves them running and
	// holding stdout); WaitDelay so a pipe a child still holds cannot outlast
	// the timeout anyway. Both matter more here than there — this wait is
	// taken under CachedResolver's per-key mutex, which no context can
	// interrupt, so an unbounded one wedges that (tenant, user, server) for
	// the life of the gateway instead of failing and backing off.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// The leader goes through os.Process, not syscall.Kill, because
		// os.Process refuses to signal a pid it has already reaped — it sets
		// statusDone before Wait4 for exactly that reason, and on Linux with
		// pidfd it cannot be fooled by reuse at all. Cmd.Wait reaps before it
		// reads the cancel result, so this runs after the reap often enough to
		// matter, and the pid being signalled is a process GROUP id: aimed at
		// a freed one it is a SIGKILL delivered to whatever now holds that
		// pgid, which on this host is as likely as not another backend's
		// subprocess tree.
		//
		// ErrProcessDone here means the helper finished a hair before the
		// deadline, which os/exec reads as "nothing was interrupted" — the
		// answer that keeps such a helper from being reported as cancelled.
		if err := cmd.Process.Kill(); err != nil {
			return err
		}
		// The leader answered to a signal a moment ago, so its pid is still
		// ours and so is the group named by it. Anything it forked is what
		// this reaches; anything that outlives the group is what WaitDelay
		// below bounds.
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil &&
			!errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}
	cmd.WaitDelay = helperWaitDelay
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"NATSMCP_CRED_TENANT=" + tenant,
		"NATSMCP_CRED_USER=" + user,
		"NATSMCP_CRED_SERVER=" + server,
	}
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
			// was cut short and what we captured may be a prefix. Say which
			// helper habit caused it: os/exec's own wording names the mechanism
			// and gives an operator nothing to fix.
			return nil, fmt.Errorf("cred: helper %s left a process holding its stdout open: %w", e.Command, err)
		}
		return nil, fmt.Errorf("cred: helper %s: %w", e.Command, err)
	}
	return decodeCredentials(out)
}
