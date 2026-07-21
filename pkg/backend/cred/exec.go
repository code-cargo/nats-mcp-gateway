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
	"time"
)

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
	// Timeout bounds one helper run (default 30s).
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
		return nil, fmt.Errorf("cred: helper %s: %w", e.Command, err)
	}
	return decodeCredentials(out)
}
