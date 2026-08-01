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

package backend

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/code-cargo/nats-mcp-gateway/pkg/jsonrpc"
)

const (
	// maxLineBytes bounds one ndjson line from the subprocess (16 MiB —
	// larger than any NATS payload we could forward anyway).
	maxLineBytes = 16 * 1024 * 1024

	// stderrRingLines is how many trailing stderr lines are kept for crash
	// reports — often the only failure signal a stdio server emits.
	stderrRingLines = 50

	// terminateGrace is how long Close waits between close-stdin, SIGTERM,
	// and SIGKILL escalations.
	terminateGrace = 3 * time.Second
)

// StdioBackend spawns an MCP server subprocess per connection and speaks
// newline-delimited JSON-RPC over its pipes.
type StdioBackend struct {
	Command string
	Args    []string
	// Env is the FULL child environment except PATH and HOME: the parent
	// environment is deliberately not inherited, because it holds the
	// gateway's own NATS credentials and secrets — inheriting it would hand
	// them to every third-party server we spawn.
	Env    map[string]string
	Logger *slog.Logger
}

// Connect spawns the subprocess.
func (b *StdioBackend) Connect(ctx context.Context) (Conn, error) {
	log := b.Logger
	if log == nil {
		log = slog.Default()
	}

	workDir, err := os.MkdirTemp("", "natsmcp-*")
	if err != nil {
		return nil, fmt.Errorf("backend: temp workdir: %w", err)
	}

	// Undo everything this function allocated on every path that returns
	// without a live subprocess to own it. What fails a pipe below is
	// descriptor exhaustion, and that is a condition the gateway sits in
	// rather than passes through: leaving a workdir (and the descriptors of
	// whichever pipes already succeeded) behind on each attempt compounds
	// exactly the shortage that caused it.
	spawned := false
	var opened []io.Closer
	defer func() {
		if spawned {
			return
		}
		for _, p := range opened {
			_ = p.Close()
		}
		_ = os.RemoveAll(workDir)
	}()

	cmd := exec.Command(b.Command, b.Args...)
	cmd.Dir = workDir
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + workDir,
	}
	for k, v := range b.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	// New process group so teardown can kill grandchildren (npx execs node,
	// which forks); WaitDelay so Wait can't hang on pipes grandchildren
	// still hold.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = terminateGrace

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("backend: stdin pipe: %w", err)
	}
	opened = append(opened, stdin)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("backend: stdout pipe: %w", err)
	}
	opened = append(opened, stdout)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("backend: stderr pipe: %w", err)
	}
	opened = append(opened, stderr)

	// Start takes over the parent ends from here — closing them itself if it
	// fails, and handing them to the loops below if it does not.
	opened = nil
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("backend: start %s: %w", b.Command, err)
	}
	spawned = true

	c := &stdioConn{
		cmd:     cmd,
		stdin:   stdin,
		log:     log.With("backend_cmd", b.Command, "pid", cmd.Process.Pid),
		lines:   make(chan []byte, 64),
		dead:    make(chan struct{}),
		workDir: workDir,
	}
	c.log.Info("backend subprocess started", "args", b.Args)
	go c.stdoutLoop(stdout)
	go c.stderrLoop(stderr)
	go c.waitLoop()
	return c, nil
}

type stdioConn struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	log     *slog.Logger
	workDir string

	writeMu sync.Mutex

	lines chan []byte // decoded-enough stdout lines

	dead      chan struct{} // closed when the process has exited
	deadOnce  sync.Once
	exitErr   error
	closeOnce sync.Once

	ringMu sync.Mutex
	ring   [][]byte // last stderr lines
}

// ErrConnDead is returned by Read/Write once the subprocess has exited.
var ErrConnDead = errors.New("backend: connection dead")

func (c *stdioConn) stdoutLoop(stdout io.Reader) {
	defer close(c.lines)
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 64*1024), maxLineBytes)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		// Some servers print banners to stdout before speaking JSON-RPC;
		// skip anything that is not a JSON object rather than dying on it.
		if line[0] != '{' {
			c.log.Warn("skipping non-JSON stdout line", "line", truncate(line, 200))
			continue
		}
		buf := make([]byte, len(line))
		copy(buf, line)
		c.lines <- buf
	}
	if err := sc.Err(); err != nil {
		c.log.Debug("stdout scanner ended", "err", err)
	}
}

func (c *stdioConn) stderrLoop(stderr io.Reader) {
	sc := bufio.NewScanner(stderr)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := make([]byte, len(sc.Bytes()))
		copy(line, sc.Bytes())
		c.log.Debug("backend stderr", "line", truncate(line, 500))
		c.ringMu.Lock()
		c.ring = append(c.ring, line)
		if len(c.ring) > stderrRingLines {
			c.ring = c.ring[1:]
		}
		c.ringMu.Unlock()
	}
}

func (c *stdioConn) waitLoop() {
	err := c.cmd.Wait()
	c.deadOnce.Do(func() {
		c.exitErr = err
		close(c.dead)
	})
	if err != nil {
		c.ringMu.Lock()
		tail := make([]string, 0, len(c.ring))
		for _, l := range c.ring {
			tail = append(tail, string(l))
		}
		c.ringMu.Unlock()
		c.log.Warn("backend subprocess exited", "err", err, "stderr_tail", tail)
	} else {
		c.log.Info("backend subprocess exited cleanly")
	}
	_ = os.RemoveAll(c.workDir)
}

// Dead reports whether the subprocess has exited.
func (c *stdioConn) Dead() bool {
	select {
	case <-c.dead:
		return true
	default:
		return false
	}
}

func (c *stdioConn) Read(ctx context.Context) (*jsonrpc.Message, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case line, ok := <-c.lines:
			if !ok {
				return nil, ErrConnDead
			}
			msg, err := jsonrpc.Decode(line)
			if err != nil {
				c.log.Warn("skipping invalid JSON-RPC line", "err", err, "line", truncate(line, 200))
				continue
			}
			return msg, nil
		}
	}
}

func (c *stdioConn) Write(ctx context.Context, msg *jsonrpc.Message) error {
	if c.Dead() {
		return ErrConnDead
	}
	data, err := jsonrpc.Encode(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := c.stdin.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("backend: write stdin: %w", err)
	}
	return nil
}

// Close tears the subprocess down: close stdin (the polite MCP shutdown
// signal), then SIGTERM the process group, then SIGKILL it.
func (c *stdioConn) Close() error {
	c.closeOnce.Do(func() {
		_ = c.stdin.Close()
		if c.waitDead(terminateGrace) {
			return
		}
		c.signalGroup(syscall.SIGTERM)
		if c.waitDead(terminateGrace) {
			return
		}
		c.signalGroup(syscall.SIGKILL)
		c.waitDead(terminateGrace)
	})
	return nil
}

func (c *stdioConn) waitDead(d time.Duration) bool {
	select {
	case <-c.dead:
		return true
	case <-time.After(d):
		return false
	}
}

// signalGroup signals the whole process group so grandchildren die too.
func (c *stdioConn) signalGroup(sig syscall.Signal) {
	if c.cmd.Process == nil {
		return
	}
	// Negative pid = the process group created by Setpgid.
	_ = syscall.Kill(-c.cmd.Process.Pid, sig)
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
