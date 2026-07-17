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

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	nats "github.com/nats-io/nats.go"

	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend/legacy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
	"github.com/code-cargo/nats-mcp-gateway/pkg/configsource"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/proxy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/reconcile"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// minRecommendedPayload is the NATS max_payload below which large MCP
// results (tools/list, base64 content) will start failing with -32012.
const minRecommendedPayload = 8 * 1024 * 1024

// bootParams are the boot-time settings that do NOT hot-reload: the gateway's
// own NATS connection and the wire's subject prefix / queue group.
type bootParams struct {
	url        string
	credsFile  string
	prefix     string
	queueGroup string
	pool       config.Pool
}

func runGateway(c *GatewayCmd, g *Globals, version string) error {
	log := NewLogger(g)

	if (c.Config == "") == (c.ConfigSubject == "") {
		return fmt.Errorf("exactly one of --config or --config-subject is required")
	}

	// Resolve boot params. File mode reads them from the file (one time);
	// fetch mode takes them from flags, since we must connect before we can
	// fetch the config.
	boot, err := c.bootParams(log)
	if err != nil {
		return err
	}

	opts := []nats.Option{nats.Name("natsmcp-gateway"), nats.MaxReconnects(-1)}
	if boot.credsFile != "" {
		opts = append(opts, nats.UserCredentials(boot.credsFile))
	}
	nc, err := nats.Connect(boot.url, opts...)
	if err != nil {
		return fmt.Errorf("connect NATS %s: %w", boot.url, err)
	}
	defer nc.Close()

	if mp := nc.MaxPayload(); mp < minRecommendedPayload {
		log.Warn("NATS max_payload is small for MCP traffic; large tool results will fail",
			"max_payload", mp, "recommended", minRecommendedPayload,
			"fix", "set max_payload: 8MB in nats-server config")
	}

	// The reconciler holds the live config; the pool factory resolves each
	// server's definition through it, so an evicted server always respawns
	// from the newest config.
	var rec *reconcile.Reconciler
	factory := func(key backend.Key) (backend.Backend, error) {
		cur := rec.Current()
		if cur == nil {
			return nil, fmt.Errorf("no config loaded yet")
		}
		s, ok := cur.Servers[key.Server]
		if !ok {
			return nil, fmt.Errorf("unknown server %q", key.Server)
		}
		return buildBackend(key, s, log), nil
	}
	pool := backend.NewPool(backend.PoolConfig{
		MaxConcurrent:     boot.pool.MaxConcurrent,
		MaxProcsPerTenant: boot.pool.MaxProcsPerTenant,
	}, factory, log)
	defer pool.Shutdown()

	// Start the wire with an EMPTY server set; the first applied config
	// populates it.
	ws, err := wire.Serve(nc, wire.ServerConfig{
		Prefix:     boot.prefix,
		QueueGroup: boot.queueGroup,
		Version:    normalizeVersion(version),
	}, proxy.New(pool, log).Handler())
	if err != nil {
		return err
	}
	rec = reconcile.New(ws, pool, log)

	// Build the config source.
	source, onSighup := c.buildSource(nc, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signals: SIGHUP reloads the file source; INT/TERM drain.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for s := range sig {
			if s == syscall.SIGHUP {
				if onSighup != nil {
					log.Info("SIGHUP: reloading config")
					onSighup()
				}
				continue
			}
			log.Info("draining", "signal", s.String())
			cancel()
			return
		}
	}()

	log.Info("gateway starting", "nats", boot.url, "source", c.sourceKind())

	// Run the config loop. It returns when ctx is cancelled (signal) or on a
	// fatal initial-config error.
	runErr := configsource.Run(ctx, log, source, func(cfg *config.Config) error {
		_, err := rec.Apply(cfg)
		return err
	})

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	drainErr := ws.Shutdown(shutdownCtx)

	if runErr != nil && ctx.Err() == nil {
		return runErr // a real error, not a clean signal-driven shutdown
	}
	return drainErr
}

// bootParams resolves the non-reloadable boot settings for the selected mode.
func (c *GatewayCmd) bootParams(_ *slog.Logger) (bootParams, error) {
	if c.Config != "" {
		cfg, err := config.Load(c.Config)
		if err != nil {
			return bootParams{}, err
		}
		url := cfg.NATS.URL
		if url == "" {
			url = nats.DefaultURL
		}
		return bootParams{
			url:        url,
			credsFile:  cfg.NATS.CredsFile,
			prefix:     cfg.NATS.SubjectPrefix,
			queueGroup: cfg.NATS.QueueGroup,
			pool:       cfg.Pool,
		}, nil
	}
	return bootParams{
		url:        c.NatsURL,
		credsFile:  c.NatsCreds,
		prefix:     c.SubjectPrefix,
		queueGroup: c.QueueGroup,
	}, nil
}

// buildSource constructs the config source and, for the file source, a SIGHUP
// reload hook.
func (c *GatewayCmd) buildSource(nc *nats.Conn, log *slog.Logger) (configsource.Source, func()) {
	if c.Config != "" {
		f := configsource.NewFile(c.Config, c.ReloadInterval)
		return f, f.Reload
	}
	return &configsource.NATS{
		Conn:           nc,
		RequestSubject: c.ConfigSubject,
		EventSubject:   c.ConfigEventsSubject,
		Refetch:        c.ConfigRefetch,
		Logger:         log,
	}, nil
}

func (c *GatewayCmd) sourceKind() string {
	if c.Config != "" {
		return "file:" + c.Config
	}
	return "nats:" + c.ConfigSubject
}

// buildBackend resolves a server definition into a Backend (stdio/http, modern
// or legacy-bridged).
func buildBackend(key backend.Key, s config.Server, log *slog.Logger) backend.Backend {
	blog := log.With("server", key.Server, "tenant", key.Tenant)
	modern := s.Protocol == mcpspec.ProtocolVersion
	var inner backend.Backend
	if s.Transport == "http" {
		inner = &backend.HTTPBackend{URL: s.URL, Headers: s.Headers, Legacy: !modern, Logger: blog}
	} else {
		inner = &backend.StdioBackend{Command: s.Command, Args: s.Args, Env: s.Env, Logger: blog}
	}
	if modern {
		return inner
	}
	// Default: 2025-11-25, because that is what exists in the wild.
	return &legacy.Backend{Inner: inner, DiscoverTTLMs: s.DiscoverTTLMs, Logger: blog}
}

// normalizeVersion maps build versions like "dev" onto a semver micro will
// accept.
func normalizeVersion(v string) string {
	if len(v) > 0 && v[0] >= '0' && v[0] <= '9' {
		return v
	}
	return "0.0.0"
}
