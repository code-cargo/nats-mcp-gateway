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
	"os"
	"os/signal"
	"syscall"
	"time"

	nats "github.com/nats-io/nats.go"

	"github.com/code-cargo/nats-mcp-gateway/pkg/backend"
	"github.com/code-cargo/nats-mcp-gateway/pkg/backend/legacy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/config"
	"github.com/code-cargo/nats-mcp-gateway/pkg/mcpspec"
	"github.com/code-cargo/nats-mcp-gateway/pkg/proxy"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

// minRecommendedPayload is the NATS max_payload below which large MCP
// results (tools/list, base64 content) will start failing with -32012.
const minRecommendedPayload = 8 * 1024 * 1024

func runGateway(c *GatewayCmd, g *Globals, version string) error {
	log := NewLogger(g)

	cfg, err := config.Load(c.Config)
	if err != nil {
		return err
	}

	opts := []nats.Option{
		nats.Name("natsmcp-gateway"),
		nats.MaxReconnects(-1),
	}
	if cfg.NATS.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.NATS.CredsFile))
	}
	url := cfg.NATS.URL
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return fmt.Errorf("connect NATS %s: %w", url, err)
	}
	defer nc.Close()

	if mp := nc.MaxPayload(); mp < minRecommendedPayload {
		log.Warn("NATS max_payload is small for MCP traffic; large tool results will fail",
			"max_payload", mp, "recommended", minRecommendedPayload,
			"fix", "set max_payload: 8MB in nats-server config")
	}

	factory := func(key backend.Key) (backend.Backend, error) {
		s, ok := cfg.Servers[key.Server]
		if !ok {
			return nil, fmt.Errorf("unknown server %q", key.Server)
		}
		blog := log.With("server", key.Server, "tenant", key.Tenant)
		modern := s.Protocol == mcpspec.ProtocolVersion
		var inner backend.Backend
		if s.Transport == "http" {
			inner = &backend.HTTPBackend{URL: s.URL, Headers: s.Headers, Legacy: !modern, Logger: blog}
		} else {
			inner = &backend.StdioBackend{Command: s.Command, Args: s.Args, Env: s.Env, Logger: blog}
		}
		if modern {
			return inner, nil
		}
		// Default: 2025-11-25, because that is what exists in the wild.
		return &legacy.Backend{Inner: inner, DiscoverTTLMs: s.DiscoverTTLMs, Logger: blog}, nil
	}
	pool := backend.NewPool(backend.PoolConfig{
		MaxConcurrent:     cfg.Pool.MaxConcurrent,
		MaxProcsPerTenant: cfg.Pool.MaxProcsPerTenant,
	}, factory, log)
	defer pool.Shutdown()

	ws, err := wire.Serve(nc, wire.ServerConfig{
		Prefix:     cfg.NATS.SubjectPrefix,
		QueueGroup: cfg.NATS.QueueGroup,
		Servers:    cfg.ServerNames(),
		Version:    normalizeVersion(version),
	}, proxy.New(pool, log).Handler())
	if err != nil {
		return err
	}
	log.Info("gateway running", "servers", cfg.ServerNames(), "nats", url)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("draining")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return ws.Shutdown(ctx)
}

// normalizeVersion maps build versions like "dev" onto a semver micro will
// accept.
func normalizeVersion(v string) string {
	if len(v) > 0 && v[0] >= '0' && v[0] <= '9' {
		return v
	}
	return "0.0.0"
}
