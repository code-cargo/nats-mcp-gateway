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

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/code-cargo/nats-mcp-gateway/pkg/shim"
	"github.com/code-cargo/nats-mcp-gateway/pkg/wire"
)

func runShim(c *ShimCmd, g *Globals) error {
	log, err := NewLogger(g) // stderr only: stdout is the MCP pipe
	if err != nil {
		return err
	}

	opts := []nats.Option{
		nats.Name("natsmcp-shim-" + c.Server),
		nats.MaxReconnects(-1),
	}
	if c.Creds != "" {
		opts = append(opts, nats.UserCredentials(c.Creds))
	}
	if c.InboxPrefix != "" {
		if err := wire.ValidateSubjectPrefix(c.InboxPrefix); err != nil {
			return fmt.Errorf("--inbox-prefix: %w", err)
		}
		opts = append(opts, nats.CustomInboxPrefix(c.InboxPrefix))
	}
	nc, err := nats.Connect(c.NatsURL, opts...)
	if err != nil {
		return connectFailure(c.NatsURL, err)
	}
	defer nc.Close()

	cfg := wire.ClientConfig{
		Prefix: c.SubjectPrefix,
		Tenant: c.Tenant,
		User:   c.User,
	}
	if c.AcceptClaims {
		js, err := jetstream.New(nc)
		if err != nil {
			return fmt.Errorf("accept-claims: %w", err)
		}
		cfg.Claims = &wire.ObjectClaims{JS: js}
	}
	wc, err := wire.NewClient(nc, cfg)
	if err != nil {
		return err
	}

	s := shim.New(wc, shim.Config{Server: c.Server, Logger: log})
	log.Info("shim running", "server", c.Server, "tenant", c.Tenant, "nats", redactNATSURL(c.NatsURL))
	return s.Run(context.Background(), os.Stdin, os.Stdout)
}
