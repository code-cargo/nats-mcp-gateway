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

// Package natstest boots embedded NATS servers for tests — the one place the
// server options live, so a boot-level change (payload sizes, timeouts, new
// server knobs) is one edit instead of a sweep across every package's test
// helper.
package natstest

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	nats "github.com/nats-io/nats.go"
)

// Run starts an embedded NATS server on a random port and returns a
// connected client plus the client URL (for tests that open extra
// connections). Both are cleaned up with the test.
//
// opts may be nil. Unset fields get the test defaults: loopback host, random
// port, silent, and the production-recommended 8MB max_payload (see the
// README) — pass an explicit MaxPayload to test payload limits.
func Run(t testing.TB, opts *server.Options) (*nats.Conn, string) {
	t.Helper()
	if opts == nil {
		opts = &server.Options{}
	}
	opts.Host = "127.0.0.1"
	opts.Port = -1
	opts.NoLog = true
	opts.NoSigs = true
	if opts.MaxPayload == 0 {
		opts.MaxPayload = 8 * 1024 * 1024
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("natstest: new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("natstest: embedded nats-server did not start")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("natstest: connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc, srv.ClientURL()
}
