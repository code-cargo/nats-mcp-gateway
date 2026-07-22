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

package wire

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// echoHandler answers every request with an empty result immediately.
func echoHandler(_ context.Context, in *Inbound, w StreamWriter) error {
	return w.End([]byte(`{"jsonrpc":"2.0","id":` + string(in.Msg.ID) + `,"result":{}}`))
}

func reqTo(server string) *Request {
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": "1", "method": "tools/list", "params": map[string]any{},
	})
	return &Request{Server: server, Method: "tools/list", ProtocolVersion: "2026-07-28", Body: body}
}

func terminal(t *testing.T, c *Client, server string) Frame {
	t.Helper()
	s, err := c.Do(context.Background(), reqTo(server))
	require.NoError(t, err)
	var last Frame
	for f := range s.C {
		last = f
	}
	return last
}

func TestSetServersAddAndRemove(t *testing.T) {
	nc := runNATS(t, nil)
	srv, err := Serve(nc, ServerConfig{Servers: []string{"alpha"}}, echoHandler)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	c := client(t, nc, time.Second)

	// alpha serves; beta does not yet exist -> no gateway.
	assert.Equal(t, FrameEnd, terminal(t, c, "alpha").Kind)
	f := terminal(t, c, "beta")
	require.NotNil(t, f.Err)
	assert.Equal(t, ErrCodeNoGateway, f.Err.Code)

	// Reconcile to {beta}: beta now serves, alpha stops (no responders).
	require.NoError(t, srv.SetServers([]string{"beta"}))
	assert.Equal(t, FrameEnd, terminal(t, c, "beta").Kind)
	start := time.Now()
	f = terminal(t, c, "alpha")
	require.NotNil(t, f.Err)
	assert.Equal(t, ErrCodeNoGateway, f.Err.Code)
	assert.Less(t, time.Since(start), 2*time.Second, "removed server must fail fast, not hang")
}

func TestSetServersLeavesUnchangedInFlight(t *testing.T) {
	nc := runNATS(t, nil)
	started := make(chan struct{})
	release := make(chan struct{})
	srv, err := Serve(nc, ServerConfig{
		Servers:   []string{"stable", "victim"},
		KeepAlive: 30 * time.Millisecond,
	}, func(ctx context.Context, in *Inbound, w StreamWriter) error {
		if in.Subject.Server == "stable" {
			close(started)
			<-release // hold the request open across a reconcile
		}
		return w.End([]byte(`{"jsonrpc":"2.0","id":"1","result":{"ok":true}}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		close(release)
		_ = srv.Shutdown(ctx)
	})
	c := client(t, nc, 2*time.Second)

	// Start a long request against the server we will NOT touch.
	stream, err := c.Do(context.Background(), reqTo("stable"))
	require.NoError(t, err)
	<-started

	// Remove an unrelated server mid-flight; the in-flight request survives.
	require.NoError(t, srv.SetServers([]string{"stable"}))
	release <- struct{}{}

	var last Frame
	for f := range stream.C {
		last = f
	}
	assert.Equal(t, FrameEnd, last.Kind)
	assert.Contains(t, string(last.Body), `"ok":true`)
}

func TestServeEmptyThenPopulate(t *testing.T) {
	nc := runNATS(t, nil)
	srv, err := Serve(nc, ServerConfig{}, echoHandler) // no servers at boot
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	c := client(t, nc, time.Second)

	f := terminal(t, c, "later")
	require.NotNil(t, f.Err)
	assert.Equal(t, ErrCodeNoGateway, f.Err.Code)

	require.NoError(t, srv.SetServers([]string{"later"}))
	assert.Equal(t, FrameEnd, terminal(t, c, "later").Kind)
}
