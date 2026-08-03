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
	"errors"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// closedPort returns a loopback port with nothing listening on it, for the
// tests below that need nats.Connect to FAIL. A hardcoded port that something
// else happens to hold would take runGateway PAST the connect and into its
// serving loop, which runs until a signal.
func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return strconv.Itoa(port)
}

// connectErr runs the gateway far enough to fail its NATS connect and returns
// that error.
//
// Bounded, for the same reason closedPort exists: if the connect ever
// succeeded, runGateway would block until a signal, and the suite would hang
// instead of failing — the one outcome a test must never have.
func connectErr(t *testing.T, raw string) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- runGateway(
			&GatewayCmd{ConfigJSON: `{"servers":{}}`, NatsURL: raw},
			&Globals{LogLevel: "error"}, "0.0.0",
		)
	}()
	select {
	case err := <-done:
		require.Error(t, err, raw)
		return err
	case <-time.After(30 * time.Second):
		t.Fatalf("runGateway never failed its connect for %q", raw)
		return nil
	}
}

// The README's own canonical config is "nats://gw:pw@nats:4222", so the
// deployment shape this gateway ships with puts a password in the one string
// the boot line and the connect error both print. Anywhere those land — a pod
// log, a crash-loop event, a support paste — the credential lands too.
func TestRedactNATSURL(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
	}{
		// The README's canonical config. The username survives: "wrong
		// identity" is a real diagnosis, "wrong password" is not one the log
		// can help with.
		{"nats://gw:s3cr3t@nats:4222", "nats://gw:xxxxx@nats:4222"},
		// nats.go reads a passwordless userinfo as an auth token, so here the
		// username IS the credential and nothing survives.
		{"nats://s3cr3t@nats:4222", "nats://xxxxx@nats:4222"},
		// No credential, nothing to hide: these must log exactly as written,
		// including the flag's own default.
		{"nats://nats:4222", "nats://nats:4222"},
		{"nats://127.0.0.1:4222", "nats://127.0.0.1:4222"},
		{"", ""},
		// nats.go supplies the scheme when it is missing, so the scheme-less
		// form carries credentials just as well.
		{"gw:s3cr3t@nats:4222", "gw:xxxxx@nats:4222"},
		// A cluster is one comma-separated string to nats.go, and every member
		// of it usually carries the same credential.
		{
			"nats://gw:s3cr3t@a:4222,nats://gw:s3cr3t@b:4222,nats://c:4222",
			"nats://gw:xxxxx@a:4222,nats://gw:xxxxx@b:4222,nats://c:4222",
		},
		// An unescaped "@" in the password makes this unparseable as a URL,
		// which is exactly when a redactor that leaned on net/url would give
		// up and pass the secret through.
		{"tls://gw:s3c@r3t@nats:4222", "tls://gw:xxxxx@nats:4222"},
		// A credential-free URL whose PATH carries an "@". The password span
		// is bounded by "/" so the authority survives: an operator reading a
		// connect failure needs the host and port intact, and reporting the
		// port as "xxxxx" reads as a redaction that fired on nothing.
		{"https://h:443/u/a@b.example", "https://h:443/u/a@b.example"},
	} {
		got := redactNATSURL(tc.raw)
		assert.Equal(t, tc.want, got, tc.raw)
		assert.NotContains(t, got, "s3cr3t", tc.raw)
	}
}

// An unrecognized level used to round down to info and an unrecognized format
// to text, so "--log-level=verbose" produced exactly the output the operator
// already had. That is the worst moment to swallow a flag: the reason to touch
// it at all is that something is wrong in production and the logs are not
// saying what.
func TestNewLoggerRejectsUnknownLevelAndFormat(t *testing.T) {
	for _, level := range []string{"verbose", "trace", "warning", "notice", "fatal"} {
		_, err := NewLogger(&Globals{LogLevel: level, LogFormat: "text"})
		require.Error(t, err, level)
		assert.Contains(t, err.Error(), "--log-level", level)
	}
	for _, format := range []string{"logfmt", "yaml", "console"} {
		_, err := NewLogger(&Globals{LogLevel: "info", LogFormat: format})
		require.Error(t, err, format)
		assert.Contains(t, err.Error(), "--log-format", format)
	}
}

// Case-insensitive matching predates the rejection above and stays: an
// operator who has been passing DEBUG must not be met with a boot failure for
// a setting that has always worked. Empty means the default, the way an unset
// duration does everywhere else here.
func TestNewLoggerAcceptsKnownLevelsAndFormats(t *testing.T) {
	for _, tc := range []struct {
		level string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"Warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
	} {
		lg, err := NewLogger(&Globals{LogLevel: tc.level})
		require.NoError(t, err, tc.level)
		assert.True(t, lg.Enabled(context.Background(), tc.want), tc.level)
		assert.False(t, lg.Enabled(context.Background(), tc.want-1), tc.level)
	}

	for _, tc := range []struct {
		format string
		want   slog.Handler
	}{
		{"", &slog.TextHandler{}},
		{"text", &slog.TextHandler{}},
		{"JSON", &slog.JSONHandler{}},
	} {
		lg, err := NewLogger(&Globals{LogFormat: tc.format})
		require.NoError(t, err, tc.format)
		assert.IsType(t, tc.want, lg.Handler(), tc.format)
	}
}

// TestConnectFailureRedactsThroughTheWrappedError covers the half of the
// message we do not format.
//
// Redacting the URL we interpolate leaves nats.Connect free to return a
// *url.Error, which prints the raw string it could not parse — so a password
// malformed enough to break url.Parse, the likeliest kind to be mistyped,
// arrived in the log beside the copy we had just hidden. Each password below
// breaks parsing in a different place, and the last one is an ordinary
// password behind a mistyped IPv6 bracket.
func TestConnectFailureRedactsThroughTheWrappedError(t *testing.T) {
	port := closedPort(t)
	for _, tmpl := range []string{
		"nats://gw:s3c r3t@127.0.0.1:PORT",
		"nats://gw:p%ss@127.0.0.1:PORT",
		"nats://gw:s3cr3t@[::1:PORT",
		// This one used to pass for the wrong reason: it PARSES, so
		// nats.Connect never returns a *url.Error and the raw string never
		// entered the message. The bracket is what puts it on the leaking
		// path, which is where the @-in-password rule actually matters —
		// userinfo ends at the LAST @, and a scrubber that stops at the first
		// leaves the tail behind.
		"nats://gw:s3c@r3t@[::1:PORT",
		"nats://gw:s3c@r3t@127.0.0.1:PORT",
		"nats://gw:pw1@a:4222,nats://gw:pw2@b:4222",
		// No colon in the userinfo: nats.go reads the whole thing as an auth
		// token, so the "username" IS the credential. A pattern requiring a
		// colon never matches these, and an ordinary opaque token behind a
		// mistyped bracket or a stray space in the host is enough.
		"nats://S3CR3TTOKEN@[::1:PORT",
		"nats://S3CR3TTOKEN@ho st:PORT",
		"nats://tok%S3CR3T@127.0.0.1:PORT",
	} {
		raw := strings.ReplaceAll(tmpl, "PORT", port)
		err := connectErr(t, raw)
		for _, secret := range []string{
			"s3c r3t", "p%ss", "s3cr3t", "s3c@r3t", "r3t", "pw1", "pw2",
			"S3CR3TTOKEN", "S3CR3T",
		} {
			assert.NotContains(t, err.Error(), secret,
				"password reached the error for %q", raw)
		}
		// A username survives only when there IS one. A colon-less userinfo
		// is entirely the credential, so nothing of it may remain.
		if strings.Contains(raw, "gw:") {
			assert.Contains(t, err.Error(), "gw:", "the username must survive: it is a diagnosis")
		}
	}
}

// The other half of the contract: everything that is NOT a credential has to
// come through untouched.
//
// The pattern scrub is bounded to the URL — a span with no whitespace and no
// comma — because an unbounded one runs past the URL to reach any later "@" in
// the message and replaces everything in between. Neither case below carries a
// credential at all, and both used to come out mangled: the first printed a
// URL nobody configured, and the second lost the reason it failed. A redactor
// that eats the host and the error is no more useful than one that prints the
// password, because in both cases the operator cannot act on the line.
func TestConnectFailureKeepsWhatIsNotACredential(t *testing.T) {
	for _, tc := range []struct{ raw, wrapped, want string }{
		{
			// A cluster whose FIRST member is credential-free. The match used
			// to start at that member's port and run to the second member's
			// "@", collapsing both into "nats://a:xxxxx@b:4222".
			raw:     "nats://a:4222,nats://gw:pw@b:4222",
			wrapped: "nats: no servers available for connection",
			want:    "connect NATS nats://a:4222,nats://gw:xxxxx@b:4222: nats: no servers available for connection",
		},
		{
			// No credential anywhere; the "@" belongs to a cert subject in the
			// error text. This is the ordinary TLS misconfiguration, and the
			// scrub used to eat the port and the whole diagnosis with it.
			raw:     "nats://nats.example.com:4222",
			wrapped: "x509: certificate is valid for admin@example.com, not nats.example.com",
			want:    "connect NATS nats://nats.example.com:4222: x509: certificate is valid for admin@example.com, not nats.example.com",
		},
		{
			// Both at once: the credential goes, the cert subject stays.
			raw:     "nats://gw:s3cr3t@nats.example.com:4222",
			wrapped: "x509: certificate is valid for admin@example.com, not nats.example.com",
			want:    "connect NATS nats://gw:xxxxx@nats.example.com:4222: x509: certificate is valid for admin@example.com, not nats.example.com",
		},
	} {
		got := connectFailure(tc.raw, errors.New(tc.wrapped)).Error()
		assert.Equal(t, tc.want, got, tc.raw)
	}
}
