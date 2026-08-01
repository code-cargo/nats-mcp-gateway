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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	for _, raw := range []string{
		"nats://gw:s3c r3t@127.0.0.1:14222",
		"nats://gw:p%ss@127.0.0.1:14222",
		"nats://gw:s3cr3t@[::1:14222",
		"nats://gw:s3c@r3t@127.0.0.1:14222",
		"nats://gw:pw1@a:4222,nats://gw:pw2@b:4222",
	} {
		err := runGateway(
			&GatewayCmd{ConfigJSON: `{"servers":{}}`, NatsURL: raw},
			&Globals{LogLevel: "error"}, "0.0.0",
		)
		require.Error(t, err)
		for _, secret := range []string{"s3c r3t", "p%ss", "s3cr3t", "s3c@r3t", "pw1", "pw2"} {
			assert.NotContains(t, err.Error(), secret,
				"password reached the error for %q", raw)
		}
		assert.Contains(t, err.Error(), "gw:", "the username must survive: it is a diagnosis")
	}
}
