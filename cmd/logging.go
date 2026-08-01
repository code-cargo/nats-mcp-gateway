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
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
)

// redacted replaces a credential in a logged URL. Spelled the way
// net/url.URL.Redacted spells it, so it reads as a redaction and not as
// somebody's actual password.
const redacted = "xxxxx"

// redactNATSURL strips credentials from a NATS URL (or comma-separated
// cluster list, which is how nats.go takes one) for logging. A URL with no
// credential comes back byte-for-byte, so the ordinary case still logs what
// the operator configured.
//
// This is deliberately textual rather than a net/url round trip. A password
// containing an unescaped "@" or a space fails net/url parsing, and a
// parse-based redactor has nothing to return but the raw string at exactly
// the moment it is holding a credential. Scanning for the last "@" ahead of
// the host cannot fail: neither a scheme nor a host may contain one.
func redactNATSURL(raw string) string {
	urls := strings.Split(raw, ",")
	for i, u := range urls {
		scheme := ""
		if n := strings.Index(u, "://"); n >= 0 {
			// nats.go supplies "nats://" when the scheme is absent, so a
			// scheme-less URL carries userinfo just the same.
			scheme, u = u[:n+3], u[n+3:]
		}
		at := strings.LastIndex(u, "@")
		if at < 0 {
			continue
		}
		userinfo, host := u[:at], u[at+1:]
		if user, _, ok := strings.Cut(userinfo, ":"); ok {
			// user:password. The username stays: "connecting as the wrong
			// identity" is a diagnosis a log line can deliver, and the
			// password is never part of one.
			urls[i] = scheme + user + ":" + redacted + "@" + host
			continue
		}
		// No password at all means nats.go reads the whole userinfo as an
		// auth token, so here the username is the secret.
		urls[i] = scheme + redacted + "@" + host
	}
	return strings.Join(urls, ",")
}

// NewLogger builds the process logger. It always writes to stderr: in the
// shim, stdout is the MCP stdio pipe and a single stray log line there
// corrupts the protocol stream.
//
// An unrecognized level or format fails the boot instead of falling back, the
// same way the config loader rejects rather than defaults. Falling back is
// worst exactly when it happens: "--log-level=verbose" used to mean info, so
// the operator raising verbosity to chase a production problem got the output
// they already had and no hint that the flag had been dropped.
//
// Matching stays case-insensitive, and empty still means the default — both
// have always worked, and tightening them here would fail boots over settings
// that were never wrong.
func NewLogger(g *Globals) (*slog.Logger, error) {
	var level slog.Level
	switch strings.ToLower(g.LogLevel) {
	case "", "info":
		level = slog.LevelInfo
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("--log-level %q is not one of debug, info, warn, error", g.LogLevel)
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch strings.ToLower(g.LogFormat) {
	case "", "text":
		handler = slog.NewTextHandler(os.Stderr, opts)
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("--log-format %q is not one of text, json", g.LogFormat)
	}
	return slog.New(handler), nil
}

// credInURL masks the password in any "scheme://user:password@" sequence,
// wherever it appears in a longer string. Non-greedy to the first "@", which
// a password may not contain unescaped and a host never does.
var credInURL = regexp.MustCompile(`(://[^:/@]*:)[^@]*?(@)`)

// connectFailure renders a NATS connect error with the credential gone from
// BOTH halves of the message.
//
// Redacting only the URL we interpolate is not enough. When url.Parse fails
// inside nats.Connect the *url.Error it returns prints the raw string it could
// not parse, and %w renders that verbatim — so a password malformed enough to
// break parsing, which is the likeliest kind to be mistyped, arrives in the log
// beside the copy we just hid. The scrub therefore runs over the whole rendered
// message and does not depend on the URL parsing, for the same reason
// redactNATSURL is textual.
//
// The error still unwraps, so callers matching on nats.ErrNoServers and friends
// are unaffected.
func connectFailure(raw string, err error) error {
	return &connectError{
		msg: credInURL.ReplaceAllString(
			fmt.Sprintf("connect NATS %s: %v", redactNATSURL(raw), err), "${1}"+redacted+"${2}"),
		err: err,
	}
}

type connectError struct {
	msg string
	err error
}

func (e *connectError) Error() string { return e.msg }
func (e *connectError) Unwrap() error { return e.err }
