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
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// redacted replaces a credential in a logged URL. Spelled the way
// net/url.URL.Redacted spells it, so it reads as a redaction and not as
// somebody's actual password.
const redacted = "xxxxx"

// userinfoEnd returns the index of the "@" that ends a URL's userinfo, or -1
// when there is none. u is the URL with any scheme already removed.
//
// Two heuristics, because neither alone is safe, and this is textual for the
// reason redactNATSURL is: a credential that breaks net/url parsing is exactly
// the one that must not be passed through.
//
// The authority ends at the first "/", so looking only there is right whenever
// the URL is well-formed, and it keeps a credential-FREE URL whose PATH holds
// an "@" — "https://h:443/u/a@b" — from reading as userinfo of "h:443/u/a"
// and reporting the port and half the path as the redaction.
//
// But a password may contain an unescaped "/", which puts the "@" that ends
// the userinfo BEHIND that bound: "nats://gw:aB3/xY9@host" has no "@" in its
// authority at all, and stopping there returns the whole credential unredacted.
// Base64 passwords carry "/" routinely. So when the authority holds no "@",
// the whole remainder is searched before giving up.
//
// The fallback re-admits the path case: an "@" in a path is redacted as though
// it were a credential. That asymmetry is deliberate. Guessing wrong in this
// direction costs an operator the host and part of the path from one log line;
// guessing wrong in the other prints a password.
func userinfoEnd(u string) int {
	authority := u
	if slash := strings.IndexByte(u, '/'); slash >= 0 {
		authority = u[:slash]
	}
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		return at
	}
	return strings.LastIndex(u, "@")
}

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
		at := userinfoEnd(u)
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
// wherever it appears in a longer string.
//
// The scanned span stops at whitespace and commas because a URL contains
// neither, and without that bound the match runs past the end of the URL to
// reach any later "@" in the message — swallowing the text in between. Two
// shapes hit that: a cluster list whose FIRST member has no credential
// ("nats://a:4222,nats://gw:pw@b" collapsing to "nats://a:xxxxx@b", a URL
// nobody configured), and an ordinary credential-free failure whose error text
// happens to carry an "@", such as a TLS error naming a cert subject. Neither
// leaks — urlSecrets does that job — but both destroy the host and the reason,
// which is the whole of what the operator reads this line for.
//
// Greedy within that span, so userinfo ends at the LAST "@": a password may
// contain an unescaped one, a host may not.
//
// "/" bounds it as well, because userinfo lives in the authority component and
// the authority ends at the first "/". Without that, a credential-FREE URL
// whose path or query carries an "@" — "https://h:443/u/a@b.example" — matches
// from the port to that "@", and the port and half the path are reported to
// the operator as "xxxxx". Nothing leaks either way; what is at stake is
// whether the line still says where the process was trying to connect.
var credInURL = regexp.MustCompile(`(://[^:/@\s,]*:)[^\s,/]*(@)`)

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
	msg := fmt.Sprintf("connect NATS %s: %v", redactNATSURL(raw), err)
	// The literal secrets first, then the pattern. connectFailure is handed
	// the raw URL, so it already HOLDS what needs removing and does not have
	// to re-derive the rules redactNATSURL applies — a second, textual
	// derivation kept reaching a different answer than the first, and the
	// weaker of the two was the one covering the half we do not format. It
	// missed a colon-less userinfo entirely (nats.go reads that as an auth
	// token, so it IS the credential) and stopped at the first @ of a
	// password rather than the last.
	//
	// Both renderings, because a *url.Error quotes its input: a tab in a
	// token arrives as \t, and searching for the raw byte would miss it.
	//
	// Order matters. Scrubbing after the pattern would search for text the
	// pattern has already rewritten.
	for _, secret := range urlSecrets(raw) {
		msg = strings.ReplaceAll(msg, secret, redacted)
		if quoted := strconv.Quote(secret); len(quoted) > 2 {
			msg = strings.ReplaceAll(msg, quoted[1:len(quoted)-1], redacted)
		}
	}
	return &connectError{
		msg: credInURL.ReplaceAllString(msg, "${1}"+redacted+"${2}"),
		err: err,
	}
}

// urlSecrets returns the credential in each element of a NATS URL list, by the
// same two rules redactNATSURL uses: userinfo ends at the LAST "@" before the
// host, and a userinfo with no colon is itself the secret, because nats.go
// reads a passwordless userinfo as an auth token.
func urlSecrets(raw string) []string {
	var out []string
	for _, u := range strings.Split(raw, ",") {
		if n := strings.Index(u, "://"); n >= 0 {
			u = u[n+3:]
		}
		// The SAME rule redactNATSURL uses, deliberately: connectFailure's
		// comment says this exists so there is only one derivation, and two
		// that disagree is the drift it was written to prevent. A bare
		// last-"@" reads "gw:pw@host/a" out of "nats://gw:pw@host/a@b" and
		// scrubs a span that is mostly not the secret.
		at := userinfoEnd(u)
		if at < 0 {
			continue
		}
		userinfo := u[:at]
		if _, pass, ok := strings.Cut(userinfo, ":"); ok {
			if pass != "" {
				out = append(out, pass)
			}
			continue
		}
		if userinfo != "" {
			out = append(out, userinfo)
		}
	}
	return out
}

type connectError struct {
	msg string
	err error
}

func (e *connectError) Error() string { return e.msg }

// Unwrap keeps errors.Is/As working across the redaction. The wrapped error is
// the ORIGINAL and still holds the credential, so it is for matching, never
// for printing: log e, not errors.Unwrap(e).

func (e *connectError) Unwrap() error { return e.err }

// warnPlaintextNATSURL says so when the gateway's own NATS credential is about
// to cross an unencrypted hop.
//
// A warning rather than a refusal, unlike the backend URLs requireHTTPS
// governs, because this one is load-bearing in a way those are not: "nats://"
// is the scheme in every quickstart, in this README's canonical config
// (nats://gw:pw@nats:4222), and in the flag's own default. Refusing it would
// fail the boot of the deployment the documentation describes, which is the
// shape of mistake that has already had to be undone once on this path.
//
// Only userinfo is worth a line. A creds file authenticates by signing a
// server nonce, so nothing secret crosses the wire even in the clear; a
// password or token in the URL is sent as written. Loopback is exempt for the
// reason it is everywhere else — that traffic reaches no network anyone can
// read — and "tls://" and "ws(s)://" are somebody else's decision to have
// already made.
func warnPlaintextNATSURL(log *slog.Logger, raw string) {
	for _, u := range strings.Split(raw, ",") {
		u = strings.TrimSpace(u)
		scheme, rest, ok := strings.Cut(u, "://")
		if !ok {
			// nats.go supplies "nats://" for a scheme-less URL, so this is the
			// plaintext scheme too.
			scheme, rest = "nats", u
		}
		// "ws" alongside "nats" because it is the other unencrypted hop nats.go
		// will take, and it sends the password exactly as nats:// does. "tls"
		// and "wss" are the decisions already made correctly.
		if !strings.EqualFold(scheme, "nats") && !strings.EqualFold(scheme, "ws") {
			continue
		}
		at := userinfoEnd(rest)
		if at < 0 {
			continue
		}
		host := rest[at+1:]
		if slash := strings.IndexByte(host, '/'); slash >= 0 {
			host = host[:slash]
		}
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if isLoopbackHost(host) {
			continue
		}
		log.Warn("the NATS URL carries a credential over an unencrypted connection; use tls://, or terminate TLS in front of this hop",
			"nats", redactNATSURL(u))
	}
}

// isLoopbackHost mirrors pkg/config's, which is not exported. Kept here rather
// than exported from there because the two answer different questions — that
// one exempts a backend URL from a refusal, this one suppresses a warning —
// and coupling them would make a change to either a change to both.
func isLoopbackHost(host string) bool {
	host = strings.ToLower(host)
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
