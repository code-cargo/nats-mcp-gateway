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
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// RefuseUnsafeRedirect is the CheckRedirect every client that carries an
// injected credential must use.
//
// Go's default follows up to ten hops and decides what to carry with them by
// rules that were written for a browser, not for a proxy holding someone
// else's secret:
//
//   - shouldCopyHeaderOnRedirect compares hosts with isDomainOrSubdomain and
//     never looks at the scheme, so an https->http hop on the same host keeps
//     Authorization, and a hop to a SUBDOMAIN keeps it too. A wildcard-DNS
//     tenant or a subdomain takeover under the backend's zone is then enough
//     to be handed the credential the gateway injects.
//   - Sensitive HEADERS are stripped cross-origin; the BODY never is, and
//     307/308 replay it verbatim. For the OAuth client that body is the
//     refresh token or the subject assertion — the one credential the README
//     says the gateway cannot re-derive.
//   - The response comes back to the caller, so a redirect into the pod's
//     network is a read primitive: 301/302/303 also convert the POST to a
//     GET, which is the whole GET surface reachable from the gateway.
//
// requireHTTPS at config-parse time constrains the URL an operator wrote. It
// says nothing about the one actually dialled, and the party choosing that is
// the backend.
//
// The rule kept here is the narrowest that closes all of it: a hop may change
// the path, and nothing else. Same authority — host AND port, because on a pod
// a different port is a different container and the sidecar next door is
// exactly what an SSRF wants — and never a downgrade out of https.
// A path redirect is the only one an MCP POST endpoint has any business
// issuing, and 301/302/303 would turn the exchange into a GET anyway.
func RefuseUnsafeRedirect(req *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		return nil
	}
	first := via[0].URL
	if !sameTarget(first, req.URL) {
		return fmt.Errorf(
			"refusing redirect from %s to %s: a credential is injected for the first and would travel with the hop",
			first.Host, req.URL.Host,
		)
	}
	if first.Scheme == "https" && req.URL.Scheme != "https" {
		return fmt.Errorf(
			"refusing redirect from https to %q: the credential injected for this backend would cross the network in cleartext",
			req.URL.Scheme,
		)
	}
	if len(via) > 5 {
		return fmt.Errorf("refusing redirect after %d hops", len(via))
	}
	return nil
}

// sameTarget reports whether two URLs name the same service.
//
// Host must match. Port must match too — on a pod a different port is a
// different container, and the sidecar next door is exactly what an SSRF
// wants. The one exception is an http->https upgrade, where the default port
// changes for a reason that is not a change of target: there the explicit
// ports are compared, so h -> h upgrades but h:8080 -> h:8443 does not.
func sameTarget(from, to *url.URL) bool {
	if !strings.EqualFold(from.Hostname(), to.Hostname()) {
		return false
	}
	if from.Scheme != to.Scheme {
		return from.Port() == to.Port()
	}
	return defaultedPort(from) == defaultedPort(to)
}

// defaultedPort makes a scheme's implicit port explicit, so https://h and
// https://h:443 compare equal while https://h and https://h:8443 do not.
func defaultedPort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	switch u.Scheme {
	case "https":
		return "443"
	case "http":
		return "80"
	}
	return ""
}
