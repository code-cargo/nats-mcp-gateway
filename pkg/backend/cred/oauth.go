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

package cred

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The OAuth resolvers speak RFC 6749 token endpoints with stdlib net/http
// only — no OAuth dependency — because each grant is one form POST and one
// JSON response. 4xx token-endpoint answers are Terminal (the IdP said no);
// 5xx and transport failures are retryable.

// OAuthClientCredentials performs the client_credentials grant: one service
// identity for the server, not per-user — the OAuth analog of Static, but
// with expiry and rotation.
type OAuthClientCredentials struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scope        string
	Audience     string
	HTTPClient   *http.Client
}

// Resolve implements Resolver.
func (o *OAuthClientCredentials) Resolve(ctx context.Context, _, _, _ string) (*Credentials, error) {
	form := url.Values{"grant_type": {"client_credentials"}}
	setOpt(form, "scope", o.Scope)
	setOpt(form, "audience", o.Audience)
	return tokenRequest(ctx, o.HTTPClient, o.TokenURL, o.ClientID, o.ClientSecret, form)
}

// OAuthTokenExchange performs RFC 8693 token exchange: the user's identity
// assertion (subject token) is exchanged for an access token scoped to the
// server — token exchange as a battery, no controller required. The subject
// token is read per call from SubjectTokenFile so a rotated assertion is
// picked up.
type OAuthTokenExchange struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scope        string
	Audience     string
	// SubjectTokenFile holds the caller's identity assertion; {tenant},
	// {user}, {server} placeholders select the per-user file.
	SubjectTokenFile string
	// SubjectTokenType defaults to urn:ietf:params:oauth:token-type:access_token.
	SubjectTokenType string
	HTTPClient       *http.Client
}

// Resolve implements Resolver.
func (o *OAuthTokenExchange) Resolve(ctx context.Context, tenant, user, server string) (*Credentials, error) {
	path := expandPath(o.SubjectTokenFile, tenant, user, server)
	subject, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cred: subject token: %w", err)
	}
	tokType := o.SubjectTokenType
	if tokType == "" {
		tokType = "urn:ietf:params:oauth:token-type:access_token"
	}
	form := url.Values{
		"grant_type":         {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"subject_token":      {strings.TrimSpace(string(subject))},
		"subject_token_type": {tokType},
	}
	setOpt(form, "scope", o.Scope)
	setOpt(form, "audience", o.Audience)
	return tokenRequest(ctx, o.HTTPClient, o.TokenURL, o.ClientID, o.ClientSecret, form)
}

// TokenStore holds per-(tenant, user, server) refresh tokens for
// OAuthRefresh. Save is called when the IdP rotates the refresh token.
type TokenStore interface {
	Load(tenant, user, server string) (string, error)
	Save(tenant, user, server, refreshToken string) error
}

// FileTokenStore stores each refresh token as the raw contents of one file.
type FileTokenStore struct {
	// Path with {tenant}, {user}, {server} placeholders.
	Path string
}

// Load implements TokenStore.
func (s *FileTokenStore) Load(tenant, user, server string) (string, error) {
	data, err := os.ReadFile(expandPath(s.Path, tenant, user, server))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// Save implements TokenStore: write a temp file, flush it, rename it over the
// destination. A refresh token is the one credential the gateway cannot
// re-derive — losing it costs the user another trip through consent — and
// truncate-then-write leaves a window where the file holds neither the old
// token nor the new one. A crash, a full disk, or a second writer landing
// inside that window leaves it holding nothing. A rename is one directory
// operation, so a reader sees one whole token or the other and a writer that
// dies leaves the previous one intact.
func (s *FileTokenStore) Save(tenant, user, server, refreshToken string) error {
	path := expandPath(s.Path, tenant, user, server)
	// Beside the destination, because rename is only atomic within one
	// filesystem and the token file is routinely a mounted secret volume.
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("cred: refresh token: %w", err)
	}
	defer func() { _ = os.Remove(f.Name()) }() // a no-op once the rename lands
	if _, err := f.WriteString(refreshToken); err != nil {
		_ = f.Close()
		return fmt.Errorf("cred: refresh token: %w", err)
	}
	// Durability before visibility: a rename can reach the disk ahead of the
	// bytes it points at, and a token file that survives a crash as zero bytes
	// is the same loss as no file at all.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("cred: refresh token: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cred: refresh token: %w", err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("cred: refresh token: %w", err)
	}
	return nil
}

// OAuthRefresh performs the refresh_token grant over a TokenStore: per-user
// long-lived refresh tokens (obtained out of band via an authorization-code
// consent) exchanged for short-lived access tokens on demand.
type OAuthRefresh struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scope        string
	Store        TokenStore
	HTTPClient   *http.Client
}

// Resolve implements Resolver.
func (o *OAuthRefresh) Resolve(ctx context.Context, tenant, user, server string) (*Credentials, error) {
	refresh, err := o.Store.Load(tenant, user, server)
	if err != nil {
		return nil, fmt.Errorf("cred: refresh token: %w", err)
	}
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refresh},
	}
	setOpt(form, "scope", o.Scope)
	creds, rotated, err := tokenRequestFull(ctx, o.HTTPClient, o.TokenURL, o.ClientID, o.ClientSecret, form)
	if err != nil {
		return nil, err
	}
	if rotated != "" && rotated != refresh {
		// Rotation is best-effort: losing the new token means the next
		// refresh fails terminally and consent is redone, which beats
		// failing this resolve after the IdP already accepted it.
		_ = o.Store.Save(tenant, user, server, rotated)
	}
	return creds, nil
}

func setOpt(form url.Values, key, val string) {
	if val != "" {
		form.Set(key, val)
	}
}

func tokenRequest(ctx context.Context, client *http.Client, tokenURL, clientID, clientSecret string, form url.Values) (*Credentials, error) {
	creds, _, err := tokenRequestFull(ctx, client, tokenURL, clientID, clientSecret, form)
	return creds, err
}

// tokenRequestFull POSTs an RFC 6749 token request and maps the response to
// Credentials, also returning any rotated refresh token.
func tokenRequestFull(ctx context.Context, client *http.Client, tokenURL, clientID, clientSecret string, form url.Values) (*Credentials, string, error) {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	// A client with an id and no secret is a PUBLIC client, and RFC 6749 §3.2.1
	// has it identify itself with client_id in the request BODY. Set before the
	// body is encoded, obviously, and before the Basic decision below.
	if clientID != "" && clientSecret == "" {
		form.Set("client_id", clientID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, "", fmt.Errorf("cred: token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if clientID != "" && clientSecret != "" {
		// The escaping is deliberate, not a bug: RFC 6749 §2.3.1 requires
		// the client id/secret to be form-urlencoded BEFORE they go into
		// the Basic header (SetBasicAuth does no encoding of its own).
		// golang.org/x/oauth2 does exactly this, so IdPs interop with it.
		//
		// Only when there IS a secret: Basic with an empty password claims the
		// client authenticated, which is a different claim from a public
		// client's, and Okta, Auth0 and Keycloak all reject it as
		// invalid_client. That rejection is a Terminal 4xx, so it was never
		// even retried — the mode simply never worked.
		req.SetBasicAuth(url.QueryEscape(clientID), url.QueryEscape(clientSecret))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("cred: token request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, "", fmt.Errorf("cred: token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("cred: token endpoint %s: http %d: %s", tokenURL, resp.StatusCode, truncateStr(body, 300))
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return nil, "", Terminal(err) // the IdP answered: no
		}
		return nil, "", err
	}
	var tok struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int64  `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return nil, "", fmt.Errorf("cred: token response: %w", err)
	}
	if tok.AccessToken == "" {
		return nil, "", Terminal(fmt.Errorf("cred: token endpoint %s returned no access_token", tokenURL))
	}
	typ := tok.TokenType
	if typ == "" || strings.EqualFold(typ, "bearer") {
		typ = "Bearer"
	}
	// Unknown expiry defaults to 5m: better a spurious refresh than serving
	// a token past its life.
	expires := 5 * time.Minute
	if tok.ExpiresIn > 0 {
		expires = time.Duration(tok.ExpiresIn) * time.Second
	}
	return &Credentials{
		Headers:   map[string]string{"Authorization": typ + " " + tok.AccessToken},
		ExpiresAt: time.Now().Add(expires),
	}, tok.RefreshToken, nil
}

func truncateStr(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
