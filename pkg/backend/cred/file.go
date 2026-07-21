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
	"fmt"
	"os"
	"time"
)

// File reads credentials from a mounted (and possibly rotated) credJSON file:
// {"headers"|"env": {...}, "expiresAt": "RFC3339"}.
type File struct {
	// Path may contain {tenant}, {user}, {server} placeholders.
	Path string
	// TTL applies when the file carries no expiresAt of its own, so a
	// rotated file is still picked up instead of cached forever (default 1m).
	TTL time.Duration
}

// Resolve implements Resolver.
func (f *File) Resolve(_ context.Context, tenant, user, server string) (*Credentials, error) {
	path := expandPath(f.Path, tenant, user, server)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cred: %w", err)
	}
	creds, err := decodeCredentials(data)
	if err != nil {
		return nil, fmt.Errorf("cred: %s: %w", path, err)
	}
	if creds.ExpiresAt.IsZero() {
		ttl := f.TTL
		if ttl <= 0 {
			ttl = time.Minute
		}
		creds.ExpiresAt = time.Now().Add(ttl)
	}
	return creds, nil
}
