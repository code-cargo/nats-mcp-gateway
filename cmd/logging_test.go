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
