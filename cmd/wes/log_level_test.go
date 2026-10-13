package main

import (
	"context"
	"log/slog"
	"testing"
)

// TestNewLoggerAcceptsWarning pins gap fix D22: LOG_LEVEL=warning means
// warn in every binary, not a silent fallback to info.
func TestNewLoggerAcceptsWarning(t *testing.T) {
	for _, level := range []string{"warn", "warning", "WARNING"} {
		l := newLogger(level)
		if l.Enabled(context.Background(), slog.LevelInfo) {
			t.Fatalf("LOG_LEVEL=%s must suppress info", level)
		}
		if !l.Enabled(context.Background(), slog.LevelWarn) {
			t.Fatalf("LOG_LEVEL=%s must keep warn", level)
		}
	}
}
