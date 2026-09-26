// Package bootretry retries a boot-time outbound dial (Postgres, Kafka)
// with exponential backoff, for the one, fleet-wide, known reason: every
// injected pod's FIRST outbound TCP dial is reset ~10s after the app
// starts by this fleet's Istio 1.30 native sidecars
// (`holdApplicationUntilProxyStarts` is a no-op for them). A single
// attempt turns that transient, well-understood condition into
// CrashLoopBackOff: the process observes "read: connection reset by
// peer", exits 1 via its normal fail-fast boot contract, and kubelet
// restarts it — over and over, even though the dependency is actually
// reachable a few seconds later.
//
// This is used by every composition root in this repo (cmd/wes,
// cmd/wes-projector, cmd/wes-reports, cmd/mcp) around whatever their
// FIRST outbound dial to Postgres or Kafka is, so the helper lives once,
// here, rather than being copied four times.
//
// Retrying is deliberately NOT a weakening of any fail-closed rule: once
// the budget below is exhausted, the caller still refuses to boot and
// still reports the real last error, not a generic timeout.
package bootretry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// bootRetries and bootRetryDelay bound the startup retry budget.
//
// ~31s total (1+2+4+8+16), comfortably past the ~10s first-dial reset and
// still far inside a startup probe's own tolerance, so a genuinely
// unreachable dependency still fails the pod rather than hanging it.
const (
	bootRetries    = 5
	bootRetryDelay = time.Second
)

// Retry runs op with the standard boot retry budget (bootRetries attempts,
// exponential backoff starting at bootRetryDelay). what names the
// operation for logging (e.g. "run migrations", "ping database").
func Retry(ctx context.Context, logger *slog.Logger, what string, op func() error) error {
	return RetryWithDelay(ctx, logger, what, bootRetryDelay, op)
}

// RetryWithDelay is Retry with the base delay injected, so tests can
// exercise the give-up path without sleeping out the real ~31s budget.
// It returns the LAST error on exhaustion, so a permanent failure still
// reports its real cause rather than "timed out" or "gave up".
func RetryWithDelay(ctx context.Context, logger *slog.Logger, what string, base time.Duration, op func() error) error {
	if logger == nil {
		logger = slog.Default()
	}
	delay := base
	var err error
	for attempt := 1; attempt <= bootRetries; attempt++ {
		if err = op(); err == nil {
			if attempt > 1 {
				logger.Info("succeeded after retry", "op", what, "attempt", attempt)
			}
			return nil
		}
		if attempt == bootRetries {
			break
		}
		logger.Warn("retrying", "op", what, "attempt", attempt, "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("%s (after %d attempts): %w", what, bootRetries, err)
}
