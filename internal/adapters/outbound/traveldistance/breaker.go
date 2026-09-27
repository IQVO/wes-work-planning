// breaker.go wraps Client with a per-dependency circuit breaker
// (sony/gobreaker/v2, ADR-0023) AND jittered retry (cenkalti/backoff/v4)
// — GetDistance is a pure GET/read, safe to retry, mirroring
// productclassification's identical breaker shape. While the breaker is
// OPEN, this falls back to PermissiveLookup's existing fail-open
// behaviour (Known=false, nil error) — the SAME fallback this client
// already had for a transport error, a 404, or a 422 (ADR-0017), just
// now also reachable via the breaker short-circuiting a call it never
// attempts.
package traveldistance

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/traveldistanceview"
	"github.com/claudioed/wes-work-planning/internal/resilience"
)

// DependencyName labels this breaker's Prometheus gauge series
// (circuit_breaker_state{dependency="facility-layout"}).
const DependencyName = "facility-layout"

// maxRetryAttempts caps the jittered retry at 3 total attempts (1
// original + 2 retries), mirroring productclassification's bound.
const maxRetryAttempts = 3

// retryInitialInterval/retryMaxInterval bound the exponential-backoff-
// with-jitter schedule between attempts — short, because this whole call
// is already bounded by DefaultTimeout end to end (see resilience.CallTimeout).
const (
	retryInitialInterval = 50 * time.Millisecond
	retryMaxInterval     = 500 * time.Millisecond
)

// BreakerClient wraps Client with retry-then-circuit-breaker for
// GetDistance.
type BreakerClient struct {
	breaker  *gobreaker.CircuitBreaker[traveldistanceview.TravelDistanceView]
	inner    *Client
	fallback *PermissiveLookup
}

var _ ports.TravelDistanceLookup = (*BreakerClient)(nil)

// NewBreakerClient builds a BreakerClient wrapping inner. recorder is
// resilience.StateRecorder (typically telemetry.CircuitBreakerMetrics)
// — nil is a valid, documented no-op (see resilience.RecordStateChange),
// so a test that does not care about the metric never needs to
// construct one.
func NewBreakerClient(inner *Client, recorder resilience.StateRecorder) *BreakerClient {
	return newBreakerClient(inner, recorder, resilience.DefaultTimeout)
}

// NewBreakerClientWithTimeout is NewBreakerClient with an explicit
// breaker cooldown (gobreaker.Settings.Timeout) instead of
// resilience.DefaultTimeout, so a half-open-recovery test does not have
// to sleep for the full production cooldown in real time. Production
// code should always use NewBreakerClient; this exists for tests.
func NewBreakerClientWithTimeout(inner *Client, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return newBreakerClient(inner, recorder, cooldown)
}

func newBreakerClient(inner *Client, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return &BreakerClient{
		breaker: gobreaker.NewCircuitBreaker[traveldistanceview.TravelDistanceView](gobreaker.Settings{
			Name:          DependencyName,
			MaxRequests:   resilience.DefaultMaxRequests,
			Interval:      resilience.DefaultInterval,
			Timeout:       cooldown,
			ReadyToTrip:   resilience.ReadyToTrip,
			IsExcluded:    func(err error) bool { return errors.Is(err, context.Canceled) },
			OnStateChange: resilience.RecordStateChange(DependencyName, recorder),
		}),
		inner:    inner,
		fallback: NewPermissiveLookup(),
	}
}

// GetDistance derives its timeout from the inbound request's remaining
// deadline (capped at DefaultTimeout), retries fetch up to
// maxRetryAttempts times with jittered backoff, and runs the whole retry
// loop through the breaker as ONE logical call — a retry storm against
// an already-degraded dependency still only ever counts as one
// success/failure toward the breaker's trip condition, not N. While the
// breaker is OPEN (or half-open and saturated), this falls back to
// PermissiveLookup — the SAME fail-open contract GetDistance already
// had for a transport error/404/422, just reached via a different path.
func (c *BreakerClient) GetDistance(ctx context.Context, from, to string) (traveldistanceview.TravelDistanceView, error) {
	callCtx, cancel := resilience.CallTimeout(ctx, resilience.DefaultTimeout)
	defer cancel()

	result, err := c.breaker.Execute(func() (traveldistanceview.TravelDistanceView, error) {
		return c.retryingFetch(callCtx, from, to)
	})
	if isBreakerRejection(err) {
		return c.fallback.GetDistance(ctx, from, to)
	}
	if err != nil {
		// GetDistance's existing port contract (ADR-0017) is
		// fail-open: any error reaching here — a retry-exhausted
		// transport error, an unexpected status — must still
		// resolve to Known=false/nil rather than propagate, so a
		// travel-distance lookup problem never blocks committing a
		// shift plan.
		return traveldistanceview.TravelDistanceView{From: from, To: to, Known: false}, nil
	}
	return result, nil
}

// retryingFetch retries inner.fetch with jittered exponential backoff,
// bounded to maxRetryAttempts total attempts and to callCtx's own
// deadline (whichever is tighter). A 404/422 (Known=false, nil error) is
// a legitimate answer, not a failure, so it returns on the first attempt
// like a 200 does — only a transport error or an unexpected status is
// retried.
func (c *BreakerClient) retryingFetch(callCtx context.Context, from, to string) (traveldistanceview.TravelDistanceView, error) {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxRetryAttempts-1), callCtx)

	return backoff.RetryNotifyWithData(func() (traveldistanceview.TravelDistanceView, error) {
		return c.inner.fetch(callCtx, from, to)
	}, bounded, nil)
}

// isBreakerRejection reports whether err is gobreaker refusing to even
// attempt the call (open, or half-open and already at its probe limit)
// — the ONLY case that means "fall back to the permissive behaviour"; a
// real error FROM a call gobreaker did let through must propagate
// unchanged, exactly as it did before this breaker existed. Kept as a
// separate unexported copy per package rather than a shared helper
// because adapters never depend on each other (hexagonal fitness test).
func isBreakerRejection(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}
