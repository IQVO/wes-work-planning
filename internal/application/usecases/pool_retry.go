package usecases

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
)

// Optimistic-concurrency retry budget around a WorkPool read-modify-write.
// Contention is per path; jittered exponential backoff (2ms doubling,
// capped at 100ms) spreads simultaneous writers out so they stop colliding.
// Exhausting the budget (~1s worst case) surfaces ErrConcurrentModification
// to the caller -- a 409 over HTTP, a retried message on Kafka -- rather
// than spinning or, worse, writing over a newer pool.
const (
	maxPoolSaveAttempts = 12
	poolRetryBase       = 2 * time.Millisecond
	poolRetryCap        = 100 * time.Millisecond
)

// retryOnPoolConflict runs attempt -- which must LOAD the pool, apply its
// change and save it, all inside one call -- again whenever the save lost
// an optimistic-concurrency race. Every attempt starts from a fresh read,
// so no update is ever written over a newer one.
func retryOnPoolConflict(ctx context.Context, attempt func(ctx context.Context) error) error {
	var err error
	for i := 0; i < maxPoolSaveAttempts; i++ {
		if err = attempt(ctx); !errors.Is(err, ports.ErrConcurrentModification) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(poolRetryDelay(i)):
		}
	}
	return err
}

// poolRetryDelay is full-jitter exponential backoff for attempt i.
func poolRetryDelay(i int) time.Duration {
	ceiling := poolRetryBase << i
	if ceiling <= 0 || ceiling > poolRetryCap {
		ceiling = poolRetryCap
	}
	return time.Duration(rand.Int64N(int64(ceiling)) + 1) //nolint:gosec // retry jitter, not a security value
}
