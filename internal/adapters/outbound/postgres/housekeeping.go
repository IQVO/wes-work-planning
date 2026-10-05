package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Housekeeper bounds the growth of the two append-only bookkeeping tables
// (ADR-0032): idempotency_keys (rows older than IdempotencyKeyTTL are
// deleted) and outbox_events (rows PUBLISHED more than OutboxRetention ago
// are deleted; an unpublished row is never touched, however old). Deletion is
// batched so one sweep never holds a long lock, and every statement is
// idempotent, so several replicas sweeping concurrently is harmless.
type Housekeeper struct {
	pool            *pgxpool.Pool
	logger          *slog.Logger
	interval        time.Duration
	idempotencyTTL  time.Duration
	outboxRetention time.Duration
	batchSize       int
}

// HousekeeperOption customises a Housekeeper.
type HousekeeperOption func(*Housekeeper)

// Defaults: how often a sweep runs, how long an Idempotency-Key is honoured
// and how long a published outbox row is kept for forensics.
const (
	DefaultHousekeepingInterval = time.Hour
	DefaultIdempotencyKeyTTL    = 24 * time.Hour
	DefaultOutboxRetention      = 7 * 24 * time.Hour
	defaultHousekeepingBatch    = 1000
)

// WithHousekeepingInterval sets the time between sweeps.
func WithHousekeepingInterval(d time.Duration) HousekeeperOption {
	return func(h *Housekeeper) { h.interval = d }
}

// WithIdempotencyKeyTTL sets how long an idempotency_keys row is kept.
func WithIdempotencyKeyTTL(d time.Duration) HousekeeperOption {
	return func(h *Housekeeper) { h.idempotencyTTL = d }
}

// WithOutboxRetention sets how long a published outbox_events row is kept.
func WithOutboxRetention(d time.Duration) HousekeeperOption {
	return func(h *Housekeeper) { h.outboxRetention = d }
}

// WithHousekeepingBatchSize caps how many rows one DELETE removes.
func WithHousekeepingBatchSize(n int) HousekeeperOption {
	return func(h *Housekeeper) { h.batchSize = n }
}

// NewHousekeeper constructs a Housekeeper over pool with the defaults above.
func NewHousekeeper(pool *pgxpool.Pool, logger *slog.Logger, opts ...HousekeeperOption) *Housekeeper {
	if logger == nil {
		logger = slog.Default()
	}
	h := &Housekeeper{
		pool:            pool,
		logger:          logger,
		interval:        DefaultHousekeepingInterval,
		idempotencyTTL:  DefaultIdempotencyKeyTTL,
		outboxRetention: DefaultOutboxRetention,
		batchSize:       defaultHousekeepingBatch,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// SweepResult reports how many rows one sweep removed.
type SweepResult struct {
	IdempotencyKeys int64
	OutboxEvents    int64
}

// idempotencySweepSQL deletes up to $2 rows older than $1 seconds. created_at
// is indexed (idx_idempotency_keys_created_at). A committed row always has its
// outcome populated (ADR-0022 §4), so an old row is never "in progress".
const idempotencySweepSQL = `
	DELETE FROM idempotency_keys
	WHERE key IN (
		SELECT key FROM idempotency_keys
		WHERE created_at < now() - make_interval(secs => $1)
		ORDER BY created_at
		LIMIT $2
	)`

// outboxSweepSQL deletes up to $2 PUBLISHED rows whose published_at is older
// than $1 seconds. published_at IS NOT NULL is what keeps an undelivered event
// safe regardless of age.
const outboxSweepSQL = `
	DELETE FROM outbox_events
	WHERE id IN (
		SELECT id FROM outbox_events
		WHERE published_at IS NOT NULL
		  AND published_at < now() - make_interval(secs => $1)
		ORDER BY id
		LIMIT $2
	)`

// SweepOnce runs one pass over both tables, looping each batched DELETE until
// a batch comes back short. A non-positive TTL/retention disables that
// table's sweep. It returns what it removed and the first error.
func (h *Housekeeper) SweepOnce(ctx context.Context) (SweepResult, error) {
	var res SweepResult
	var err error
	if h.idempotencyTTL > 0 {
		if res.IdempotencyKeys, err = h.deleteBatched(ctx, idempotencySweepSQL, h.idempotencyTTL); err != nil {
			return res, fmt.Errorf("postgres: sweep idempotency_keys: %w", err)
		}
	}
	if h.outboxRetention > 0 {
		if res.OutboxEvents, err = h.deleteBatched(ctx, outboxSweepSQL, h.outboxRetention); err != nil {
			return res, fmt.Errorf("postgres: sweep outbox_events: %w", err)
		}
	}
	return res, nil
}

func (h *Housekeeper) deleteBatched(ctx context.Context, sql string, age time.Duration) (int64, error) {
	var total int64
	for {
		tag, err := h.pool.Exec(ctx, sql, age.Seconds(), h.batchSize)
		if err != nil {
			return total, err
		}
		n := tag.RowsAffected()
		total += n
		if n < int64(h.batchSize) {
			return total, nil
		}
	}
}

// Run sweeps once immediately and then every interval until ctx is cancelled.
// A failing sweep is logged and retried on the next tick; the tables only
// grow in the meantime, nothing is lost.
func (h *Housekeeper) Run(ctx context.Context) error {
	for {
		res, err := h.SweepOnce(ctx)
		switch {
		case err != nil && !errors.Is(err, context.Canceled):
			h.logger.ErrorContext(ctx, "housekeeping sweep failed", "error", err)
		case res.IdempotencyKeys > 0 || res.OutboxEvents > 0:
			h.logger.InfoContext(ctx, "housekeeping sweep removed rows",
				"idempotency_keys", res.IdempotencyKeys, "outbox_events", res.OutboxEvents)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(h.interval):
		}
	}
}
