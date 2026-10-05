//go:build integration

package postgres_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
)

// ADR-0032: the sweeper deletes idempotency keys past the TTL and PUBLISHED
// outbox rows past the retention — and never an unpublished row, however old.
func TestHousekeeper_SweepsExpiredRowsAndNeverUnpublishedOutbox(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	for _, q := range []string{
		// idempotency_keys: 3 expired (>24h), 2 fresh
		`INSERT INTO idempotency_keys (key, method, path, request_hash, created_at) VALUES ('old-1','POST','/p','h', now() - interval '25 hours')`,
		`INSERT INTO idempotency_keys (key, method, path, request_hash, created_at) VALUES ('old-2','POST','/p','h', now() - interval '3 days')`,
		`INSERT INTO idempotency_keys (key, method, path, request_hash, created_at) VALUES ('old-3','POST','/p','h', now() - interval '30 days')`,
		`INSERT INTO idempotency_keys (key, method, path, request_hash, created_at) VALUES ('fresh-1','POST','/p','h', now() - interval '1 hour')`,
		`INSERT INTO idempotency_keys (key, method, path, request_hash, created_at) VALUES ('fresh-2','POST','/p','h', now())`,
		// outbox_events: 3 published long ago (>7d), 1 published recently,
		// 1 unpublished AND ancient (must survive), 1 unpublished fresh
		`INSERT INTO outbox_events (topic, event_type, key, value, created_at, published_at) VALUES ('t','e','k','v', now() - interval '9 days', now() - interval '8 days')`,
		`INSERT INTO outbox_events (topic, event_type, key, value, created_at, published_at) VALUES ('t','e','k','v', now() - interval '20 days', now() - interval '19 days')`,
		`INSERT INTO outbox_events (topic, event_type, key, value, created_at, published_at) VALUES ('t','e','k','v', now() - interval '40 days', now() - interval '39 days')`,
		`INSERT INTO outbox_events (topic, event_type, key, value, created_at, published_at) VALUES ('t','e','k','v', now() - interval '2 days', now() - interval '1 day')`,
		`INSERT INTO outbox_events (topic, event_type, key, value, created_at) VALUES ('t','e','k','v', now() - interval '60 days')`,
		`INSERT INTO outbox_events (topic, event_type, key, value, created_at) VALUES ('t','e','k','v', now())`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Batch size 2 forces the batched loop to iterate (3 rows -> 2 + 1).
	h := postgres.NewHousekeeper(pool, slog.Default(), postgres.WithHousekeepingBatchSize(2))
	res, err := h.SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if res.IdempotencyKeys != 3 || res.OutboxEvents != 3 {
		t.Fatalf("removed = %+v, want 3 idempotency keys and 3 outbox rows", res)
	}

	if got := countRows(t, pool, "idempotency_keys", "key LIKE 'fresh-%'"); got != 2 {
		t.Fatalf("fresh idempotency keys remaining = %d, want 2", got)
	}
	if got := countRows(t, pool, "idempotency_keys", "key LIKE 'old-%'"); got != 0 {
		t.Fatalf("expired idempotency keys remaining = %d, want 0", got)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 2 {
		t.Fatalf("unpublished outbox rows remaining = %d, want 2 (never swept, however old)", got)
	}
	if got := countOutbox(t, pool, "published_at IS NOT NULL"); got != 1 {
		t.Fatalf("published outbox rows remaining = %d, want 1 (the one inside the retention window)", got)
	}

	// A second sweep is a no-op (idempotent).
	if res, err := h.SweepOnce(ctx); err != nil || res.IdempotencyKeys != 0 || res.OutboxEvents != 0 {
		t.Fatalf("second sweep = (%+v, %v), want a no-op", res, err)
	}
}

// A non-positive TTL/retention disables that table's sweep.
func TestHousekeeper_NonPositiveRetentionDisablesThatSweep(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO idempotency_keys (key, method, path, request_hash, created_at) VALUES ('old','POST','/p','h', now() - interval '30 days')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO outbox_events (topic, event_type, key, value, created_at, published_at) VALUES ('t','e','k','v', now() - interval '30 days', now() - interval '30 days')`); err != nil {
		t.Fatal(err)
	}

	h := postgres.NewHousekeeper(pool, nil, postgres.WithIdempotencyKeyTTL(0), postgres.WithOutboxRetention(-1))
	if res, err := h.SweepOnce(ctx); err != nil || res.IdempotencyKeys != 0 || res.OutboxEvents != 0 {
		t.Fatalf("disabled sweep = (%+v, %v), want a no-op", res, err)
	}
	if countRows(t, pool, "idempotency_keys", "true") != 1 || countOutbox(t, pool, "true") != 1 {
		t.Fatal("a disabled sweep must delete nothing")
	}
}

// Run sweeps immediately, repeats on the interval, and stops on cancel.
func TestHousekeeper_RunSweepsAndStopsOnCancel(t *testing.T) {
	pool := outboxDB(t)
	if _, err := pool.Exec(context.Background(), `INSERT INTO idempotency_keys (key, method, path, request_hash, created_at) VALUES ('old','POST','/p','h', now() - interval '30 days')`); err != nil {
		t.Fatal(err)
	}

	h := postgres.NewHousekeeper(pool, nil, postgres.WithHousekeepingInterval(20*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for countRows(t, pool, "idempotency_keys", "true") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("Run never swept the expired key")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run should return the context error on cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}
