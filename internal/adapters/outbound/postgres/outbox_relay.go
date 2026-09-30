package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
)

// Sink is where OutboxRelay forwards drained rows — in production
// kafka.RelaySink (one topic-less writer routing each message by its own
// Encoded.Topic); in tests a recorder.
type Sink interface {
	Send(ctx context.Context, msgs ...outboundkafka.Encoded) error
}

// OutboxRelay drains unpublished outbox_events rows onto a Sink, oldest
// first, marking each row published as it goes (ADR-0014).
//
// Delivery is at-least-once: a crash between a successful Send and the
// row's UPDATE republishes that row on the next pass. Both topics' consumers
// already dedupe on the envelope's event_id (the analytics projector is
// idempotent on it; integration consumers keep a processed_events table),
// and the republished message carries the same event_id because the wire
// bytes were fixed at encode time. Ordering is preserved per row-id within
// one relay, and FOR UPDATE SKIP LOCKED keeps two relays (a rolling
// deploy's overlapping pods) from claiming the same row.
type OutboxRelay struct {
	pool      *pgxpool.Pool
	sink      Sink
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

// RelayOption customises an OutboxRelay.
type RelayOption func(*OutboxRelay)

// WithInterval sets how long the relay sleeps between passes when the
// last pass found nothing to publish. Default 1s.
func WithInterval(d time.Duration) RelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// WithBatchSize caps how many rows one pass claims. Default 100.
func WithBatchSize(n int) RelayOption {
	return func(r *OutboxRelay) { r.batchSize = n }
}

// NewOutboxRelay constructs a relay draining pool into sink.
func NewOutboxRelay(pool *pgxpool.Pool, sink Sink, logger *slog.Logger, opts ...RelayOption) *OutboxRelay {
	if logger == nil {
		logger = slog.Default()
	}
	r := &OutboxRelay{pool: pool, sink: sink, logger: logger, interval: time.Second, batchSize: 100}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run drains the outbox until ctx is cancelled. A pass that publishes a
// full batch is followed immediately by another pass (there is probably
// more waiting); an empty pass sleeps for the configured interval. A
// failing pass is logged and retried after the interval — the rows stay
// unpublished, so nothing is lost.
func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		n, err := r.RelayOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "outbox relay pass failed", "error", err)
		}
		if n == r.batchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval):
		}
	}
}

// outboxRow pairs a claimed outbox_events row's id with its already-encoded
// wire message.
type outboxRow struct {
	id  int64
	msg outboundkafka.Encoded
}

// RelayOnce performs a single pass: claim up to batchSize unpublished rows
// under a row lock, send each ONE AT A TIME in id order, and mark it
// published. It returns how many rows were published. On the first Send
// failure the pass stops (a later message for the same aggregate must not
// overtake a failed earlier one), records attempts/last_error on that
// row, commits what was already sent, and returns the error.
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: begin relay pass: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	batch, err := claimOutboxRows(ctx, tx, r.batchSize)
	if err != nil {
		return 0, err
	}

	published, err := r.publishBatch(ctx, tx, batch)
	if err != nil {
		return published, err
	}

	if err := tx.Commit(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return published, fmt.Errorf("postgres: commit relay pass: %w", err)
	}
	if published > 0 {
		r.logger.DebugContext(ctx, "outbox relay published events", "count", published)
	}
	return published, nil
}

// claimOutboxRows selects up to batchSize unpublished rows under FOR
// UPDATE SKIP LOCKED and decodes each into its wire message, oldest
// (lowest id) first.
func claimOutboxRows(ctx context.Context, tx pgx.Tx, batchSize int) ([]outboxRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, topic, event_type, key, value, headers
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, batchSize)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim outbox rows: %w", err)
	}
	var batch []outboxRow
	for rows.Next() {
		var p outboxRow
		var headers []byte
		if err := rows.Scan(&p.id, &p.msg.Topic, &p.msg.EventType, &p.msg.Key, &p.msg.Value, &headers); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan outbox row: %w", err)
		}
		if p.msg.Headers, err = unmarshalHeaders(headers); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: decode outbox row %d headers: %w", p.id, err)
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read outbox rows: %w", err)
	}
	return batch, nil
}

// publishBatch sends each claimed row to the sink one at a time in id
// order, marking it published as it goes. On the first Send failure it
// stops immediately (a later message for the same aggregate must not
// overtake a failed earlier one) and hands the failure to abortSend; on a
// mark-published failure the pass returns with the transaction left to
// the caller's rollback — everything already marked stays uncommitted,
// so it is re-drained next pass (at-least-once).
func (r *OutboxRelay) publishBatch(ctx context.Context, tx pgx.Tx, batch []outboxRow) (int, error) {
	published := 0
	for _, p := range batch {
		if err := r.sink.Send(ctx, p.msg); err != nil {
			return published, r.abortSend(ctx, tx, p, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = $1
		`, p.id); err != nil {
			return published, fmt.Errorf("postgres: mark outbox row %d published: %w", p.id, err)
		}
		published++
	}
	return published, nil
}

// abortSend records the failed attempt (attempts + last_error) on the row
// that just failed to send, commits what the pass already accomplished so
// the failure is durable, and returns the wrapped error. The failed row
// stays unpublished and is retried on a later pass.
func (r *OutboxRelay) abortSend(ctx context.Context, tx pgx.Tx, p outboxRow, err error) error {
	if _, uerr := tx.Exec(ctx, `
		UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1
	`, p.id, err.Error()); uerr != nil {
		err = errors.Join(err, fmt.Errorf("postgres: record outbox failure: %w", uerr))
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		err = errors.Join(err, fmt.Errorf("postgres: commit relay pass: %w", cerr))
	}
	return fmt.Errorf("outbox relay: send %s to %s (row %d): %w", p.msg.EventType, p.msg.Topic, p.id, err)
}

func unmarshalHeaders(raw []byte) ([]kafkago.Header, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var hs []outboxHeader
	if err := json.Unmarshal(raw, &hs); err != nil {
		return nil, err
	}
	if len(hs) == 0 {
		return nil, nil
	}
	out := make([]kafkago.Header, len(hs))
	for i, h := range hs {
		out[i] = kafkago.Header{Key: h.Key, Value: []byte(h.Value)}
	}
	return out, nil
}
