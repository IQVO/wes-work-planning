package productclassificationcopy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
	"github.com/claudioed/wes-work-planning/internal/pgtx"
)

// querier is the subset of pgx that both *pgxpool.Pool and pgx.Tx satisfy.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store is the Postgres local copy (table product_classification_copy,
// migration 0012). Writes join the UnitOfWork transaction bound to ctx
// (internal/pgtx), so the ProductClassified consumer's processed-event claim
// and the upsert commit together (ADR-0028). cmd/mcp uses it read-only.
type Store struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

var (
	_ ports.ProductClassificationLookup   = (*Store)(nil)
	_ ports.ProductClassificationCopyRepo = (*Store)(nil)
)

// NewStore constructs a Store over pool. logger receives the WARN logged
// when a lookup fails open; nil silences it.
func NewStore(pool *pgxpool.Pool, logger *slog.Logger) *Store {
	return &Store{pool: pool, logger: logger}
}

const upsertIfNewerSQL = `
	INSERT INTO product_classification_copy (sku, handling_tags, temperature_class, dot_hazard_class, version, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (sku) DO UPDATE SET
		handling_tags     = EXCLUDED.handling_tags,
		temperature_class = EXCLUDED.temperature_class,
		dot_hazard_class  = EXCLUDED.dot_hazard_class,
		version           = EXCLUDED.version,
		updated_at        = EXCLUDED.updated_at
	WHERE product_classification_copy.version < EXCLUDED.version`

const selectClassificationSQL = `
	SELECT handling_tags, temperature_class FROM product_classification_copy WHERE sku = $1`

// ApplyIfNewer implements ports.ProductClassificationCopyRepo with ONE
// statement: insert when absent, overwrite only when the stored version is
// lower. An equal or newer stored version affects no row (applied=false).
func (s *Store) ApplyIfNewer(ctx context.Context, view productclassificationview.ProductClassificationView, dotHazardClass int, version int64, updatedAt time.Time) (bool, error) {
	tags := view.HandlingTags
	if tags == nil {
		tags = []string{}
	}
	tag, err := s.querier(ctx).Exec(ctx, upsertIfNewerSQL,
		view.SKU, tags, nullableText(view.TemperatureClass), nullableSmallint(dotHazardClass), version, updatedAt.UTC())
	if err != nil {
		return false, fmt.Errorf("product classification copy: upsert %s: %w", view.SKU, err)
	}
	return tag.RowsAffected() == 1, nil
}

// GetClassification implements ports.ProductClassificationLookup. It never
// returns an error: an unknown SKU and an unreadable copy are both
// Known=false (fail-open, ADR-0009 §5).
//
// The WorkReleased encoder calls it INSIDE the release transaction (the
// outbox row is encoded there). A failed statement would abort that whole
// transaction in Postgres, turning a fail-open lookup into a failed release,
// so when ctx carries a transaction the read runs in a savepoint that is
// rolled back on failure. Outside a transaction it reads through the pool.
func (s *Store) GetClassification(ctx context.Context, sku string) (productclassificationview.ProductClassificationView, error) {
	view, err := s.read(ctx, sku)
	if err != nil {
		if s.logger != nil {
			s.logger.WarnContext(ctx, "product classification copy unreadable; releasing without hints", "sku", sku, "error", err)
		}
		return unknown(sku), nil
	}
	return view, nil
}

func (s *Store) read(ctx context.Context, sku string) (productclassificationview.ProductClassificationView, error) {
	tx, inTx := pgtx.TxFrom(ctx)
	if !inTx {
		return scanClassification(s.pool.QueryRow(ctx, selectClassificationSQL, sku), sku)
	}
	savepoint, err := tx.Begin(ctx)
	if err != nil {
		return productclassificationview.ProductClassificationView{}, fmt.Errorf("savepoint: %w", err)
	}
	view, err := scanClassification(savepoint.QueryRow(ctx, selectClassificationSQL, sku), sku)
	if err != nil {
		_ = savepoint.Rollback(ctx)
		return productclassificationview.ProductClassificationView{}, err
	}
	if err := savepoint.Commit(ctx); err != nil {
		return productclassificationview.ProductClassificationView{}, fmt.Errorf("release savepoint: %w", err)
	}
	return view, nil
}

// scanClassification maps one row to the view; no row is Known=false.
func scanClassification(row pgx.Row, sku string) (productclassificationview.ProductClassificationView, error) {
	var (
		tags             []string
		temperatureClass *string
	)
	if err := row.Scan(&tags, &temperatureClass); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return unknown(sku), nil
		}
		return productclassificationview.ProductClassificationView{}, err
	}
	view := productclassificationview.ProductClassificationView{SKU: sku, HandlingTags: tags, Known: true}
	if temperatureClass != nil {
		view.TemperatureClass = *temperatureClass
	}
	return view, nil
}

// querier resolves the UnitOfWork transaction bound to ctx, or the pool.
func (s *Store) querier(ctx context.Context) querier {
	if tx, ok := pgtx.TxFrom(ctx); ok {
		return tx
	}
	return s.pool
}

func nullableText(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func nullableSmallint(v int) *int16 {
	if v == 0 {
		return nil
	}
	n := int16(v) // #nosec G115 -- DOT hazard classes are 1..9, validated by the consumer.
	return &n
}
