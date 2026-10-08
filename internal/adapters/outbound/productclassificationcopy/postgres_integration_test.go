//go:build integration

package productclassificationcopy_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/pgtx"
)

// startCopyPostgres boots a throwaway Postgres (testcontainers, never
// DATABASE_URL) and applies the product_classification_copy migration
// itself, so this package needs no other adapter to set up its schema.
func startCopyPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wes_work_planning"),
		tcpostgres.WithUsername("wes"),
		tcpostgres.WithPassword("wes"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	ddl, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "migrations", "0012_product_classification_copy.up.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(ddl)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	return pool
}

func TestStore_Postgres(t *testing.T) {
	ctx := context.Background()
	pool := startCopyPostgres(t)
	store := productclassificationcopy.NewStore(pool, nil)

	t.Run("found maps tags and temperature class, stores DOT class and version", func(t *testing.T) {
		view := classified("SKU-PG-1", "Hazmat", "TemperatureSensitive")
		view.TemperatureClass = "Frozen"
		applied, err := store.ApplyIfNewer(ctx, view, 3, 3, time.Now())
		if err != nil || !applied {
			t.Fatalf("ApplyIfNewer = %v, %v", applied, err)
		}
		got, err := store.GetClassification(ctx, "SKU-PG-1")
		if err != nil {
			t.Fatalf("GetClassification: %v", err)
		}
		if !got.Known || got.TemperatureClass != "Frozen" || !got.HasTag("Hazmat") || !got.HasTag("TemperatureSensitive") {
			t.Fatalf("view = %+v", got)
		}
		var dot *int16
		var version int64
		if err := pool.QueryRow(ctx, `SELECT dot_hazard_class, version FROM product_classification_copy WHERE sku = $1`, "SKU-PG-1").Scan(&dot, &version); err != nil {
			t.Fatalf("read row: %v", err)
		}
		if dot == nil || *dot != 3 || version != 3 {
			t.Fatalf("row dot=%v version=%d, want 3/3", dot, version)
		}
	})

	t.Run("optional fields absent are NULL and map to empty", func(t *testing.T) {
		if _, err := store.ApplyIfNewer(ctx, classified("SKU-PG-2", "Fragile"), 0, 1, time.Now()); err != nil {
			t.Fatalf("ApplyIfNewer: %v", err)
		}
		got, _ := store.GetClassification(ctx, "SKU-PG-2")
		if !got.Known || got.TemperatureClass != "" || !got.HasTag("Fragile") {
			t.Fatalf("view = %+v", got)
		}
		var nulls bool
		if err := pool.QueryRow(ctx, `SELECT temperature_class IS NULL AND dot_hazard_class IS NULL FROM product_classification_copy WHERE sku = $1`, "SKU-PG-2").Scan(&nulls); err != nil || !nulls {
			t.Fatalf("optional columns not NULL (err %v)", err)
		}
	})

	t.Run("unknown SKU is Known=false", func(t *testing.T) {
		got, err := store.GetClassification(ctx, "SKU-PG-none")
		if err != nil || got.Known || got.SKU != "SKU-PG-none" {
			t.Fatalf("view = %+v, %v", got, err)
		}
	})

	t.Run("version guard: equal and older ignored, newer replaces", func(t *testing.T) {
		if applied, _ := store.ApplyIfNewer(ctx, classified("SKU-PG-3", "Hazmat"), 9, 5, time.Now()); !applied {
			t.Fatal("first version must apply")
		}
		for _, stale := range []int64{5, 4} {
			if applied, err := store.ApplyIfNewer(ctx, classified("SKU-PG-3", "Fragile"), 0, stale, time.Now()); err != nil || applied {
				t.Fatalf("version %d over stored 5: applied=%v err=%v", stale, applied, err)
			}
		}
		if got, _ := store.GetClassification(ctx, "SKU-PG-3"); !got.HasTag("Hazmat") || got.HasTag("Fragile") {
			t.Fatalf("stale version overwrote the copy: %+v", got)
		}
		if applied, _ := store.ApplyIfNewer(ctx, classified("SKU-PG-3", "Fragile"), 0, 6, time.Now()); !applied {
			t.Fatal("newer version must apply")
		}
		if got, _ := store.GetClassification(ctx, "SKU-PG-3"); got.HasTag("Hazmat") || !got.HasTag("Fragile") {
			t.Fatalf("newer version not a full replacement: %+v", got)
		}
	})

	t.Run("write joins the ctx transaction and rolls back with it", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if _, err := store.ApplyIfNewer(pgtx.WithTx(ctx, tx), classified("SKU-PG-4", "Hazmat"), 0, 1, time.Now()); err != nil {
			t.Fatalf("ApplyIfNewer in tx: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("rollback: %v", err)
		}
		if got, _ := store.GetClassification(ctx, "SKU-PG-4"); got.Known {
			t.Fatal("a write inside a rolled-back transaction must not survive")
		}
	})

	t.Run("a failed read inside a transaction fails open and leaves the transaction usable", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		// Make the lookup's SELECT fail inside this transaction only.
		if _, err := tx.Exec(ctx, `ALTER TABLE product_classification_copy RENAME TO product_classification_copy_hidden`); err != nil {
			t.Fatalf("rename: %v", err)
		}
		got, err := store.GetClassification(pgtx.WithTx(ctx, tx), "SKU-PG-1")
		if err != nil || got.Known {
			t.Fatalf("view = %+v, %v; want fail-open Known=false, nil", got, err)
		}
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
			t.Fatalf("the release transaction was aborted by the failed lookup: %v", err)
		}
	})

	t.Run("a successful read inside a transaction sees the row", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		got, err := store.GetClassification(pgtx.WithTx(ctx, tx), "SKU-PG-1")
		if err != nil || !got.HasTag("Hazmat") {
			t.Fatalf("view = %+v, %v", got, err)
		}
		if _, err := tx.Exec(ctx, `SELECT 1`); err != nil {
			t.Fatalf("transaction unusable after the lookup: %v", err)
		}
	})
}
