//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0036: line_no is a nullable column. A set line survives every
// save/read path; a unit with no line stores NULL (not 0) and a row that
// predates the column rehydrates as unknown.

func newLineNoUnit(t *testing.T, id string, lineNo int) *workunit.WorkUnit {
	t.Helper()
	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
	unit, err := workunit.NewWorkUnit(id, mustPath(t, "pick-line-no"), cpt, "order-line-ref")
	if err != nil {
		t.Fatalf("new work unit: %v", err)
	}
	unit.SetLineNo(lineNo)
	return unit
}

func TestWorkUnitRepo_LineNoRoundTripsThroughEveryFinder(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)

	if err := repo.Save(ctx, newLineNoUnit(t, "order-9-line-3", 3)); err != nil {
		t.Fatalf("save: %v", err)
	}

	byId, err := repo.FindById(ctx, "order-9-line-3")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if byId.LineNo() != 3 {
		t.Errorf("FindById LineNo = %d, want 3", byId.LineNo())
	}

	byPath, err := repo.FindByPathId(ctx, mustPath(t, "pick-line-no"))
	if err != nil || len(byPath) != 1 {
		t.Fatalf("FindByPathId: %d units, err %v", len(byPath), err)
	}
	if byPath[0].LineNo() != 3 {
		t.Errorf("FindByPathId LineNo = %d, want 3", byPath[0].LineNo())
	}

	byRef, err := repo.FindByReference(ctx, "order-line-ref")
	if err != nil || len(byRef) != 1 {
		t.Fatalf("FindByReference: %d units, err %v", len(byRef), err)
	}
	if byRef[0].LineNo() != 3 {
		t.Errorf("FindByReference LineNo = %d, want 3", byRef[0].LineNo())
	}
}

func TestWorkUnitRepo_UnknownLineNoIsStoredAsNull(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)

	if err := repo.Save(ctx, newLineNoUnit(t, "wu-no-line", 0)); err != nil {
		t.Fatalf("save: %v", err)
	}
	var isNull bool
	if err := pool.QueryRow(ctx, `SELECT line_no IS NULL FROM work_units WHERE id = 'wu-no-line'`).Scan(&isNull); err != nil {
		t.Fatalf("read line_no: %v", err)
	}
	if !isNull {
		t.Fatal("an unknown line must be stored as NULL, not 0")
	}
	got, err := repo.FindById(ctx, "wu-no-line")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got.LineNo() != 0 {
		t.Fatalf("LineNo = %d, want 0 (unknown)", got.LineNo())
	}
}

// A row written before the column existed (or by an older writer that does
// not list it) has NULL and must rehydrate cleanly as "unknown".
func TestWorkUnitRepo_RowWithoutLineNoRehydratesAsUnknown(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO work_units (id, path_id, cpt, reference, sku, gift_wrap, state)
		VALUES ('wu-legacy', 'pick-line-no', now(), 'order-legacy', '', false, 'Pending')
	`); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	got, err := postgres.NewWorkUnitRepo(pool).FindById(ctx, "wu-legacy")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got.LineNo() != 0 {
		t.Fatalf("LineNo = %d, want 0 (unknown) for a pre-column row", got.LineNo())
	}
}

// Re-saving a unit overwrites line_no (the upsert lists the column).
func TestWorkUnitRepo_SaveUpdatesLineNo(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)

	unit := newLineNoUnit(t, "wu-upsert", 2)
	if err := repo.Save(ctx, unit); err != nil {
		t.Fatalf("save 1: %v", err)
	}
	unit.SetLineNo(5)
	if err := repo.Save(ctx, unit); err != nil {
		t.Fatalf("save 2: %v", err)
	}
	got, err := repo.FindById(ctx, "wu-upsert")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got.LineNo() != 5 {
		t.Fatalf("LineNo = %d, want 5 after re-save", got.LineNo())
	}
}
