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

// ADR-0033 regression: transfer metadata (transfer_ref, work_kind, site_id,
// quantity) must survive every save/read path exactly like sku/gift_wrap do,
// or WorkReleased for a transfer unit would silently lose its transfer
// context after a re-read from Postgres.

func newTransferUnit(t *testing.T, id string) *workunit.WorkUnit {
	t.Helper()
	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
	unit, err := workunit.NewWorkUnit(id, mustPath(t, "pick-transfer-gw"), cpt, "demand-gw")
	if err != nil {
		t.Fatalf("new work unit: %v", err)
	}
	unit.SetSKU("sku-transfer")
	unit.SetTransferRef("TRF-2026-042")
	unit.SetWorkKind(workunit.WorkKindTransferPick)
	unit.SetSiteId("site-north-1")
	unit.SetQuantity(17)
	return unit
}

func assertTransferUnit(t *testing.T, u *workunit.WorkUnit) {
	t.Helper()
	if u.TransferRef() != "TRF-2026-042" {
		t.Errorf("TransferRef = %q, want TRF-2026-042", u.TransferRef())
	}
	if u.WorkKind() != workunit.WorkKindTransferPick {
		t.Errorf("WorkKind = %q, want TRANSFER_PICK", u.WorkKind())
	}
	if u.SiteId() != "site-north-1" {
		t.Errorf("SiteId = %q, want site-north-1", u.SiteId())
	}
	if u.Quantity() != 17 {
		t.Errorf("Quantity = %d, want 17", u.Quantity())
	}
}

func TestWorkUnitRepo_TransferMetadataRoundTripsThroughEveryFinder(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)

	if err := repo.Save(ctx, newTransferUnit(t, "wu-transfer-yes")); err != nil {
		t.Fatalf("save transfer unit: %v", err)
	}

	byId, err := repo.FindById(ctx, "wu-transfer-yes")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	assertTransferUnit(t, byId)

	byPath, err := repo.FindByPathId(ctx, mustPath(t, "pick-transfer-gw"))
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	if len(byPath) != 1 {
		t.Fatalf("FindByPathId returned %d units, want 1", len(byPath))
	}
	assertTransferUnit(t, byPath[0])

	byRef, err := repo.FindByReference(ctx, "demand-gw")
	if err != nil {
		t.Fatalf("FindByReference: %v", err)
	}
	if len(byRef) != 1 {
		t.Fatalf("FindByReference returned %d units, want 1", len(byRef))
	}
	assertTransferUnit(t, byRef[0])
}

func TestWorkUnitRepo_NonTransferUnitKeepsZeroValues(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)

	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
	unit, err := workunit.NewWorkUnit("wu-transfer-plain", mustPath(t, "pick-transfer-gw"), cpt, "ref-plain")
	if err != nil {
		t.Fatalf("new work unit: %v", err)
	}
	if err := repo.Save(ctx, unit); err != nil {
		t.Fatalf("save plain unit: %v", err)
	}

	got, err := repo.FindById(ctx, "wu-transfer-plain")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got.TransferRef() != "" || got.WorkKind() != "" || got.SiteId() != "" || got.Quantity() != 0 {
		t.Fatalf("plain unit must keep zero-valued transfer metadata, got ref=%q kind=%q site=%q qty=%d",
			got.TransferRef(), got.WorkKind(), got.SiteId(), got.Quantity())
	}
}

func TestWorkUnitRepo_RehydratedTransferWorkKindIsValidated(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	// A row written outside the aggregate (e.g. by an older writer or a
	// manual fixup) whose work_kind is not one of the declared legs must
	// fail rehydration loudly rather than yield an invalid enum value.
	if _, err := pool.Exec(ctx, `
		INSERT INTO work_units (id, path_id, cpt, reference, sku, gift_wrap, state, transfer_ref, work_kind, site_id, quantity)
		VALUES ('wu-transfer-bad', 'pick-transfer-gw', now(), 'ref-bad', '', false, 'Pending', 'TRF-1', 'NOT_A_KIND', 'site-1', 1)
	`); err != nil {
		t.Fatalf("seed bad row: %v", err)
	}
	repo := postgres.NewWorkUnitRepo(pool)
	if _, err := repo.FindById(ctx, "wu-transfer-bad"); err == nil {
		t.Fatal("expected rehydration of an unknown work_kind to fail")
	}
}
