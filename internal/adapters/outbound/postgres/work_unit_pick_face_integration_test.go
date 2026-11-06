//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/domain/relocation"
)

// ADR-0048: ForwardPickFaceBySKU reads the SKU's forward-pick face from
// the local slot-plan copy — the RELOCATE work units joined to their
// slot_plans receipts. The ordering (approved_at DESC) and the Cancelled
// exclusion are the contract ReleaseNextWork derives from_bin with; a
// wrong face sends a picker to the wrong aisle.

func TestWorkUnitRepo_ForwardPickFaceBySKU(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)
	plans := postgres.NewSlotPlanRepo(pool)

	// plan-old (approved earlier) put SKU-FACE at FWD-OLD; plan-new
	// (approved later) moved it to FWD-NEW.
	oldApproved := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	newApproved := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	for _, p := range []struct {
		planId, siteId, sku, kind, from, to, state string
		at                                         time.Time
	}{
		{"plan-old", "WH1", "SKU-FACE", "Relocate", "WH1-RSV-A07-03-02-B", "FWD-OLD", "Completed", oldApproved},
		{"plan-new", "WH1", "SKU-FACE", "Relocate", "FWD-OLD", "FWD-NEW", "Released", newApproved},
		// A Vacate never defines a face, whatever its plan's age.
		{"plan-new", "WH1", "SKU-OTHER", "Vacate", "FWD-GONE", "", "Pending", newApproved},
		// A superseded plan's cancelled move never wins, even when it is
		// the newest.
		{"plan-cancelled", "WH1", "SKU-CANCELLED", "Relocate", "R-1", "FWD-CANCELLED", "Cancelled", newApproved.Add(time.Hour)},
		// Another site's face for the same SKU.
		{"plan-wh2", "WH2", "SKU-MULTISITE", "Assign", "", "WH2-FWD-01", "Completed", newApproved},
	} {
		if err := plans.Save(ctx, relocation.Receipt{PlanId: p.planId, SiteId: p.siteId, ApprovedAt: p.at}); err != nil {
			t.Fatalf("save plan %s: %v", p.planId, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO work_units (id, path_id, cpt, reference, sku, gift_wrap, state, work_kind, site_id,
				relocation_plan_id, relocation_move_seq, relocation_kind, from_slot, to_slot)
			VALUES ($1, 'RELOCATE', now(), $2, $3, false, $4, 'RELOCATE', $5, $2, 0, $6, $7, $8)
		`, p.planId+"-move-0-"+p.kind, p.planId, p.sku, p.state, p.siteId, p.kind, p.from, p.to); err != nil {
			t.Fatalf("seed relocate %s: %v", p.planId, err)
		}
	}

	face, err := repo.ForwardPickFaceBySKU(ctx, "WH1", "SKU-FACE")
	if err != nil {
		t.Fatalf("ForwardPickFaceBySKU: %v", err)
	}
	if face != "FWD-NEW" {
		t.Errorf("face = %q, want FWD-NEW (the latest-approved plan's to_slot)", face)
	}

	// A SKU whose only move has no to_slot has no face at all.
	if face, err := repo.ForwardPickFaceBySKU(ctx, "WH1", "SKU-OTHER"); err != nil || face != "" {
		t.Errorf("Vacate-only SKU: face = %q, err = %v; want \"\", nil", face, err)
	}

	// A cancelled move never answers.
	if face, err := repo.ForwardPickFaceBySKU(ctx, "WH1", "SKU-CANCELLED"); err != nil || face != "" {
		t.Errorf("cancelled-only SKU: face = %q, err = %v; want \"\", nil", face, err)
	}

	// Site scoping: WH1 never sees WH2's face; the empty site matches any.
	if face, err := repo.ForwardPickFaceBySKU(ctx, "WH1", "SKU-MULTISITE"); err != nil || face != "" {
		t.Errorf("WH1 lookup of a WH2-only SKU: face = %q, err = %v; want \"\", nil", face, err)
	}
	if face, err := repo.ForwardPickFaceBySKU(ctx, "", "SKU-MULTISITE"); err != nil || face != "WH2-FWD-01" {
		t.Errorf("site-less lookup: face = %q, err = %v; want WH2-FWD-01", face, err)
	}

	// Unknown SKU: no face, no error.
	if face, err := repo.ForwardPickFaceBySKU(ctx, "WH1", "SKU-NOT-PLANNED"); err != nil || face != "" {
		t.Errorf("unknown SKU: face = %q, err = %v; want \"\", nil", face, err)
	}
}
