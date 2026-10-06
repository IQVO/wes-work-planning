//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/domain/charge"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/plan"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// TestPostgresRepos boots its own throwaway Postgres via testcontainers
// (outboxDB, with the real migrations applied); it needs only Docker.
func TestPostgresRepos(t *testing.T) {
	ctx := context.Background()
	pool := outboxDB(t)

	pathId, _ := shared.NewPathId("integration-pick-a")

	t.Run("charge repo round trip", func(t *testing.T) {
		repo := postgres.NewChargeRepo(pool)
		qty, _ := shared.NewQuantity(42)
		cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))

		f, err := charge.NewChargeForecast(pathId, []charge.CPTBucket{{CPT: cpt, Quantity: qty}}, time.Now().Truncate(time.Microsecond))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := repo.Save(ctx, f); err != nil {
			t.Fatalf("save: %v", err)
		}

		got, err := repo.FindByPathId(ctx, pathId)
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if got.TotalQuantity().Value() != 42 {
			t.Fatalf("got total %d, want 42", got.TotalQuantity().Value())
		}
	})

	t.Run("plan repo round trip", func(t *testing.T) {
		repo := postgres.NewPlanRepo(pool)
		heads, _ := shared.NewStationCount(3)
		installed, _ := shared.NewStationCount(5)
		rate, _ := shared.NewRate(40)

		pp, err := plan.NewPathPlan(pathId, heads, installed, rate, 8)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		sp, err := plan.NewShiftPlan([]plan.PathPlan{pp})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := repo.Save(ctx, pathId, sp); err != nil {
			t.Fatalf("save: %v", err)
		}

		got, err := repo.FindByPathId(ctx, pathId)
		if err != nil {
			t.Fatalf("find: %v", err)
		}
		if _, ok := got.PathPlan(pathId); !ok {
			t.Fatalf("expected path plan to round trip")
		}
	})

	t.Run("plan repo round trips the ADR-0017 travel-distance hint", func(t *testing.T) {
		repo := postgres.NewPlanRepo(pool)
		heads, _ := shared.NewStationCount(3)
		installed, _ := shared.NewStationCount(5)
		rate, _ := shared.NewRate(40)

		save := func(p shared.PathId, hint func(*plan.PathPlan)) {
			t.Helper()
			pp, err := plan.NewPathPlan(p, heads, installed, rate, 8)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if hint != nil {
				hint(&pp)
			}
			sp, err := plan.NewShiftPlan([]plan.PathPlan{pp})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := repo.Save(ctx, p, sp); err != nil {
				t.Fatalf("save: %v", err)
			}
		}
		load := func(p shared.PathId) plan.PathPlan {
			t.Helper()
			sp, err := repo.FindByPathId(ctx, p)
			if err != nil {
				t.Fatalf("find: %v", err)
			}
			pp, ok := sp.PathPlan(p)
			if !ok {
				t.Fatalf("expected path plan to round trip")
			}
			return pp
		}

		withHint, _ := shared.NewPathId("integration-travel-hint")
		save(withHint, func(pp *plan.PathPlan) { pp.SetTravelDistance(42.5, true) })
		got := load(withHint)
		if !got.TravelDistanceKnown() || got.TravelDistanceM() != 42.5 || !got.TravelDistanceEstimated() {
			t.Fatalf("hint did not round trip: known=%v m=%v est=%v", got.TravelDistanceKnown(), got.TravelDistanceM(), got.TravelDistanceEstimated())
		}

		// A zero distance is a valid hint and must stay distinct from "no hint".
		zero, _ := shared.NewPathId("integration-travel-zero")
		save(zero, func(pp *plan.PathPlan) { pp.SetTravelDistance(0, false) })
		got = load(zero)
		if !got.TravelDistanceKnown() || got.TravelDistanceM() != 0 || got.TravelDistanceEstimated() {
			t.Fatalf("zero hint did not round trip: known=%v m=%v est=%v", got.TravelDistanceKnown(), got.TravelDistanceM(), got.TravelDistanceEstimated())
		}

		// No hint stays "not known" (NULL columns).
		none, _ := shared.NewPathId("integration-travel-none")
		save(none, nil)
		if load(none).TravelDistanceKnown() {
			t.Fatalf("path plan without a hint must not report a known distance")
		}

		// Re-committing without a hint clears a previously stored one (upsert).
		save(withHint, nil)
		if load(withHint).TravelDistanceKnown() {
			t.Fatalf("re-commit without a hint must clear the stored hint")
		}
	})

	t.Run("work pool and work unit repo round trip", func(t *testing.T) {
		poolRepo := postgres.NewWorkPoolRepo(pool)
		unitRepo := postgres.NewWorkUnitRepo(pool)

		wp := release.NewWorkPool(pathId, release.ReleaseFed, 10, 5)
		cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
		if err := wp.Enqueue("integration-wu-1", cpt); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := poolRepo.Save(ctx, wp); err != nil {
			t.Fatalf("save pool: %v", err)
		}

		unit, err := workunit.NewWorkUnit("integration-wu-1", pathId, cpt, "ref-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := unitRepo.Save(ctx, unit); err != nil {
			t.Fatalf("save unit: %v", err)
		}

		gotPool, err := poolRepo.FindByPathId(ctx, pathId)
		if err != nil {
			t.Fatalf("find pool: %v", err)
		}
		if gotPool.BacklogDepth() != 1 {
			t.Fatalf("got backlog depth %d, want 1", gotPool.BacklogDepth())
		}

		gotUnit, err := unitRepo.FindById(ctx, "integration-wu-1")
		if err != nil {
			t.Fatalf("find unit: %v", err)
		}
		if gotUnit.State() != workunit.Pending {
			t.Fatalf("got state %v, want Pending", gotUnit.State())
		}
	})

	t.Run("work unit repo FindByReference", func(t *testing.T) {
		unitRepo := postgres.NewWorkUnitRepo(pool)
		cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
		reference := "integration-order-77213-line-1"

		unit1, err := workunit.NewWorkUnit("integration-ref-wu-1", pathId, cpt, reference)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := unitRepo.Save(ctx, unit1); err != nil {
			t.Fatalf("save unit 1: %v", err)
		}
		unit2, err := workunit.NewWorkUnit("integration-ref-wu-2", pathId, cpt, reference)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := unitRepo.Save(ctx, unit2); err != nil {
			t.Fatalf("save unit 2: %v", err)
		}
		otherUnit, err := workunit.NewWorkUnit("integration-ref-wu-other", pathId, cpt, "integration-other-ref")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := unitRepo.Save(ctx, otherUnit); err != nil {
			t.Fatalf("save other unit: %v", err)
		}

		got, err := unitRepo.FindByReference(ctx, reference)
		if err != nil {
			t.Fatalf("find by reference: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d units, want 2", len(got))
		}
		for _, u := range got {
			if u.Reference() != reference {
				t.Fatalf("got reference %q, want %q", u.Reference(), reference)
			}
		}

		none, err := unitRepo.FindByReference(ctx, "integration-nonexistent-ref")
		if err != nil {
			t.Fatalf("find by reference (no match): %v", err)
		}
		if len(none) != 0 {
			t.Fatalf("got %d units, want 0", len(none))
		}
	})

	t.Run("labor plan view, inventory view, and idempotent processed events", func(t *testing.T) {
		laborRepo := postgres.NewLaborPlanViewRepo(pool)
		inventoryRepo := postgres.NewInventoryViewRepo(pool)
		processedRepo := postgres.NewProcessedEventRepo(pool)

		observedAt := time.Now().Truncate(time.Microsecond)
		view := laborview.LaborPlanObserved{
			PathId: pathId, PlannedHeads: 4, PlannedRate: 90, PlannedHours: 8, ObservedAt: observedAt,
		}
		if err := laborRepo.Save(ctx, view); err != nil {
			t.Fatalf("save labor plan view: %v", err)
		}
		gotLabor, err := laborRepo.FindByPathId(ctx, pathId)
		if err != nil {
			t.Fatalf("find labor plan view: %v", err)
		}
		if gotLabor.PlannedHeads != 4 {
			t.Fatalf("got planned heads %d, want 4", gotLabor.PlannedHeads)
		}

		sku := "integration-sku-1"
		if _, err := inventoryRepo.ApplyDelta(ctx, sku, -5, observedAt); err != nil {
			t.Fatalf("apply delta: %v", err)
		}
		gotInv, err := inventoryRepo.ApplyDelta(ctx, sku, 2, observedAt)
		if err != nil {
			t.Fatalf("apply delta: %v", err)
		}
		if gotInv.UsableQuantity != -3 {
			t.Fatalf("got usable quantity %d, want -3", gotInv.UsableQuantity)
		}
		gotInvFind, err := inventoryRepo.FindBySKU(ctx, sku)
		if err != nil {
			t.Fatalf("find by sku: %v", err)
		}
		if gotInvFind.UsableQuantity != -3 {
			t.Fatalf("got usable quantity %d, want -3", gotInvFind.UsableQuantity)
		}

		eventId := "integration-evt-1"
		alreadyProcessed, err := processedRepo.TryMarkProcessed(ctx, eventId, observedAt)
		if err != nil {
			t.Fatalf("try mark processed: %v", err)
		}
		if alreadyProcessed {
			t.Fatalf("expected first mark to report alreadyProcessed=false")
		}
		alreadyProcessed, err = processedRepo.TryMarkProcessed(ctx, eventId, observedAt)
		if err != nil {
			t.Fatalf("try mark processed (redelivery): %v", err)
		}
		if !alreadyProcessed {
			t.Fatalf("expected redelivery to report alreadyProcessed=true")
		}
	})
}
