//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/relocation"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0040 on a real Postgres (testcontainers, migrations 0001-0014): the
// relocation columns round-trip, the two relocation finders select exactly
// the Pending units they promise, slot_plans orders a site's plans, and the
// whole plan / sequence / supersede cycle commits atomically through the
// real UnitOfWork.

var relocApprovedAt = time.Date(2026, 10, 15, 8, 0, 0, 0, time.UTC)

func relocationUnitFor(t *testing.T, id, site string, seq int, kind, from, to, blockedBy string) *workunit.WorkUnit {
	t.Helper()
	unit, err := workunit.NewWorkUnit(id, mustPath(t, "RELOCATE"), shared.NewCPT(relocApprovedAt.Add(24*time.Hour)), "plan-pg")
	if err != nil {
		t.Fatalf("new work unit: %v", err)
	}
	move, err := workunit.NewRelocationMove("plan-pg", seq, kind, from, to)
	if err != nil {
		t.Fatalf("new move: %v", err)
	}
	unit.SetWorkKind(workunit.WorkKindRelocate)
	unit.SetSiteId(site)
	unit.SetSKU("SKU-PG")
	unit.SetRelocation(move)
	unit.SetBlockedBy(blockedBy)
	return unit
}

func TestWorkUnitRepo_RelocationColumnsRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)

	blocked := relocationUnitFor(t, "plan-pg-move-1", "SITE-PG", 1, "Relocate", "S2", "S3", "plan-pg-move-2")
	if err := repo.Save(ctx, blocked); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := repo.FindById(ctx, "plan-pg-move-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	m, ok := got.Relocation()
	if !ok || m.PlanId != "plan-pg" || m.MoveSeq != 1 || m.Kind != workunit.RelocationKindRelocate || m.FromSlot != "S2" || m.ToSlot != "S3" {
		t.Fatalf("relocation = (%+v, %v)", m, ok)
	}
	if got.BlockedBy() != "plan-pg-move-2" || got.WorkKind() != workunit.WorkKindRelocate || got.SiteId() != "SITE-PG" {
		t.Fatalf("blocked_by=%q kind=%q site=%q", got.BlockedBy(), got.WorkKind(), got.SiteId())
	}

	at := relocApprovedAt.Add(time.Hour)
	if err := got.Cancel(at); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("save cancelled: %v", err)
	}
	cancelled, err := repo.FindById(ctx, "plan-pg-move-1")
	if err != nil || cancelled.State() != workunit.Cancelled || !cancelled.CancelledAt().Equal(at) {
		t.Fatalf("cancelled round trip = (%v, %v)", cancelled, err)
	}
}

func TestWorkUnitRepo_RelocationFindersSelectOnlyPendingUnits(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)

	pending := relocationUnitFor(t, "plan-pg-move-0", "SITE-PG", 0, "Vacate", "S1", "", "")
	waiting := relocationUnitFor(t, "plan-pg-move-1", "SITE-PG", 1, "Assign", "", "S1", "plan-pg-move-0")
	released := relocationUnitFor(t, "plan-pg-move-2", "SITE-PG", 2, "Vacate", "S5", "", "")
	_ = released.Release(relocApprovedAt)
	otherSite := relocationUnitFor(t, "plan-pg-move-3", "SITE-OTHER", 3, "Vacate", "S7", "", "")
	for _, u := range []*workunit.WorkUnit{pending, waiting, released, otherSite} {
		if err := repo.Save(ctx, u); err != nil {
			t.Fatalf("save %s: %v", u.Id(), err)
		}
	}

	atSite, err := repo.PendingRelocationsAtSite(ctx, "SITE-PG")
	if err != nil {
		t.Fatalf("PendingRelocationsAtSite: %v", err)
	}
	if ids := unitIds(atSite); len(ids) != 2 || ids[0] != "plan-pg-move-0" || ids[1] != "plan-pg-move-1" {
		t.Fatalf("pending at site = %v, want moves 0 and 1", ids)
	}
	waiters, err := repo.PendingBlockedBy(ctx, "plan-pg-move-0")
	if err != nil {
		t.Fatalf("PendingBlockedBy: %v", err)
	}
	if ids := unitIds(waiters); len(ids) != 1 || ids[0] != "plan-pg-move-1" {
		t.Fatalf("waiters = %v, want move 1", ids)
	}
	none, err := repo.PendingBlockedBy(ctx, "nobody")
	if err != nil || none == nil || len(none) != 0 {
		t.Fatalf("PendingBlockedBy(nobody) = (%v, %v), want an empty slice", none, err)
	}
}

func unitIds(units []*workunit.WorkUnit) []string {
	out := make([]string, len(units))
	for i, u := range units {
		out[i] = u.Id()
	}
	return out
}

func TestSlotPlanRepo_LatestAtSiteAndIdempotentSave(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewSlotPlanRepo(pool)

	if _, err := repo.LatestAtSite(ctx, "SITE-PG"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("empty site = %v, want ErrNotFound", err)
	}
	older := relocation.Receipt{PlanId: "plan-a", SiteId: "SITE-PG", ApprovedAt: relocApprovedAt, MoveCount: 3}
	tieWinner := relocation.Receipt{PlanId: "plan-b", SiteId: "SITE-PG", ApprovedAt: relocApprovedAt, SupersedesPlanId: "plan-a", MoveCount: 1}
	otherSite := relocation.Receipt{PlanId: "plan-z", SiteId: "SITE-OTHER", ApprovedAt: relocApprovedAt.Add(time.Hour)}
	for _, r := range []relocation.Receipt{tieWinner, older, otherSite, older} {
		if err := repo.Save(ctx, r); err != nil {
			t.Fatalf("save %s: %v", r.PlanId, err)
		}
	}
	latest, err := repo.LatestAtSite(ctx, "SITE-PG")
	if err != nil {
		t.Fatalf("LatestAtSite: %v", err)
	}
	if latest.PlanId != "plan-b" || latest.SupersedesPlanId != "plan-a" || latest.MoveCount != 1 || !latest.ApprovedAt.Equal(relocApprovedAt) {
		t.Fatalf("latest = %+v, want plan-b (tie broken by plan id)", latest)
	}
}

// TestApplySlotPlanApproved_OnPostgres runs the use cases exactly as
// cmd/wes wires them: plan A, release its unblocked moves, complete the
// vacate (admitting the blocked move), then plan B supersedes A.
func TestApplySlotPlanApproved_OnPostgres(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	plans := postgres.NewSlotPlanRepo(pool)
	publisher := events.NewLogPublisher(nil)
	clock := memory.SystemClock{}
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "RELOCATE", MatchPrefix: "relocate", Intake: []string{pathcatalog.IntakeSlotRelocation}}})

	apply := usecases.NewApplySlotPlanApproved(workUnits, workUnits, pools, plans, postgres.NewProcessedEventRepo(pool), catalogue, publisher, clock).WithUnitOfWork(uow)
	releaseNext := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock).WithUnitOfWork(uow)
	complete := usecases.NewRecordCompletion(workUnits, pools, publisher, clock).WithUnitOfWork(uow).WithRelocationWork(workUnits)
	path := mustPath(t, "RELOCATE")

	planA := usecases.ApplySlotPlanApprovedRequest{EventId: "evt-pg-a", OccurredAt: relocApprovedAt, PlanId: "plan-A", SiteId: "SITE-PG", ApprovedAt: relocApprovedAt,
		Moves: []usecases.SlotPlanMove{
			{SKU: "SKU-1", Kind: "Assign", ToSlot: "S1"},
			{SKU: "SKU-2", Kind: "Relocate", FromSlot: "S2", ToSlot: "S3"},
			{SKU: "SKU-3", Kind: "Vacate", FromSlot: "S3"},
			{SKU: "SKU-4", Kind: "Assign", ToSlot: "S2"},
		}}
	if _, err := apply.Execute(ctx, planA); err != nil {
		t.Fatalf("apply plan A: %v", err)
	}
	// Release moves 0 and 2 (move 1 waits on 2, move 3 waits on 1).
	for _, want := range []string{"plan-A-move-0", "plan-A-move-2"} {
		unit, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: path})
		if err != nil || unit.Id() != want {
			t.Fatalf("release = (%v, %v), want %s", unit, err, want)
		}
	}
	if _, err := complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: "plan-A-move-2"}); err != nil {
		t.Fatalf("complete the vacate: %v", err)
	}
	unit, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: path})
	if err != nil || unit.Id() != "plan-A-move-1" {
		t.Fatalf("release after the vacate = (%v, %v), want plan-A-move-1", unit, err)
	}

	planB := usecases.ApplySlotPlanApprovedRequest{EventId: "evt-pg-b", OccurredAt: relocApprovedAt.Add(time.Hour), PlanId: "plan-B", SiteId: "SITE-PG",
		ApprovedAt: relocApprovedAt.Add(time.Hour), SupersedesPlanId: "plan-A", Moves: []usecases.SlotPlanMove{{SKU: "SKU-9", Kind: "Assign", ToSlot: "S9"}}}
	if _, err := apply.Execute(ctx, planB); err != nil {
		t.Fatalf("apply plan B: %v", err)
	}
	wantStates := map[string]workunit.State{
		"plan-A-move-0": workunit.Released, "plan-A-move-1": workunit.Released, "plan-A-move-2": workunit.Completed,
		"plan-A-move-3": workunit.Cancelled, "plan-B-move-0": workunit.Pending,
	}
	for id, want := range wantStates {
		got, err := workUnits.FindById(ctx, id)
		if err != nil || got.State() != want {
			t.Fatalf("%s = (%v, %v), want %v", id, got, err, want)
		}
	}
	var pooled int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM work_pool_entries WHERE path_id = 'RELOCATE' AND work_unit_id = 'plan-B-move-0' AND state = 'pending'`).Scan(&pooled); err != nil || pooled != 1 {
		t.Fatalf("plan B's move pooled = (%d, %v), want 1", pooled, err)
	}
}
