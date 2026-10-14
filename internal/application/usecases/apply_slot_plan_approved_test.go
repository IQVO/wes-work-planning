package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/relocation"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// These tests pin ADR-0040's ApplySlotPlanApproved: one RELOCATE work unit
// per move, a move into a slot another move vacates stays out of the pool
// until that move completes, a newer plan cancels the site's not-yet-
// released units, and replays are no-ops.

var slotApprovedAt = time.Date(2026, 10, 15, 8, 0, 0, 0, time.UTC)

type slotFixture struct {
	workUnits *memory.WorkUnitRepo
	pools     *memory.WorkPoolRepo
	plans     *memory.SlotPlanRepo
	uc        *usecases.ApplySlotPlanApproved
	release   *usecases.ReleaseNextWork
	complete  *usecases.RecordCompletion
	pathId    shared.PathId
}

func relocateCatalogue(intake ...string) *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}, Intake: []string{"CUSTOMER_ORDER"}},
		{Id: "RELOCATE", MatchPrefix: "relocate", RequiredCapabilities: []string{"relocate"}, Intake: intake},
	})
}

func newSlotFixture(t *testing.T, catalogue *pathcatalog.Catalogue) slotFixture {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	plans := memory.NewSlotPlanRepo()
	clock := memory.FixedClock{At: slotApprovedAt.Add(time.Minute)}
	publisher := events.NewLogPublisher(nil)
	pathId, err := shared.NewPathId("RELOCATE")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	return slotFixture{
		workUnits: workUnits,
		pools:     pools,
		plans:     plans,
		uc:        usecases.NewApplySlotPlanApproved(workUnits, workUnits, pools, plans, memory.NewProcessedEventRepo(), catalogue, publisher, clock),
		release:   usecases.NewReleaseNextWork(pools, workUnits, publisher, clock),
		complete:  usecases.NewRecordCompletion(workUnits, pools, publisher, clock).WithRelocationWork(workUnits),
		pathId:    pathId,
	}
}

func slotMove(sku, kind, from, to string) usecases.SlotPlanMove {
	return usecases.SlotPlanMove{SKU: sku, Kind: kind, FromSlot: from, ToSlot: to}
}

func slotRequest(eventId, planId string, at time.Time, moves ...usecases.SlotPlanMove) usecases.ApplySlotPlanApprovedRequest {
	return usecases.ApplySlotPlanApprovedRequest{
		EventId:    eventId,
		OccurredAt: at,
		PlanId:     planId,
		SiteId:     "SITE-1",
		ApprovedAt: at,
		Moves:      moves,
	}
}

// The ADR's sample plan: SKU-1 assigned into S1, SKU-2 relocated from S2
// into S3, SKU-3 vacated out of S3 (so move 1 waits on move 2).
func samplePlan(eventId, planId string, at time.Time) usecases.ApplySlotPlanApprovedRequest {
	return slotRequest(eventId, planId, at,
		slotMove("SKU-1", "Assign", "", "S1"),
		slotMove("SKU-2", "Relocate", "S2", "S3"),
		slotMove("SKU-3", "Vacate", "S3", ""),
	)
}

func (f slotFixture) unit(t *testing.T, id string) *workunit.WorkUnit {
	t.Helper()
	u, err := f.workUnits.FindById(context.Background(), id)
	if err != nil {
		t.Fatalf("FindById %s: %v", id, err)
	}
	return u
}

func (f slotFixture) poolIds(t *testing.T) map[string]release.PoolEntrySnapshot {
	t.Helper()
	pool, err := f.pools.FindByPathId(context.Background(), f.pathId)
	if err != nil {
		t.Fatalf("RELOCATE pool: %v", err)
	}
	out := map[string]release.PoolEntrySnapshot{}
	for _, e := range pool.Entries() {
		out[e.WorkUnitId] = e
	}
	return out
}

func mustApply(t *testing.T, f slotFixture, req usecases.ApplySlotPlanApprovedRequest) usecases.SlotPlanOutcome {
	t.Helper()
	outcome, err := f.uc.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return outcome
}

func TestApplySlotPlanApproved_PlansOneRelocateUnitPerMove(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	if got := mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt)); got != usecases.SlotPlanApplied {
		t.Fatalf("outcome = %v, want SlotPlanApplied", got)
	}
	u := f.unit(t, "plan-A-move-1")
	m, ok := u.Relocation()
	if !ok || m.PlanId != "plan-A" || m.MoveSeq != 1 || m.Kind != workunit.RelocationKindRelocate || m.FromSlot != "S2" || m.ToSlot != "S3" {
		t.Fatalf("relocation = (%+v, %v)", m, ok)
	}
	assertRelocateUnit(t, u, "SKU-2")
	assertRelocateUnit(t, f.unit(t, "plan-A-move-0"), "SKU-1")
	assertRelocateUnit(t, f.unit(t, "plan-A-move-2"), "SKU-3")
}

func assertRelocateUnit(t *testing.T, u *workunit.WorkUnit, sku string) {
	t.Helper()
	if u.PathId().String() != "RELOCATE" || u.Reference() != "plan-A" || u.WorkKind() != workunit.WorkKindRelocate {
		t.Fatalf("%s: path=%s ref=%s kind=%s", u.Id(), u.PathId(), u.Reference(), u.WorkKind())
	}
	if u.SiteId() != "SITE-1" || u.SKU() != sku || u.State() != workunit.Pending {
		t.Fatalf("%s: site=%s sku=%s state=%v", u.Id(), u.SiteId(), u.SKU(), u.State())
	}
	if !u.CPT().Time().Equal(slotApprovedAt.Add(usecases.DefaultRelocationCPTHorizon)) {
		t.Fatalf("%s: cpt = %v, want approved_at + 24h", u.Id(), u.CPT().Time())
	}
}

func TestApplySlotPlanApproved_BlockedMoveStaysOutOfThePool(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))

	if got := f.unit(t, "plan-A-move-1").BlockedBy(); got != "plan-A-move-2" {
		t.Fatalf("move 1 BlockedBy = %q, want plan-A-move-2", got)
	}
	entries := f.poolIds(t)
	if _, pooled := entries["plan-A-move-1"]; pooled || len(entries) != 2 {
		t.Fatalf("pool = %v, want moves 0 and 2 only", entries)
	}
}

func TestApplySlotPlanApproved_VacateCompletionAdmitsTheBlockedMove(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	ctx := context.Background()
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))

	released := releaseAll(t, f)
	if len(released) != 2 || released["plan-A-move-1"] {
		t.Fatalf("released before the vacate completed = %v, want moves 0 and 2", released)
	}
	if _, err := f.complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: "plan-A-move-2"}); err != nil {
		t.Fatalf("complete the vacate: %v", err)
	}
	if got := f.unit(t, "plan-A-move-1"); got.IsBlocked() || got.State() != workunit.Pending {
		t.Fatalf("move 1 after the vacate: blocked=%q state=%v", got.BlockedBy(), got.State())
	}
	unit, err := f.release.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: f.pathId})
	if err != nil || unit.Id() != "plan-A-move-1" {
		t.Fatalf("release after the vacate = (%v, %v), want plan-A-move-1", unit, err)
	}
}

func TestApplySlotPlanApproved_CompletingAnUnrelatedUnitAdmitsNothing(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	ctx := context.Background()
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))
	releaseAll(t, f)
	if _, err := f.complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: "plan-A-move-0"}); err != nil {
		t.Fatalf("complete move 0: %v", err)
	}
	if !f.unit(t, "plan-A-move-1").IsBlocked() {
		t.Fatal("move 1 must stay blocked until ITS vacate completes")
	}
}

// releaseAll releases every releasable unit of the RELOCATE pool.
func releaseAll(t *testing.T, f slotFixture) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for {
		unit, err := f.release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: f.pathId})
		if errors.Is(err, release.ErrEmptyPool) {
			return out
		}
		if err != nil {
			t.Fatalf("release: %v", err)
		}
		out[unit.Id()] = true
	}
}

func TestApplySlotPlanApproved_NewerPlanCancelsTheOlderPlansUnreleasedWork(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	ctx := context.Background()
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))
	// Move 0 is handed out to the floor; moves 1 (blocked) and 2 (pooled)
	// are still unreleased.
	if unit, err := f.release.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: f.pathId}); err != nil || unit.Id() != "plan-A-move-0" {
		t.Fatalf("release = (%v, %v), want plan-A-move-0", unit, err)
	}

	later := slotApprovedAt.Add(time.Hour)
	newer := slotRequest("evt-2", "plan-B", later, slotMove("SKU-9", "Assign", "", "S9"))
	newer.SupersedesPlanId = "plan-A"
	if got := mustApply(t, f, newer); got != usecases.SlotPlanApplied {
		t.Fatalf("outcome = %v, want SlotPlanApplied", got)
	}

	if got := f.unit(t, "plan-A-move-0").State(); got != workunit.Released {
		t.Fatalf("released move 0 = %v, want it to run to completion (Released)", got)
	}
	for _, id := range []string{"plan-A-move-1", "plan-A-move-2"} {
		u := f.unit(t, id)
		if u.State() != workunit.Cancelled || u.CancelledAt() == nil {
			t.Fatalf("%s = %v, want Cancelled", id, u.State())
		}
	}
	entries := f.poolIds(t)
	if _, ok := entries["plan-A-move-2"]; ok {
		t.Fatal("the cancelled pooled unit must be withdrawn from the pool")
	}
	if _, ok := entries["plan-B-move-0"]; !ok {
		t.Fatalf("plan-B's move must be pooled, pool = %v", entries)
	}
	if _, ok := entries["plan-A-move-0"]; !ok {
		t.Fatal("the released unit's entry must stay")
	}
	latest, err := f.plans.LatestAtSite(ctx, "SITE-1")
	if err != nil || latest.PlanId != "plan-B" || latest.SupersedesPlanId != "plan-A" {
		t.Fatalf("latest plan = (%+v, %v), want plan-B", latest, err)
	}
}

func TestApplySlotPlanApproved_OlderPlanArrivingLateIsStale(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	later := slotApprovedAt.Add(time.Hour)
	mustApply(t, f, slotRequest("evt-2", "plan-B", later, slotMove("SKU-9", "Assign", "", "S9")))

	if got := mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt)); got != usecases.SlotPlanStale {
		t.Fatalf("outcome = %v, want SlotPlanStale", got)
	}
	if _, err := f.workUnits.FindById(context.Background(), "plan-A-move-0"); err == nil {
		t.Fatal("a stale plan must plan nothing")
	}
	if got := f.unit(t, "plan-B-move-0").State(); got != workunit.Pending {
		t.Fatalf("the newer plan's unit = %v, want untouched Pending", got)
	}
}

func TestApplySlotPlanApproved_ReplaysAreNoOps(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))

	if got := mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt)); got != usecases.SlotPlanAlreadyProcessed {
		t.Fatalf("same event id = %v, want SlotPlanAlreadyProcessed", got)
	}
	if got := mustApply(t, f, samplePlan("evt-other", "plan-A", slotApprovedAt)); got != usecases.SlotPlanAlreadyApplied {
		t.Fatalf("same plan, new event id = %v, want SlotPlanAlreadyApplied", got)
	}
	if len(f.poolIds(t)) != 2 {
		t.Fatalf("a replay must not enqueue twice: %v", f.poolIds(t))
	}
	if got := f.unit(t, "plan-A-move-0").State(); got != workunit.Pending {
		t.Fatalf("a replay must not cancel its own plan's units: %v", got)
	}
}

func TestApplySlotPlanApproved_IntakeMustAdmitSlotRelocation(t *testing.T) {
	admitted := newSlotFixture(t, relocateCatalogue(pathcatalog.IntakeSlotRelocation))
	mustApply(t, admitted, samplePlan("evt-1", "plan-A", slotApprovedAt))

	refused := newSlotFixture(t, relocateCatalogue("CUSTOMER_ORDER"))
	_, err := refused.uc.Execute(context.Background(), samplePlan("evt-1", "plan-A", slotApprovedAt))
	if !errors.Is(err, usecases.ErrPathRefusesSlotRelocation) {
		t.Fatalf("err = %v, want ErrPathRefusesSlotRelocation", err)
	}
}

func TestApplySlotPlanApproved_PoisonIsAnErrorAndPlansNothing(t *testing.T) {
	cases := []struct {
		name      string
		catalogue *pathcatalog.Catalogue
		req       usecases.ApplySlotPlanApprovedRequest
		want      error
	}{
		{"unknown move kind", relocateCatalogue(), slotRequest("e", "p", slotApprovedAt, slotMove("SKU", "Swap", "A", "B")), workunit.ErrUnknownRelocationKind},
		{"move without its slot", relocateCatalogue(), slotRequest("e", "p", slotApprovedAt, slotMove("SKU", "Vacate", "", "")), workunit.ErrInvalidRelocationMove},
		{"no site", relocateCatalogue(), usecases.ApplySlotPlanApprovedRequest{EventId: "e", PlanId: "p", ApprovedAt: slotApprovedAt}, relocation.ErrInvalidPlan},
		{"RELOCATE not in the catalogue", pathcatalog.New([]pathcatalog.PathDefinition{{Id: "PICK", MatchPrefix: "pick"}}), samplePlan("e", "p", slotApprovedAt), pathcatalog.ErrUnknownPath},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSlotFixture(t, tc.catalogue)
			if _, err := f.uc.Execute(context.Background(), tc.req); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			// The processed mark was rolled back: a retry fails the same way.
			if _, err := f.uc.Execute(context.Background(), tc.req); !errors.Is(err, tc.want) {
				t.Fatalf("retry err = %v, want %v again", err, tc.want)
			}
			if _, err := f.plans.LatestAtSite(context.Background(), "SITE-1"); err == nil {
				t.Fatal("a poison plan must record nothing")
			}
		})
	}
}

func TestApplySlotPlanApproved_CPTHorizonIsConfigurable(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	f.uc.WithCPTHorizon(2 * time.Hour)
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))
	if got := f.unit(t, "plan-A-move-0").CPT().Time(); !got.Equal(slotApprovedAt.Add(2 * time.Hour)) {
		t.Fatalf("cpt = %v, want approved_at + 2h", got)
	}
	f.uc.WithCPTHorizon(0) // non-positive keeps the current horizon
	mustApply(t, f, samplePlan("evt-2", "plan-B", slotApprovedAt.Add(time.Hour)))
	if got := f.unit(t, "plan-B-move-0").CPT().Time(); !got.Equal(slotApprovedAt.Add(3 * time.Hour)) {
		t.Fatalf("cpt = %v, want approved_at + 2h (0 ignored)", got)
	}
}

func TestApplySlotPlanApproved_PlanWithNoMovesOnlyCancels(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))
	mustApply(t, f, slotRequest("evt-2", "plan-B", slotApprovedAt.Add(time.Hour)))
	for _, id := range []string{"plan-A-move-0", "plan-A-move-1", "plan-A-move-2"} {
		if got := f.unit(t, id).State(); got != workunit.Cancelled {
			t.Fatalf("%s = %v, want Cancelled", id, got)
		}
	}
	if len(f.poolIds(t)) != 0 {
		t.Fatalf("pool = %v, want empty", f.poolIds(t))
	}
}

func TestApplySlotPlanApproved_OtherSitesAreUntouched(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))
	other := slotRequest("evt-2", "plan-Z", slotApprovedAt.Add(time.Hour), slotMove("SKU-9", "Assign", "", "S9"))
	other.SiteId = "SITE-2"
	mustApply(t, f, other)
	if got := f.unit(t, "plan-A-move-0").State(); got != workunit.Pending {
		t.Fatalf("another site's plan cancelled SITE-1 work: %v", got)
	}
}

func TestReleaseNextWork_CancelledEntryIsNeverHandedOut(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	ctx := context.Background()
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))
	// Simulate a pool entry left pending while its unit was cancelled (the
	// two are written in one transaction, so only a lost update can do it).
	u := f.unit(t, "plan-A-move-0")
	if err := u.Cancel(slotApprovedAt); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := f.workUnits.Save(ctx, u); err != nil {
		t.Fatalf("Save: %v", err)
	}
	released := releaseAll(t, f)
	if released["plan-A-move-0"] || !released["plan-A-move-2"] {
		t.Fatalf("released = %v, want move 2 only", released)
	}
	pool, err := f.pools.FindByPathId(ctx, f.pathId)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if pool.WIP() != 1 {
		t.Fatalf("WIP = %d, want 1: a cancelled unit must not hold a WIP slot", pool.WIP())
	}
}
