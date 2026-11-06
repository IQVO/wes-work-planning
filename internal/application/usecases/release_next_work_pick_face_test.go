package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0048: ReleaseNextWork derives a pick plan's FromBin at release time
// from the local slot-plan copy (the SKU's forward-pick face), never
// overriding a caller-stated bin and never blocking a release.

// relocateUnit builds and stores the RELOCATE work unit a consumed
// SlotPlanApproved move becomes (ADR-0040): the local copy the face is
// derived from.
func relocateUnit(t *testing.T, repo *memory.WorkUnitRepo, planId, siteId, sku, fromSlot, toSlot string) {
	t.Helper()
	pathId, err := shared.NewPathId("RELOCATE")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	unit, err := workunit.NewWorkUnit(planId+"-move-0", pathId, shared.NewCPT(time.Date(2026, 11, 5, 23, 0, 0, 0, time.UTC)), planId)
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	move, err := workunit.NewRelocationMove(planId, 0, "Relocate", fromSlot, toSlot)
	if err != nil {
		t.Fatalf("NewRelocationMove: %v", err)
	}
	unit.SetWorkKind(workunit.WorkKindRelocate)
	unit.SetSiteId(siteId)
	unit.SetSKU(sku)
	unit.SetRelocation(move)
	if err := repo.Save(context.Background(), unit); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

// enqueuePlanlessPick enqueues an EACH_PICK unit whose caller stated no
// bin — the unit the derivation is for.
func enqueuePlanlessPick(t *testing.T, f fixture, id string) shared.PathId {
	t.Helper()
	plan, err := workunit.NewPickPlan("SKU-FACE", 6, "EACH", "")
	if err != nil {
		t.Fatalf("NewPickPlan: %v", err)
	}
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	if _, err := enqueue.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: id, PathId: pathId,
		CPT:       shared.NewCPT(f.clock.Now().Add(time.Hour)),
		Reference: "order-" + id, SKU: "SKU-FACE", Pick: &plan,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return pathId
}

func TestReleaseNextWork_DerivesPickFromBinFromForwardPickFace(t *testing.T) {
	f := newFixture()
	relocateUnit(t, f.workUnits, "plan-2026-11-05", "WH1", "SKU-FACE", "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-02-A")
	pathId := enqueuePlanlessPick(t, f, "wu-derive-1")
	release := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock).WithPickFaces(f.workUnits)

	released, err := release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	plan, ok := released.Pick()
	if !ok {
		t.Fatal("expected the released unit to keep its pick plan")
	}
	if plan.FromBin != "WH1-FWD-A01-01-02-A" {
		t.Fatalf("FromBin = %q, want the SKU's forward-pick face WH1-FWD-A01-01-02-A", plan.FromBin)
	}

	// The derived bin is persisted, not just returned: the publisher
	// reads the unit back from the repo at encode time.
	stored, err := f.workUnits.FindById(context.Background(), "wu-derive-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	storedPlan, _ := stored.Pick()
	if storedPlan.FromBin != "WH1-FWD-A01-01-02-A" {
		t.Fatalf("stored FromBin = %q, want WH1-FWD-A01-01-02-A", storedPlan.FromBin)
	}
}

func TestReleaseNextWork_KeepsCallerStatedFromBin(t *testing.T) {
	f := newFixture()
	relocateUnit(t, f.workUnits, "plan-2026-11-05", "WH1", "SKU-FACE", "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-02-A")

	plan, err := workunit.NewPickPlan("SKU-FACE", 6, "EACH", "WH1-FWD-B02-01-01-A")
	if err != nil {
		t.Fatalf("NewPickPlan: %v", err)
	}
	pathId, _ := shared.NewPathId("pick-a")
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	if _, err := enqueue.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-caller-bin", PathId: pathId,
		CPT: shared.NewCPT(f.clock.Now().Add(time.Hour)), Reference: "order-wu-caller-bin",
		SKU: "SKU-FACE", Pick: &plan,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	release := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock).WithPickFaces(f.workUnits)
	released, err := release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got, _ := released.Pick()
	if got.FromBin != "WH1-FWD-B02-01-01-A" {
		t.Fatalf("FromBin = %q, want the caller-stated WH1-FWD-B02-01-01-A", got.FromBin)
	}
}

func TestReleaseNextWork_NoForwardPickFaceOmitsFromBin(t *testing.T) {
	f := newFixture()
	pathId := enqueuePlanlessPick(t, f, "wu-noface-1")
	release := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock).WithPickFaces(f.workUnits)

	released, err := release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	plan, ok := released.Pick()
	if !ok || plan.FromBin != "" {
		t.Fatalf("plan = %+v, ok = %v: a SKU with no forward-pick face releases with no FromBin (task completes as today)", plan, ok)
	}
}

func TestReleaseNextWork_UnwiredPickFacesDerivesNothing(t *testing.T) {
	f := newFixture()
	relocateUnit(t, f.workUnits, "plan-2026-11-05", "WH1", "SKU-FACE", "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-02-A")
	pathId := enqueuePlanlessPick(t, f, "wu-unwired-1")
	release := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)

	released, err := release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	plan, _ := released.Pick()
	if plan.FromBin != "" {
		t.Fatalf("FromBin = %q, want empty with no pick-face port wired", plan.FromBin)
	}
}

// brokenPickFaces proves the fail-open contract: a lookup error omits the
// enrichment and still releases the unit.
type brokenPickFaces struct{}

func (brokenPickFaces) ForwardPickFaceBySKU(ctx context.Context, siteId, sku string) (string, error) {
	return "", errors.New("boom")
}

func TestReleaseNextWork_PickFaceLookupErrorFailsOpen(t *testing.T) {
	f := newFixture()
	pathId := enqueuePlanlessPick(t, f, "wu-broken-1")
	release := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock).WithPickFaces(brokenPickFaces{})

	released, err := release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("a pick-face lookup error must never block the release: %v", err)
	}
	plan, _ := released.Pick()
	if plan.FromBin != "" {
		t.Fatalf("FromBin = %q, want empty on a failed lookup", plan.FromBin)
	}
}

// The site constrains the face: a plan at another site never answers for
// a site-scoped pick.
func TestReleaseNextWork_FaceIsScopedToTheUnitsSite(t *testing.T) {
	f := newFixture()
	relocateUnit(t, f.workUnits, "plan-2026-11-05", "WH2", "SKU-FACE", "WH2-RSV-A01-01-01-A", "WH2-FWD-A01-01-02-A")

	plan, err := workunit.NewPickPlan("SKU-FACE", 6, "EACH", "")
	if err != nil {
		t.Fatalf("NewPickPlan: %v", err)
	}
	pathId, _ := shared.NewPathId("pick-a")
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	if _, err := enqueue.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-other-site", PathId: pathId,
		CPT: shared.NewCPT(f.clock.Now().Add(time.Hour)), Reference: "order-wu-other-site",
		SKU: "SKU-FACE", SiteId: "WH1", Pick: &plan,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	release := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock).WithPickFaces(f.workUnits)
	released, err := release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got, _ := released.Pick()
	if got.FromBin != "" {
		t.Fatalf("FromBin = %q, want empty: the only face is at WH2, the pick is at WH1", got.FromBin)
	}
}

// ports compile guard: the memory double and the broken stub both satisfy
// the optional port.
var _ ports.ForwardPickFaceLookup = (*memory.WorkUnitRepo)(nil)
var _ ports.ForwardPickFaceLookup = brokenPickFaces{}
