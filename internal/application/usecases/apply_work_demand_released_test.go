package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// These tests pin ADR-0033's ApplyWorkDemandReleased: network-inventory-
// planning's WorkDemandReleased maps to one WorkUnit with the deterministic
// id demand_id, the processed-event mark + enqueue commit atomically
// (onceAtomically, ADR-0028), and a duplicate CE id / different-CE-same-
// demand is a benign no-op exactly like ApplyOrderAllocated's line
// deduplication.

type demandFixture struct {
	workUnits *memory.WorkUnitRepo
	pools     *memory.WorkPoolRepo
	uc        *usecases.ApplyWorkDemandReleased
}

func demandCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}}})
}

func newDemandFixture() demandFixture {
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	clock := memory.FixedClock{At: time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)}
	processed := memory.NewProcessedEventRepo()
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, events.NewLogPublisher(nil), clock)
	return demandFixture{
		workUnits: workUnits,
		pools:     pools,
		uc:        usecases.NewApplyWorkDemandReleased(enqueue, processed, demandCatalogue()),
	}
}

func demandRequest(eventId, demandId string) usecases.ApplyWorkDemandReleasedRequest {
	return usecases.ApplyWorkDemandReleasedRequest{
		EventId:     eventId,
		OccurredAt:  time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC),
		DemandId:    demandId,
		WorkKind:    workunit.WorkKindTransferPick,
		TransferRef: "TRF-2026-042",
		PathId:      "pick-transfer-a",
		SiteId:      "site-north-1",
		SKU:         "SKU-T1",
		Quantity:    17,
		CPT:         time.Date(2026, 10, 6, 23, 0, 0, 0, time.UTC),
	}
}

func TestApplyWorkDemandReleased_MapsDemandToWorkUnit(t *testing.T) {
	f := newDemandFixture()
	ctx := context.Background()

	already, err := f.uc.Execute(ctx, demandRequest("evt-demand-1", "demand-1"))
	if err != nil || already {
		t.Fatalf("Execute = (already=%v, %v), want (false, nil)", already, err)
	}

	unit, err := f.workUnits.FindById(ctx, "demand-1")
	if err != nil {
		t.Fatalf("work unit demand-1 was never enqueued: %v", err)
	}
	// reference = demand_id as the generic operator ref.
	if unit.Reference() != "demand-1" {
		t.Errorf("Reference = %q, want demand-1", unit.Reference())
	}
	if unit.SKU() != "SKU-T1" {
		t.Errorf("SKU = %q, want SKU-T1", unit.SKU())
	}
	if unit.TransferRef() != "TRF-2026-042" {
		t.Errorf("TransferRef = %q, want TRF-2026-042", unit.TransferRef())
	}
	if unit.WorkKind() != workunit.WorkKindTransferPick {
		t.Errorf("WorkKind = %q, want TRANSFER_PICK", unit.WorkKind())
	}
	if unit.SiteId() != "site-north-1" {
		t.Errorf("SiteId = %q, want site-north-1", unit.SiteId())
	}
	if unit.Quantity() != 17 {
		t.Errorf("Quantity = %d, want 17", unit.Quantity())
	}
	if unit.PathId().String() != "pick-transfer-a" {
		t.Errorf("PathId = %q, want pick-transfer-a", unit.PathId().String())
	}
	if !unit.CPT().Time().Equal(time.Date(2026, 10, 6, 23, 0, 0, 0, time.UTC)) {
		t.Errorf("CPT = %v, want the event's cpt", unit.CPT().Time())
	}

	// The unit is in the path's pool, ready to release.
	pool, err := f.pools.FindByPathId(ctx, unit.PathId())
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	if len(pool.Entries()) == 0 {
		t.Fatalf("work unit demand-1 never entered the pool: %+v", pool)
	}
}

func TestApplyWorkDemandReleased_RedeliveryIsBenignNoOp(t *testing.T) {
	f := newDemandFixture()
	ctx := context.Background()
	req := demandRequest("evt-demand-dup", "demand-2")

	if already, err := f.uc.Execute(ctx, req); err != nil || already {
		t.Fatalf("first Execute = (already=%v, %v)", already, err)
	}
	// Same CloudEvents id redelivered: processed-events guard reports
	// alreadyProcessed, no second enqueue.
	if already, err := f.uc.Execute(ctx, req); err != nil || !already {
		t.Fatalf("redelivery = (already=%v, %v), want (true, nil)", already, err)
	}
}

func TestApplyWorkDemandReleased_DifferentCEIdSameDemandIsBenignNoOp(t *testing.T) {
	f := newDemandFixture()
	ctx := context.Background()

	if _, err := f.uc.Execute(ctx, demandRequest("evt-demand-a", "demand-3")); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	// A DIFFERENT CloudEvents id carrying the SAME demand_id: the
	// deterministic WorkUnitId (demand_id) is already in the pool, so
	// ErrDuplicateEntry is a benign no-op — never a stall.
	if _, err := f.uc.Execute(ctx, demandRequest("evt-demand-b", "demand-3")); err != nil {
		t.Fatalf("same-demand different-CE Execute: %v", err)
	}
	if _, err := f.uc.Execute(ctx, demandRequest("evt-demand-c", "demand-3")); err != nil {
		t.Fatalf("third same-demand Execute: %v", err)
	}
}

func TestApplyWorkDemandReleased_UnknownPathFailsLoud(t *testing.T) {
	f := newDemandFixture()
	ctx := context.Background()
	req := demandRequest("evt-demand-bad", "demand-4")
	req.PathId = "not-a-real-path"

	if _, err := f.uc.Execute(ctx, req); !errors.Is(err, pathcatalog.ErrUnknownPath) {
		t.Fatalf("got %v, want ErrUnknownPath", err)
	}
	if _, err := f.workUnits.FindById(ctx, "demand-4"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("no work unit may exist for an unknown path, got %v", err)
	}
}

func TestApplyWorkDemandReleased_UnknownWorkKindFailsLoud(t *testing.T) {
	f := newDemandFixture()
	ctx := context.Background()
	req := demandRequest("evt-demand-badkind", "demand-5")
	req.WorkKind = "NOT_A_KIND"

	if _, err := f.uc.Execute(ctx, req); !errors.Is(err, workunit.ErrUnknownWorkKind) {
		t.Fatalf("got %v, want ErrUnknownWorkKind", err)
	}
	if _, err := f.workUnits.FindById(ctx, "demand-5"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("no work unit may exist for an unknown work kind, got %v", err)
	}
}

func TestApplyWorkDemandReleased_EmptyDemandIdFails(t *testing.T) {
	f := newDemandFixture()
	ctx := context.Background()
	req := demandRequest("evt-demand-empty", "")

	if _, err := f.uc.Execute(ctx, req); !errors.Is(err, workunit.ErrEmptyId) {
		t.Fatalf("got %v, want ErrEmptyId", err)
	}
}
