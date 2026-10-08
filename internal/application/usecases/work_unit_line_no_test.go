package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// Decision 18 (ADR-0036): the order line number an OrderAllocated line
// carries is stored on the WorkUnit, in addition to staying in the id.

func TestApplyOrderAllocated_StampsLineNoAndKeepsTheIdUnchanged(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			f := newOrderFixture(v, 0)
			ctx := context.Background()
			req := orderRequest("evt-lines",
				usecases.OrderAllocatedLine{LineNo: 1, SKU: "SKU-1", PathId: "pick-a"},
				usecases.OrderAllocatedLine{LineNo: 7, SKU: "SKU-7", PathId: "pick-a"})

			if _, err := f.uc.Execute(ctx, req); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			for lineNo, id := range map[int]string{1: "order-1-line-1", 7: "order-1-line-7"} {
				unit, err := f.workUnits.FindById(ctx, id)
				if err != nil {
					t.Fatalf("work unit %q (id must stay <order>-line-<n>): %v", id, err)
				}
				if unit.LineNo() != lineNo {
					t.Errorf("%s: LineNo = %d, want %d", id, unit.LineNo(), lineNo)
				}
				if unit.Reference() != "order-1" {
					t.Errorf("%s: Reference = %q, want order-1", id, unit.Reference())
				}
			}
		})
	}
}

// A non-positive line number on an inbound line is treated as unknown: the
// pre-decision behaviour (the id is built, nothing is stored) is preserved
// so a malformed upstream line is never newly dead-lettered.
func TestApplyOrderAllocated_NonPositiveLineNoIsStoredAsUnknown(t *testing.T) {
	f := newOrderFixture(processedVariant{processed: memory.NewProcessedEventRepo()}, 0)
	ctx := context.Background()
	if _, err := f.uc.Execute(ctx, orderRequest("evt-zero", usecases.OrderAllocatedLine{LineNo: 0, SKU: "SKU-0", PathId: "pick-a"})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	unit, err := f.workUnits.FindById(ctx, "order-1-line-0")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.LineNo() != 0 {
		t.Fatalf("LineNo = %d, want 0 (unknown)", unit.LineNo())
	}
}

func newEnqueueForLineNo() (*usecases.EnqueueWorkUnit, *memory.WorkUnitRepo) {
	workUnits := memory.NewWorkUnitRepo()
	clock := memory.FixedClock{At: time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)}
	return usecases.NewEnqueueWorkUnit(workUnits, memory.NewWorkPoolRepo(), events.NewLogPublisher(nil), clock), workUnits
}

func enqueueRequestForLineNo(t *testing.T, id string, lineNo int) usecases.EnqueueWorkUnitRequest {
	t.Helper()
	return usecases.EnqueueWorkUnitRequest{
		WorkUnitId: id,
		PathId:     mustPath(t, "pick-a"),
		CPT:        shared.NewCPT(time.Date(2026, 10, 7, 23, 0, 0, 0, time.UTC)),
		Reference:  "order-9",
		LineNo:     lineNo,
	}
}

func TestEnqueueWorkUnit_StoresTheOptionalLineNo(t *testing.T) {
	uc, workUnits := newEnqueueForLineNo()
	ctx := context.Background()

	unit, err := uc.Execute(ctx, enqueueRequestForLineNo(t, "wu-with-line", 4))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if unit.LineNo() != 4 {
		t.Fatalf("returned unit LineNo = %d, want 4", unit.LineNo())
	}
	stored, err := workUnits.FindById(ctx, "wu-with-line")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if stored.LineNo() != 4 || stored.Id() != "wu-with-line" {
		t.Fatalf("stored LineNo = %d id = %q, want 4 / wu-with-line", stored.LineNo(), stored.Id())
	}
}

func TestEnqueueWorkUnit_LineNoIsOptional(t *testing.T) {
	uc, workUnits := newEnqueueForLineNo()
	ctx := context.Background()

	if _, err := uc.Execute(ctx, enqueueRequestForLineNo(t, "wu-no-line", 0)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stored, err := workUnits.FindById(ctx, "wu-no-line")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if stored.LineNo() != 0 {
		t.Fatalf("LineNo = %d, want 0 (unknown) when the caller gave none", stored.LineNo())
	}
}

func TestEnqueueWorkUnit_NegativeLineNoIsRejected(t *testing.T) {
	uc, workUnits := newEnqueueForLineNo()
	ctx := context.Background()

	_, err := uc.Execute(ctx, enqueueRequestForLineNo(t, "wu-neg-line", -1))
	if !errors.Is(err, workunit.ErrInvalidLineNo) {
		t.Fatalf("got %v, want workunit.ErrInvalidLineNo", err)
	}
	if _, err := workUnits.FindById(ctx, "wu-neg-line"); err == nil {
		t.Fatal("a rejected enqueue must not persist the work unit")
	}
}
