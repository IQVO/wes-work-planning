package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0040: Cancelled is terminal, Release and Complete refuse it. RecordCompletion
// on a cancelled RELOCATE move must surface workunit.ErrCancelled (REST 409
// work-unit-cancelled), never ErrNotReleased, and must leave the unit Cancelled.
func TestRecordCompletion_CancelledMoveIsRefusedAsCancelled(t *testing.T) {
	f := newSlotFixture(t, relocateCatalogue())
	mustApply(t, f, samplePlan("evt-1", "plan-A", slotApprovedAt))
	newer := slotRequest("evt-2", "plan-B", slotApprovedAt.Add(time.Hour), slotMove("SKU-9", "Assign", "", "S9"))
	newer.SupersedesPlanId = "plan-A"
	mustApply(t, f, newer)
	if got := f.unit(t, "plan-A-move-1").State(); got != workunit.Cancelled {
		t.Fatalf("precondition: plan-A-move-1 = %v, want Cancelled", got)
	}

	_, err := f.complete.Execute(context.Background(), usecases.RecordCompletionRequest{WorkUnitId: "plan-A-move-1"})
	if !errors.Is(err, workunit.ErrCancelled) {
		t.Fatalf("RecordCompletion of a cancelled move = %v, want workunit.ErrCancelled", err)
	}
	if errors.Is(err, workunit.ErrNotReleased) {
		t.Fatalf("RecordCompletion of a cancelled move = %v, must not be ErrNotReleased", err)
	}
	if got := f.unit(t, "plan-A-move-1").State(); got != workunit.Cancelled {
		t.Fatalf("a refused completion changed the unit to %v", got)
	}
}
