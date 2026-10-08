package workunit_test

import (
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// Decision 18 (ADR-0036): the order line number a work unit was made for is
// stored on the aggregate as an optional, additive hint. 0 means "unknown".

func newLineNoUnit(t *testing.T) *workunit.WorkUnit {
	t.Helper()
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	unit, err := workunit.NewWorkUnit("order-1-line-3", pathId, shared.NewCPT(time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)), "order-1")
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	return unit
}

func TestWorkUnit_LineNo_DefaultsToUnknown(t *testing.T) {
	if got := newLineNoUnit(t).LineNo(); got != 0 {
		t.Fatalf("LineNo = %d, want 0 (unknown) on a freshly built unit", got)
	}
}

func TestWorkUnit_SetLineNo_StoresTheLineAndLeavesTheIdAlone(t *testing.T) {
	unit := newLineNoUnit(t)
	unit.SetLineNo(3)

	if got := unit.LineNo(); got != 3 {
		t.Fatalf("LineNo = %d, want 3", got)
	}
	if unit.Id() != "order-1-line-3" {
		t.Fatalf("Id = %q, the work unit id must not change when a line is set", unit.Id())
	}
	if unit.Reference() != "order-1" {
		t.Fatalf("Reference = %q, must be unchanged", unit.Reference())
	}
}
