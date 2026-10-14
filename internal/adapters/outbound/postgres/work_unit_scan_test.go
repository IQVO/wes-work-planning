package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// A stored Released/Completed row without its transition timestamp used to
// nil-dereference in scanWorkUnit. It must now come back as a wrapped
// ErrMissingTransitionTime, and well-formed rows must keep rehydrating.
func TestScanWorkUnit_TransitionTimestamps(t *testing.T) {
	cpt := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	rel := cpt.Add(-time.Hour)
	done := cpt.Add(-30 * time.Minute)

	tests := []struct {
		name        string
		state       string
		releasedAt  *time.Time
		completedAt *time.Time
		wantErr     error
		wantState   workunit.State
	}{
		{"pending without timestamps", "Pending", nil, nil, nil, workunit.Pending},
		{"released with timestamp", "Released", &rel, nil, nil, workunit.Released},
		{"completed with both", "Completed", &rel, &done, nil, workunit.Completed},
		{"released missing released_at", "Released", nil, nil, workunit.ErrMissingTransitionTime, 0},
		{"completed missing released_at", "Completed", nil, &done, workunit.ErrMissingTransitionTime, 0},
		{"completed missing completed_at", "Completed", &rel, nil, workunit.ErrMissingTransitionTime, 0},
		{"completed missing both", "Completed", nil, nil, workunit.ErrMissingTransitionTime, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &WorkUnitRepo{}
			unit, err := r.scanWorkUnit(workUnitRow{id: "wu-1", pathId: "pick-a", reference: "ref-1", sku: "sku", state: tc.state, cpt: cpt, releasedAt: tc.releasedAt, completedAt: tc.completedAt})
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				if unit != nil {
					t.Fatalf("unit = %v, want nil on error", unit)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if unit.State() != tc.wantState {
				t.Fatalf("state = %v, want %v", unit.State(), tc.wantState)
			}
		})
	}
}

// ADR-0036: line_no is a nullable column; the aggregate's 0 means unknown.
func TestScanWorkUnit_LineNoColumn(t *testing.T) {
	cpt := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	three := 3
	r := &WorkUnitRepo{}

	withLine, err := r.scanWorkUnit(workUnitRow{id: "order-1-line-3", pathId: "pick-a", reference: "order-1", state: "Pending", lineNo: &three, cpt: cpt})
	if err != nil {
		t.Fatalf("scan with line: %v", err)
	}
	if withLine.LineNo() != 3 {
		t.Fatalf("LineNo = %d, want 3", withLine.LineNo())
	}

	noLine, err := r.scanWorkUnit(workUnitRow{id: "wu-legacy", pathId: "pick-a", reference: "order-1", state: "Pending", cpt: cpt})
	if err != nil {
		t.Fatalf("scan NULL line: %v", err)
	}
	if noLine.LineNo() != 0 {
		t.Fatalf("LineNo = %d, want 0 (unknown) for a NULL column", noLine.LineNo())
	}

	if lineNoToColumn(0) != nil || lineNoToColumn(-1) != nil {
		t.Fatal("an unknown line must be written as NULL")
	}
	if got := lineNoToColumn(7); got == nil || *got != 7 {
		t.Fatalf("lineNoToColumn(7) = %v, want 7", got)
	}
}

// Defence in depth: the column is a 32-bit INTEGER, so a value above
// MaxInt32 must never be bound (it would fail with "integer out of range");
// it is written as NULL (unknown) instead.
func TestLineNoToColumn_NeverBindsAnOutOfRangeValue(t *testing.T) {
	if got := lineNoToColumn(2147483647); got == nil || *got != 2147483647 {
		t.Fatalf("lineNoToColumn(MaxInt32) = %v, want 2147483647", got)
	}
	if lineNoToColumn(2147483648) != nil {
		t.Fatal("lineNoToColumn(MaxInt32+1) must be NULL")
	}
	if lineNoToColumn(9223372036854775807) != nil {
		t.Fatal("lineNoToColumn(MaxInt64) must be NULL")
	}
}

// ADR-0040: a Cancelled row needs cancelled_at; the relocation columns
// rehydrate the move and a corrupt relocation_kind fails loud.
func TestScanWorkUnit_RelocationColumns(t *testing.T) {
	cpt := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	cancelledAt := cpt.Add(-time.Hour)
	r := &WorkUnitRepo{}
	base := workUnitRow{id: "plan-1-move-1", pathId: "RELOCATE", reference: "plan-1", sku: "SKU-2", state: "Pending",
		workKind: "RELOCATE", siteId: "SITE-1", cpt: cpt, relocationPlanId: "plan-1", relocationMoveSeq: 1,
		relocationKind: "Relocate", fromSlot: "S2", toSlot: "S3", blockedBy: "plan-1-move-2"}

	unit, err := r.scanWorkUnit(base)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	m, ok := unit.Relocation()
	if !ok || m.PlanId != "plan-1" || m.MoveSeq != 1 || m.Kind != workunit.RelocationKindRelocate || m.FromSlot != "S2" || m.ToSlot != "S3" {
		t.Fatalf("relocation = (%+v, %v)", m, ok)
	}
	if unit.BlockedBy() != "plan-1-move-2" || unit.WorkKind() != workunit.WorkKindRelocate {
		t.Fatalf("blocked_by = %q kind = %q", unit.BlockedBy(), unit.WorkKind())
	}

	cancelled := base
	cancelled.state, cancelled.cancelledAt = "Cancelled", &cancelledAt
	got, err := r.scanWorkUnit(cancelled)
	if err != nil || got.State() != workunit.Cancelled || !got.CancelledAt().Equal(cancelledAt) {
		t.Fatalf("cancelled scan = (%v, %v)", got, err)
	}

	noStamp := base
	noStamp.state = "Cancelled"
	if _, err := r.scanWorkUnit(noStamp); !errors.Is(err, workunit.ErrMissingTransitionTime) {
		t.Fatalf("cancelled without cancelled_at = %v, want ErrMissingTransitionTime", err)
	}

	corrupt := base
	corrupt.relocationKind = "Swap"
	if _, err := r.scanWorkUnit(corrupt); !errors.Is(err, workunit.ErrUnknownRelocationKind) {
		t.Fatalf("corrupt relocation_kind = %v, want ErrUnknownRelocationKind", err)
	}
}
