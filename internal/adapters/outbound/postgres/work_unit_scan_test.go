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
			unit, err := r.scanWorkUnit("wu-1", "pick-a", "ref-1", "sku", tc.state, "", "", "", false, 0, nil, cpt, tc.releasedAt, tc.completedAt)
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

	withLine, err := r.scanWorkUnit("order-1-line-3", "pick-a", "order-1", "", "Pending", "", "", "", false, 0, &three, cpt, nil, nil)
	if err != nil {
		t.Fatalf("scan with line: %v", err)
	}
	if withLine.LineNo() != 3 {
		t.Fatalf("LineNo = %d, want 3", withLine.LineNo())
	}

	noLine, err := r.scanWorkUnit("wu-legacy", "pick-a", "order-1", "", "Pending", "", "", "", false, 0, nil, cpt, nil, nil)
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
