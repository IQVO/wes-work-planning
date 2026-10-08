package workunit_test

import (
	"errors"
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

// A line number is valid iff it is unknown (0) or 1 <= n <= MaxLineNo
// (math.MaxInt32): every line_no column is a 32-bit INTEGER.
func TestValidateLineNo_Boundaries(t *testing.T) {
	cases := []struct {
		name    string
		lineNo  int
		wantErr bool
	}{
		{"unknown (0)", 0, false},
		{"one", 1, false},
		{"max int32", 2147483647, false},
		{"negative", -1, true},
		{"max int32 + 1", 2147483648, true},
		{"max int64", 9223372036854775807, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := workunit.ValidateLineNo(tc.lineNo)
			if tc.wantErr && !errors.Is(err, workunit.ErrInvalidLineNo) {
				t.Fatalf("ValidateLineNo(%d) = %v, want ErrInvalidLineNo", tc.lineNo, err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateLineNo(%d) = %v, want nil", tc.lineNo, err)
			}
		})
	}
}

func TestMaxLineNo_IsMaxInt32(t *testing.T) {
	if workunit.MaxLineNo != 2147483647 {
		t.Fatalf("MaxLineNo = %d, want 2147483647", workunit.MaxLineNo)
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
