package laborview_test

import (
	"testing"

	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
)

func TestCompareHeads(t *testing.T) {
	cases := []struct {
		name         string
		wes, workfrc int
		wantHeads    int
		wantDetected bool
	}{
		{"agree", 6, 6, 0, false},
		{"workforce committed more", 6, 7, 1, true},
		{"workforce committed fewer", 6, 4, -2, true},
		{"both zero", 0, 0, 0, false},
		{"we planned none", 0, 3, 3, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := laborview.CompareHeads(tc.wes, laborview.LaborPlanObserved{PlannedHeads: tc.workfrc})
			if d.Heads != tc.wantHeads || d.Detected() != tc.wantDetected {
				t.Fatalf("got %+v detected=%v, want heads=%d detected=%v", d, d.Detected(), tc.wantHeads, tc.wantDetected)
			}
			if d.WesPlannedHeads != tc.wes || d.ObservedPlannedHeads != tc.workfrc {
				t.Fatalf("operands not carried through: %+v", d)
			}
		})
	}
}
