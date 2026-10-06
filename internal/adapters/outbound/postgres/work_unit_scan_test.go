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
			unit, err := r.scanWorkUnit("wu-1", "pick-a", "ref-1", "sku", tc.state, false, cpt, tc.releasedAt, tc.completedAt)
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
