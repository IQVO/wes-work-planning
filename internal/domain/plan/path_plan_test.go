package plan

import (
	"errors"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func TestNewPathPlan(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	rate, _ := shared.NewRate(50)

	tests := []struct {
		name              string
		plannedHeads      int
		installedStations int
		hours             float64
		wantErr           error
	}{
		{"heads within installed stations", 3, 5, 8, nil},
		{"heads equal installed stations", 5, 5, 8, nil},
		{"heads exceed installed stations", 6, 5, 8, ErrHeadsExceedStations},
		{"zero hours invalid", 3, 5, 0, shared.ErrInvalidHours},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			heads, _ := shared.NewStationCount(tt.plannedHeads)
			installed, _ := shared.NewStationCount(tt.installedStations)

			_, err := NewPathPlan(pathId, heads, installed, rate, tt.hours)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewPathPlan_ThroughputOverflowRejected(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	// Each factor is individually positive and finite, yet the product
	// overflows float64 to +Inf — such a plan has no representable JSON
	// response body and must be rejected at construction time.
	rate, _ := shared.NewRate(5010217082820735.0)
	heads, _ := shared.NewStationCount(32)
	installed, _ := shared.NewStationCount(1800)

	if _, err := NewPathPlan(pathId, heads, installed, rate, 1.4080993563511214e+308); !errors.Is(err, ErrThroughputNotFinite) {
		t.Fatalf("got err %v, want ErrThroughputNotFinite", err)
	}
}

func TestPathPlan_PlannedThroughput(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	rate, _ := shared.NewRate(50)
	heads, _ := shared.NewStationCount(4)
	installed, _ := shared.NewStationCount(4)

	p, err := NewPathPlan(pathId, heads, installed, rate, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := 50.0 * 4 * 8
	if got := p.PlannedThroughput(); got != want {
		t.Fatalf("got throughput %v, want %v", got, want)
	}
}

func TestPathPlan_TravelDistance(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	rate, _ := shared.NewRate(50)
	heads, _ := shared.NewStationCount(4)
	installed, _ := shared.NewStationCount(4)

	t.Run("unset by default", func(t *testing.T) {
		p, err := NewPathPlan(pathId, heads, installed, rate, 8)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.TravelDistanceKnown() {
			t.Fatal("expected TravelDistanceKnown to be false by default")
		}
	})

	t.Run("SetTravelDistance records the hint", func(t *testing.T) {
		p, err := NewPathPlan(pathId, heads, installed, rate, 8)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		p.SetTravelDistance(42.5, true)
		if !p.TravelDistanceKnown() {
			t.Fatal("expected TravelDistanceKnown to be true after SetTravelDistance")
		}
		if got := p.TravelDistanceM(); got != 42.5 {
			t.Fatalf("got TravelDistanceM %v, want 42.5", got)
		}
		if !p.TravelDistanceEstimated() {
			t.Fatal("expected TravelDistanceEstimated to be true")
		}
	})

	t.Run("SetTravelDistance with a real (non-estimated) leg", func(t *testing.T) {
		p, err := NewPathPlan(pathId, heads, installed, rate, 8)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		p.SetTravelDistance(0, false)
		if !p.TravelDistanceKnown() {
			t.Fatal("expected TravelDistanceKnown to be true even for a zero distance")
		}
		if p.TravelDistanceEstimated() {
			t.Fatal("expected TravelDistanceEstimated to be false")
		}
	})
}
