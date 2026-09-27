package shared

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestCPT(t *testing.T) {
	now := time.Now()
	later := now.Add(time.Hour)

	cptNow := NewCPT(now)
	cptLater := NewCPT(later)

	t.Run("Time", func(t *testing.T) {
		if !cptNow.Time().Equal(now) {
			t.Fatalf("got %v, want %v", cptNow.Time(), now)
		}
	})

	t.Run("Before true", func(t *testing.T) {
		if !cptNow.Before(cptLater) {
			t.Fatalf("expected %v to be before %v", cptNow, cptLater)
		}
	})

	t.Run("Before false", func(t *testing.T) {
		if cptLater.Before(cptNow) {
			t.Fatalf("did not expect %v to be before %v", cptLater, cptNow)
		}
	})

	t.Run("Equals true", func(t *testing.T) {
		other := NewCPT(now)
		if !cptNow.Equals(other) {
			t.Fatalf("expected %v to equal %v", cptNow, other)
		}
	})

	t.Run("Equals false", func(t *testing.T) {
		if cptNow.Equals(cptLater) {
			t.Fatalf("did not expect %v to equal %v", cptNow, cptLater)
		}
	})
}

func TestEvents(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	at := time.Now()

	t.Run("ChargeForecastReceived", func(t *testing.T) {
		ev := NewChargeForecastReceived(pathId, at)
		if ev.EventName() != "ChargeForecastReceived" {
			t.Fatalf("got %s, want ChargeForecastReceived", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
	})

	t.Run("ShiftPlanCommitted", func(t *testing.T) {
		ev := NewShiftPlanCommitted(pathId, at)
		if ev.EventName() != "ShiftPlanCommitted" {
			t.Fatalf("got %s, want ShiftPlanCommitted", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
	})

	t.Run("WorkUnitCreated", func(t *testing.T) {
		ev := NewWorkUnitCreated("wu-1", pathId, at)
		if ev.EventName() != "WorkUnitCreated" {
			t.Fatalf("got %s, want WorkUnitCreated", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if ev.WorkUnitId != "wu-1" {
			t.Fatalf("got %s, want wu-1", ev.WorkUnitId)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
	})

	t.Run("WorkReleased", func(t *testing.T) {
		ev := NewWorkReleased("wu-2", pathId, at)
		if ev.EventName() != "WorkReleased" {
			t.Fatalf("got %s, want WorkReleased", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if ev.WorkUnitId != "wu-2" {
			t.Fatalf("got %s, want wu-2", ev.WorkUnitId)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
	})

	t.Run("BacklogThresholdBreached", func(t *testing.T) {
		ev := NewBacklogThresholdBreached(pathId, at)
		if ev.EventName() != "BacklogThresholdBreached" {
			t.Fatalf("got %s, want BacklogThresholdBreached", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
	})

	t.Run("RateDeviationDetected", func(t *testing.T) {
		ev := NewRateDeviationDetected(pathId, at)
		if ev.EventName() != "RateDeviationDetected" {
			t.Fatalf("got %s, want RateDeviationDetected", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
	})

	t.Run("PathThrottled", func(t *testing.T) {
		ev := NewPathThrottled(pathId, at)
		if ev.EventName() != "PathThrottled" {
			t.Fatalf("got %s, want PathThrottled", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
	})

	t.Run("LaborReassignmentFlagged", func(t *testing.T) {
		ev := NewLaborReassignmentFlagged(pathId, at)
		if ev.EventName() != "LaborReassignmentFlagged" {
			t.Fatalf("got %s, want LaborReassignmentFlagged", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
	})

	t.Run("WorkUnitCompleted", func(t *testing.T) {
		ev := NewWorkUnitCompleted("wu-3", pathId, at)
		if ev.EventName() != "WorkUnitCompleted" {
			t.Fatalf("got %s, want WorkUnitCompleted", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if ev.WorkUnitId != "wu-3" {
			t.Fatalf("got %s, want wu-3", ev.WorkUnitId)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
	})

	t.Run("PathCapacityChanged known", func(t *testing.T) {
		cutoff := at.Add(2 * time.Hour)
		ev := NewPathCapacityChanged(pathId, cutoff, 42, true, at)
		if ev.EventName() != "PathCapacityChanged" {
			t.Fatalf("got %s, want PathCapacityChanged", ev.EventName())
		}
		if !ev.OccurredAt().Equal(at) {
			t.Fatalf("got %v, want %v", ev.OccurredAt(), at)
		}
		if !ev.PathId.Equals(pathId) {
			t.Fatalf("got %v, want %v", ev.PathId, pathId)
		}
		if !ev.CutoffAt.Equal(cutoff) {
			t.Fatalf("got CutoffAt %v, want %v", ev.CutoffAt, cutoff)
		}
		if ev.RemainingUnits != 42 {
			t.Fatalf("got RemainingUnits %d, want 42", ev.RemainingUnits)
		}
		if !ev.Known {
			t.Fatalf("got Known false, want true")
		}
	})

	t.Run("PathCapacityChanged unknown", func(t *testing.T) {
		cutoff := at.Add(time.Hour)
		ev := NewPathCapacityChanged(pathId, cutoff, 0, false, at)
		if ev.Known {
			t.Fatalf("got Known true, want false")
		}
		if ev.RemainingUnits != 0 {
			t.Fatalf("got RemainingUnits %d, want 0", ev.RemainingUnits)
		}
	})
}

func TestPathId(t *testing.T) {
	t.Run("NewPathId valid", func(t *testing.T) {
		p, err := NewPathId("pick-zone-a")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.String() != "pick-zone-a" {
			t.Fatalf("got %s, want pick-zone-a", p.String())
		}
	})

	t.Run("NewPathId empty", func(t *testing.T) {
		_, err := NewPathId("")
		if !errors.Is(err, ErrInvalidPathId) {
			t.Fatalf("got err %v, want %v", err, ErrInvalidPathId)
		}
	})

	t.Run("Equals true", func(t *testing.T) {
		p1, _ := NewPathId("pack-station-3")
		p2, _ := NewPathId("pack-station-3")
		if !p1.Equals(p2) {
			t.Fatalf("expected %v to equal %v", p1, p2)
		}
	})

	t.Run("Equals false", func(t *testing.T) {
		p1, _ := NewPathId("pack-station-3")
		p2, _ := NewPathId("pick-zone-a")
		if p1.Equals(p2) {
			t.Fatalf("did not expect %v to equal %v", p1, p2)
		}
	})
}

func TestQuantity(t *testing.T) {
	t.Run("NewQuantity valid", func(t *testing.T) {
		q, err := NewQuantity(10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if q.Value() != 10 {
			t.Fatalf("got %d, want 10", q.Value())
		}
	})

	t.Run("NewQuantity negative", func(t *testing.T) {
		_, err := NewQuantity(-1)
		if !errors.Is(err, ErrInvalidQuantity) {
			t.Fatalf("got err %v, want %v", err, ErrInvalidQuantity)
		}
	})

	t.Run("NewQuantity zero", func(t *testing.T) {
		q, err := NewQuantity(0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if q.Value() != 0 {
			t.Fatalf("got %d, want 0", q.Value())
		}
	})

	t.Run("Add", func(t *testing.T) {
		q1, _ := NewQuantity(10)
		q2, _ := NewQuantity(5)
		sum := q1.Add(q2)
		if sum.Value() != 15 {
			t.Fatalf("got %d, want 15", sum.Value())
		}
	})

	t.Run("Subtract valid", func(t *testing.T) {
		q1, _ := NewQuantity(10)
		q2, _ := NewQuantity(4)
		diff, err := q1.Subtract(q2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if diff.Value() != 6 {
			t.Fatalf("got %d, want 6", diff.Value())
		}
	})

	t.Run("Subtract underflow", func(t *testing.T) {
		q1, _ := NewQuantity(4)
		q2, _ := NewQuantity(10)
		_, err := q1.Subtract(q2)
		if !errors.Is(err, ErrInvalidQuantity) {
			t.Fatalf("got err %v, want %v", err, ErrInvalidQuantity)
		}
	})
}

func TestRate(t *testing.T) {
	t.Run("NewRate valid", func(t *testing.T) {
		r, err := NewRate(120.5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if r.UnitsPerHour() != 120.5 {
			t.Fatalf("got %v, want 120.5", r.UnitsPerHour())
		}
	})

	t.Run("NewRate zero", func(t *testing.T) {
		_, err := NewRate(0)
		if !errors.Is(err, ErrInvalidRate) {
			t.Fatalf("got err %v, want %v", err, ErrInvalidRate)
		}
	})

	t.Run("NewRate negative", func(t *testing.T) {
		_, err := NewRate(-5)
		if !errors.Is(err, ErrInvalidRate) {
			t.Fatalf("got err %v, want %v", err, ErrInvalidRate)
		}
	})
}

func TestStationCount(t *testing.T) {
	t.Run("NewStationCount valid", func(t *testing.T) {
		s, err := NewStationCount(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.Value() != 3 {
			t.Fatalf("got %d, want 3", s.Value())
		}
	})

	t.Run("NewStationCount negative", func(t *testing.T) {
		_, err := NewStationCount(-1)
		if !errors.Is(err, ErrInvalidStationCount) {
			t.Fatalf("got err %v, want %v", err, ErrInvalidStationCount)
		}
	})

	t.Run("NewStationCount above int32 range", func(t *testing.T) {
		_, err := NewStationCount(math.MaxInt32 + 1)
		if !errors.Is(err, ErrInvalidStationCount) {
			t.Fatalf("got err %v, want %v", err, ErrInvalidStationCount)
		}
	})

	t.Run("NewStationCount zero", func(t *testing.T) {
		s, err := NewStationCount(0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.Value() != 0 {
			t.Fatalf("got %d, want 0", s.Value())
		}
	})

	t.Run("LessThan true", func(t *testing.T) {
		s1, _ := NewStationCount(2)
		s2, _ := NewStationCount(5)
		if !s1.LessThan(s2) {
			t.Fatalf("expected %v to be less than %v", s1, s2)
		}
	})

	t.Run("LessThan false", func(t *testing.T) {
		s1, _ := NewStationCount(5)
		s2, _ := NewStationCount(2)
		if s1.LessThan(s2) {
			t.Fatalf("did not expect %v to be less than %v", s1, s2)
		}
	})

	t.Run("GreaterThan true", func(t *testing.T) {
		s1, _ := NewStationCount(5)
		s2, _ := NewStationCount(2)
		if !s1.GreaterThan(s2) {
			t.Fatalf("expected %v to be greater than %v", s1, s2)
		}
	})

	t.Run("GreaterThan false", func(t *testing.T) {
		s1, _ := NewStationCount(2)
		s2, _ := NewStationCount(5)
		if s1.GreaterThan(s2) {
			t.Fatalf("did not expect %v to be greater than %v", s1, s2)
		}
	})

	t.Run("GreaterThan equal", func(t *testing.T) {
		s1, _ := NewStationCount(3)
		s2, _ := NewStationCount(3)
		if s1.GreaterThan(s2) {
			t.Fatalf("did not expect %v to be greater than %v", s1, s2)
		}
		if s1.LessThan(s2) {
			t.Fatalf("did not expect %v to be less than %v", s1, s2)
		}
	})
}
