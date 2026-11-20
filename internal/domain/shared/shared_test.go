package shared

import (
	"errors"
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

// The per-event-type tests below were split out of one TestEvents function:
// each event's constructor/field assertions stand on their own now, but
// every case and assertion is unchanged.

func TestWorkUnitCreatedEvent(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	at := time.Now()

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
}

func TestWorkUnitCreatedEvent_WithOrigin(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	base := NewWorkUnitCreated("order-1-line-2", pathId, time.Now())
	if base.Reference != "" || base.LineNo != 0 {
		t.Fatalf("a bare event carries no origin, got reference %q line %d", base.Reference, base.LineNo)
	}
	ev := base.WithOrigin("order-1", 2)
	if ev.Reference != "order-1" || ev.LineNo != 2 {
		t.Fatalf("got reference %q line %d, want order-1 / 2", ev.Reference, ev.LineNo)
	}
	if base.Reference != "" {
		t.Fatal("WithOrigin must not mutate the receiver")
	}
	if ev.WorkUnitId != "order-1-line-2" || ev.EventName() != "WorkUnitCreated" {
		t.Fatalf("WithOrigin changed the identity: %+v", ev)
	}
}

func TestWorkReleasedEvent(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	at := time.Now()

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
}

func TestBacklogThresholdBreachedEvent(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	at := time.Now()

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
}

func TestRateDeviationDetectedEvent(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	at := time.Now()

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
}

func TestPathThrottledEvent(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	at := time.Now()

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
}

func TestLaborReassignmentFlaggedEvent(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	at := time.Now()

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
}

func TestWorkUnitCompletedEvent(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	at := time.Now()

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
}

func TestPathCapacityChangedEvent(t *testing.T) {
	pathId, err := NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	at := time.Now()

	t.Run("known", func(t *testing.T) {
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

	t.Run("unknown", func(t *testing.T) {
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
