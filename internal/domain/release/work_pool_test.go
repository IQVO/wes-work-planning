package release

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func TestWorkPool_ReleasesInCPTPriorityOrder(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)

	late := shared.NewCPT(time.Now().Add(2 * time.Hour))
	early := shared.NewCPT(time.Now())
	mid := shared.NewCPT(time.Now().Add(time.Hour))

	if err := pool.Enqueue("wu-late", late); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-early", early); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-mid", mid); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	first, err := pool.ReleaseNext()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != "wu-early" {
		t.Fatalf("got %q, want wu-early", first)
	}

	second, err := pool.ReleaseNext()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if second != "wu-mid" {
		t.Fatalf("got %q, want wu-mid", second)
	}
}

func TestWorkPool_EnqueueRejectsDuplicates(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-1", cpt); !errors.Is(err, ErrDuplicateEntry) {
		t.Fatalf("got err %v, want %v", err, ErrDuplicateEntry)
	}
}

func TestWorkPool_AtMostOnceHandout(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Release("wu-1"); err != nil {
		t.Fatalf("unexpected error on first release: %v", err)
	}
	if err := pool.Release("wu-1"); !errors.Is(err, ErrAlreadyReleased) {
		t.Fatalf("got err %v, want %v", err, ErrAlreadyReleased)
	}
}

func TestWorkPool_ReleaseUnknownEntry(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)

	if err := pool.Release("does-not-exist"); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("got err %v, want %v", err, ErrUnknownEntry)
	}
}

func TestWorkPool_WIPLimitInvariant_ReleaseFed(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 1, 0)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-2", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("unexpected error releasing first unit: %v", err)
	}

	if _, err := pool.ReleaseNext(); !errors.Is(err, ErrWIPLimitReached) {
		t.Fatalf("got err %v, want %v", err, ErrWIPLimitReached)
	}
}

func TestWorkPool_WIPLimitNotEnforced_FlowFed(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, FlowFed, 1, 1)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-2", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("flow-fed pool should not enforce WIP limit as invariant: %v", err)
	}
}

func TestWorkPool_ReleaseNextOnEmptyPool(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)

	if _, err := pool.ReleaseNext(); !errors.Is(err, ErrEmptyPool) {
		t.Fatalf("got err %v, want %v", err, ErrEmptyPool)
	}
}

func TestWorkPool_AlarmThresholdOnFlowFed(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, FlowFed, 0, 1)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool.IsOverAlarmThreshold() {
		t.Fatalf("did not expect alarm threshold breach yet")
	}

	if err := pool.Enqueue("wu-2", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !pool.IsOverAlarmThreshold() {
		t.Fatalf("expected alarm threshold breach")
	}
}

// TestWorkPool_CompleteFreesWIPSlot is the regression test for the bug
// found via the e2e soak_backlog_ramp scenario: a release-fed pool whose
// released entries are never marked Complete permanently wedges shut once
// wipLimit entries have EVER been released — even though every one of them
// finished downstream long ago. Complete must free the slot so a new unit
// can be released in its place.
func TestWorkPool_CompleteFreesWIPSlot(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 1, 0)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-2", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("unexpected error releasing wu-1: %v", err)
	}
	if pool.WIP() != 1 {
		t.Fatalf("got WIP %d, want 1 after one release", pool.WIP())
	}

	// Without Complete, this next line is exactly the bug: WIP stays 1
	// forever and every future ReleaseNext on this pool 409s, regardless
	// of downstream throughput.
	if _, err := pool.ReleaseNext(); !errors.Is(err, ErrWIPLimitReached) {
		t.Fatalf("got err %v, want %v (pool should be at its WIP limit)", err, ErrWIPLimitReached)
	}

	if err := pool.Complete("wu-1"); err != nil {
		t.Fatalf("unexpected error completing wu-1: %v", err)
	}
	if pool.WIP() != 0 {
		t.Fatalf("got WIP %d, want 0 after Complete frees the slot", pool.WIP())
	}

	second, err := pool.ReleaseNext()
	if err != nil {
		t.Fatalf("expected the freed slot to admit wu-2, got error: %v", err)
	}
	if second != "wu-2" {
		t.Fatalf("got %q, want wu-2", second)
	}
}

func TestWorkPool_Complete_UnknownEntry(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)

	if err := pool.Complete("does-not-exist"); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("got err %v, want %v", err, ErrUnknownEntry)
	}
}

func TestWorkPool_Complete_RequiresReleasedFirst(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Complete("wu-1"); !errors.Is(err, ErrNotReleased) {
		t.Fatalf("got err %v, want %v", err, ErrNotReleased)
	}
}

func TestWorkPool_Complete_IdempotentOnRedelivery(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Complete("wu-1"); err != nil {
		t.Fatalf("unexpected error on first Complete: %v", err)
	}
	// A redelivered TaskCompleted event must not error the consumer.
	if err := pool.Complete("wu-1"); err != nil {
		t.Fatalf("expected idempotent no-op on redelivery, got: %v", err)
	}
}

func TestWorkPool_Complete_ThenReleaseIsRejectedAsAlreadyReleased(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Complete("wu-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// A completed entry is not re-releasable — it already had its one
	// handout, same at-most-once invariant as a merely-released entry.
	if err := pool.Release("wu-1"); !errors.Is(err, ErrAlreadyReleased) {
		t.Fatalf("got err %v, want %v", err, ErrAlreadyReleased)
	}
}

// TestWorkPool_CompleteScansPastNonMatchingEntries proves Complete walks
// the whole entry list: with several released entries queued, completing a
// later unit must scan past the earlier ones (which stay untouched in
// WIP) instead of acting on the first entry it sees or failing with
// ErrUnknownEntry.
func TestWorkPool_CompleteScansPastNonMatchingEntries(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)
	cpt := shared.NewCPT(time.Now())

	for _, id := range []string{"wu-1", "wu-2", "wu-3"} {
		if err := pool.Enqueue(id, cpt); err != nil {
			t.Fatalf("unexpected error enqueuing %s: %v", id, err)
		}
	}
	for _, id := range []string{"wu-1", "wu-2", "wu-3"} {
		if err := pool.Release(id); err != nil {
			t.Fatalf("unexpected error releasing %s: %v", id, err)
		}
	}
	if pool.WIP() != 3 {
		t.Fatalf("got WIP %d, want 3 after three releases", pool.WIP())
	}

	// Completing the LAST entry exercises the scan past wu-1 and wu-2.
	if err := pool.Complete("wu-3"); err != nil {
		t.Fatalf("unexpected error completing wu-3: %v", err)
	}
	if pool.WIP() != 2 {
		t.Fatalf("got WIP %d, want 2 after completing wu-3", pool.WIP())
	}

	// The entries scanned past keep their released (not completed) state:
	// each still holds a WIP slot until individually completed.
	entries := pool.Entries()
	byId := make(map[string]PoolEntrySnapshot, len(entries))
	for _, e := range entries {
		byId[e.WorkUnitId] = e
	}
	for _, id := range []string{"wu-1", "wu-2"} {
		if e := byId[id]; e.Completed {
			t.Fatalf("%s was scanned past, must not be completed", id)
		} else if !e.Released {
			t.Fatalf("%s must still be released", id)
		}
	}
	if e := byId["wu-3"]; !e.Completed {
		t.Fatal("wu-3 must be completed")
	}

	// And the middle entry completes just as cleanly.
	if err := pool.Complete("wu-2"); err != nil {
		t.Fatalf("unexpected error completing wu-2: %v", err)
	}
	if pool.WIP() != 1 {
		t.Fatalf("got WIP %d, want 1 after completing wu-2", pool.WIP())
	}
}
