package release

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ADR-0040: Withdraw takes a still-pending entry out of the pool (a
// superseded slot plan's move). Released and completed entries stay.

func withdrawPool(t *testing.T) *WorkPool {
	t.Helper()
	pathId, err := shared.NewPathId("RELOCATE")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	pool := NewWorkPool(pathId, ReleaseFed, 10, 10)
	base := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	for i, id := range []string{"wu-a", "wu-b", "wu-c"} {
		if err := pool.Enqueue(id, shared.NewCPT(base.Add(time.Duration(i)*time.Minute))); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}
	return pool
}

func TestWithdraw_RemovesAPendingEntry(t *testing.T) {
	pool := withdrawPool(t)
	if err := pool.Withdraw("wu-b"); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	entries := pool.Entries()
	if len(entries) != 2 || entries[0].WorkUnitId != "wu-a" || entries[1].WorkUnitId != "wu-c" {
		t.Fatalf("entries after withdraw = %+v, want wu-a, wu-c", entries)
	}
	if pool.BacklogDepth() != 2 {
		t.Fatalf("BacklogDepth = %d, want 2", pool.BacklogDepth())
	}
	// The withdrawn unit can be enqueued again (no tombstone is kept).
	if err := pool.Enqueue("wu-b", shared.NewCPT(time.Now())); err != nil {
		t.Fatalf("re-enqueue after withdraw: %v", err)
	}
}

func TestWithdraw_FirstAndLastEntries(t *testing.T) {
	pool := withdrawPool(t)
	if err := pool.Withdraw("wu-a"); err != nil {
		t.Fatalf("Withdraw first: %v", err)
	}
	if err := pool.Withdraw("wu-c"); err != nil {
		t.Fatalf("Withdraw last: %v", err)
	}
	entries := pool.Entries()
	if len(entries) != 1 || entries[0].WorkUnitId != "wu-b" {
		t.Fatalf("entries = %+v, want only wu-b", entries)
	}
}

func TestWithdraw_RefusesReleasedAndCompletedEntries(t *testing.T) {
	pool := withdrawPool(t)
	released, err := pool.ReleaseNext()
	if err != nil {
		t.Fatalf("ReleaseNext: %v", err)
	}
	if err := pool.Withdraw(released); !errors.Is(err, ErrAlreadyReleased) {
		t.Fatalf("Withdraw released = %v, want ErrAlreadyReleased", err)
	}
	if err := pool.Complete(released); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := pool.Withdraw(released); !errors.Is(err, ErrAlreadyReleased) {
		t.Fatalf("Withdraw completed = %v, want ErrAlreadyReleased", err)
	}
	if len(pool.Entries()) != 3 {
		t.Fatalf("a refused withdraw must keep every entry, got %d", len(pool.Entries()))
	}
}

func TestWithdraw_UnknownEntry(t *testing.T) {
	pool := withdrawPool(t)
	if err := pool.Withdraw("wu-zzz"); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("Withdraw unknown = %v, want ErrUnknownEntry", err)
	}
	if len(pool.Entries()) != 3 {
		t.Fatalf("entries = %d, want 3", len(pool.Entries()))
	}
}
