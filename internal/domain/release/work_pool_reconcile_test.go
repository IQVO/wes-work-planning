package release

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func entryOf(t *testing.T, p *WorkPool, id string) PoolEntrySnapshot {
	t.Helper()
	for _, e := range p.Entries() {
		if e.WorkUnitId == id {
			return e
		}
	}
	t.Fatalf("no entry %s", id)
	return PoolEntrySnapshot{}
}

func TestWorkPool_RestoreEntry_ReproducesStoredStateIgnoringWIPLimit(t *testing.T) {
	pathId, _ := shared.NewPathId("pick")
	cpt := shared.NewCPT(time.Now())
	p := NewWorkPool(pathId, ReleaseFed, 1, 0) // limit lowered below stored WIP
	tests := []struct {
		id                  string
		released, completed bool
	}{
		{"pending", false, false},
		{"released-1", true, false},
		{"released-2", true, false},
		{"done", true, true},
		{"done-only-flag", false, true},
	}
	for _, tt := range tests {
		if err := p.RestoreEntry(tt.id, cpt, tt.released, tt.completed); err != nil {
			t.Fatalf("RestoreEntry(%s): %v", tt.id, err)
		}
	}
	for _, tt := range tests {
		e := entryOf(t, p, tt.id)
		wantReleased := tt.released || tt.completed
		if e.Released != wantReleased || e.Completed != tt.completed {
			t.Fatalf("%s: got released=%v completed=%v, want %v/%v", tt.id, e.Released, e.Completed, wantReleased, tt.completed)
		}
	}
	if p.WIP() != 2 {
		t.Fatalf("WIP = %d, want 2 (restored above the lowered limit)", p.WIP())
	}
	if err := p.RestoreEntry("pending", cpt, false, false); !errors.Is(err, ErrDuplicateEntry) {
		t.Fatalf("duplicate restore err = %v, want ErrDuplicateEntry", err)
	}
}

func TestWorkPool_Reconcile_OnlyMovesForward(t *testing.T) {
	pathId, _ := shared.NewPathId("pick")
	cpt := shared.NewCPT(time.Now())
	tests := []struct {
		name                        string
		storedReleased, storedDone  bool
		unitReleased, unitCompleted bool
		wantReleased, wantDone      bool
	}{
		{"pending unit stays pending", false, false, false, false, false, false},
		{"pending entry, released unit -> released", false, false, true, false, true, false},
		{"pending entry, completed unit -> completed", false, false, true, true, true, true},
		{"released entry, completed unit -> completed", true, false, true, true, true, true},
		{"released entry, released unit unchanged", true, false, true, false, true, false},
		{"completed entry never moves back", true, true, false, false, true, true},
		{"completed entry, released unit stays completed", true, true, true, false, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewWorkPool(pathId, ReleaseFed, 10, 0)
			_ = p.RestoreEntry("wu", cpt, tt.storedReleased, tt.storedDone)
			if err := p.Reconcile("wu", tt.unitReleased, tt.unitCompleted); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			e := entryOf(t, p, "wu")
			if e.Released != tt.wantReleased || e.Completed != tt.wantDone {
				t.Fatalf("got released=%v completed=%v, want %v/%v", e.Released, e.Completed, tt.wantReleased, tt.wantDone)
			}
		})
	}
	// The match must be on THIS entry: reconciling one unit leaves a
	// sibling entry untouched.
	p := NewWorkPool(pathId, ReleaseFed, 10, 0)
	_ = p.RestoreEntry("other", cpt, false, false)
	_ = p.RestoreEntry("target", cpt, false, false)
	if err := p.Reconcile("target", true, false); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if e := entryOf(t, p, "other"); e.Released || e.Completed {
		t.Fatalf("sibling entry changed: %+v", e)
	}
	if e := entryOf(t, p, "target"); !e.Released || e.Completed {
		t.Fatalf("target entry not released: %+v", e)
	}
	if err := p.Reconcile("missing", true, true); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("unknown entry err = %v, want ErrUnknownEntry", err)
	}
}

func TestWorkPool_Version_RoundTrips(t *testing.T) {
	pathId, _ := shared.NewPathId("pick")
	p := NewWorkPool(pathId, ReleaseFed, 10, 0)
	if p.Version() != 0 {
		t.Fatalf("new pool version = %d, want 0", p.Version())
	}
	p.SetVersion(7)
	if p.Version() != 7 {
		t.Fatalf("version = %d, want 7", p.Version())
	}
}
