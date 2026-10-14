package workunit

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ADR-0040: a RELOCATE work unit carries one slot-plan move, may wait on
// another unit (blocked_by), and can be cancelled while still Pending.

func relocationUnit(t *testing.T) *WorkUnit {
	t.Helper()
	pathId, err := shared.NewPathId("RELOCATE")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	unit, err := NewWorkUnit("plan-1-move-0", pathId, shared.NewCPT(time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)), "plan-1")
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	return unit
}

func TestParseRelocationKind(t *testing.T) {
	cases := []struct {
		in      string
		want    RelocationKind
		wantErr bool
	}{
		{"Relocate", RelocationKindRelocate, false},
		{"Assign", RelocationKindAssign, false},
		{"Vacate", RelocationKindVacate, false},
		{"relocate", "", true},
		{"", "", true},
		{"Swap", "", true},
	}
	for _, tc := range cases {
		got, err := ParseRelocationKind(tc.in)
		if tc.wantErr {
			if !errors.Is(err, ErrUnknownRelocationKind) {
				t.Errorf("ParseRelocationKind(%q) err = %v, want ErrUnknownRelocationKind", tc.in, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ParseRelocationKind(%q) = (%q, %v), want %q", tc.in, got, err, tc.want)
		}
	}
	if RelocationKindVacate.String() != "Vacate" {
		t.Fatalf("String = %q", RelocationKindVacate.String())
	}
}

func TestNewRelocationMove_ValidMoves(t *testing.T) {
	cases := []struct {
		name, kind, from, to string
	}{
		{"relocate with both slots", "Relocate", "A", "B"},
		{"assign with only to_slot", "Assign", "", "B"},
		{"assign with a fixed source", "Assign", "R1", "B"},
		{"vacate with only from_slot", "Vacate", "A", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewRelocationMove("plan-1", 2, tc.kind, tc.from, tc.to)
			if err != nil {
				t.Fatalf("NewRelocationMove: %v", err)
			}
			if m.PlanId != "plan-1" || m.MoveSeq != 2 || string(m.Kind) != tc.kind || m.FromSlot != tc.from || m.ToSlot != tc.to {
				t.Fatalf("move = %+v", m)
			}
		})
	}
}

func TestNewRelocationMove_RejectsBrokenMoves(t *testing.T) {
	cases := []struct {
		name, plan string
		seq        int
		kind       string
		from, to   string
		want       error
	}{
		{"no plan id", "", 0, "Relocate", "A", "B", ErrInvalidRelocationMove},
		{"negative seq", "p", -1, "Relocate", "A", "B", ErrInvalidRelocationMove},
		{"unknown kind", "p", 0, "Swap", "A", "B", ErrUnknownRelocationKind},
		{"relocate without from", "p", 0, "Relocate", "", "B", ErrInvalidRelocationMove},
		{"relocate without to", "p", 0, "Relocate", "A", "", ErrInvalidRelocationMove},
		{"relocate onto itself", "p", 0, "Relocate", "A", "A", ErrInvalidRelocationMove},
		{"assign without to", "p", 0, "Assign", "R1", "", ErrInvalidRelocationMove},
		{"vacate without from", "p", 0, "Vacate", "", "B", ErrInvalidRelocationMove},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRelocationMove(tc.plan, tc.seq, tc.kind, tc.from, tc.to); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestNewRelocationMove_SeqZeroIsValid(t *testing.T) {
	if _, err := NewRelocationMove("p", 0, "Vacate", "A", ""); err != nil {
		t.Fatalf("move_seq 0 must be valid: %v", err)
	}
}

func TestWorkUnit_RelocationValue(t *testing.T) {
	unit := relocationUnit(t)
	if _, ok := unit.Relocation(); ok {
		t.Fatal("a fresh unit must carry no relocation")
	}
	m, err := NewRelocationMove("plan-1", 0, "Relocate", "A", "B")
	if err != nil {
		t.Fatalf("NewRelocationMove: %v", err)
	}
	unit.SetRelocation(m)
	got, ok := unit.Relocation()
	if !ok || got != m {
		t.Fatalf("Relocation = (%+v, %v), want %+v", got, ok, m)
	}
}

func TestWorkUnit_BlockedByAndUnblock(t *testing.T) {
	unit := relocationUnit(t)
	if unit.BlockedBy() != "" || unit.IsBlocked() {
		t.Fatal("a fresh unit must not be blocked")
	}
	unit.SetBlockedBy("plan-1-move-3")
	if unit.BlockedBy() != "plan-1-move-3" || !unit.IsBlocked() {
		t.Fatalf("BlockedBy = %q", unit.BlockedBy())
	}
	unit.Unblock()
	if unit.BlockedBy() != "" || unit.IsBlocked() {
		t.Fatalf("Unblock left BlockedBy = %q", unit.BlockedBy())
	}
}

func TestWorkUnit_CancelPending(t *testing.T) {
	unit := relocationUnit(t)
	at := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	if err := unit.Cancel(at); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if unit.State() != Cancelled || unit.State().String() != "Cancelled" {
		t.Fatalf("state = %v", unit.State())
	}
	if got := unit.CancelledAt(); got == nil || !got.Equal(at) {
		t.Fatalf("CancelledAt = %v, want %v", got, at)
	}
	if err := unit.Release(at); !errors.Is(err, ErrCancelled) {
		t.Fatalf("Release of a cancelled unit = %v, want ErrCancelled", err)
	}
	if err := unit.Complete(at); !errors.Is(err, ErrNotReleased) {
		t.Fatalf("Complete of a cancelled unit = %v, want ErrNotReleased", err)
	}
	if err := unit.Cancel(at); !errors.Is(err, ErrCancelled) {
		t.Fatalf("second Cancel = %v, want ErrCancelled", err)
	}
}

func TestWorkUnit_CancelRefusesReleasedAndCompleted(t *testing.T) {
	at := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	released := relocationUnit(t)
	if err := released.Release(at); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := released.Cancel(at); !errors.Is(err, ErrAlreadyReleased) {
		t.Fatalf("Cancel released = %v, want ErrAlreadyReleased", err)
	}
	if released.CancelledAt() != nil {
		t.Fatal("a refused Cancel must not stamp cancelledAt")
	}
	completed := relocationUnit(t)
	_ = completed.Release(at)
	_ = completed.Complete(at)
	if err := completed.Cancel(at); !errors.Is(err, ErrAlreadyReleased) {
		t.Fatalf("Cancel completed = %v, want ErrAlreadyReleased", err)
	}
	if completed.State() != Completed {
		t.Fatalf("state = %v, want Completed", completed.State())
	}
}

func TestParseState_Cancelled(t *testing.T) {
	st, err := ParseState("Cancelled")
	if err != nil || st != Cancelled {
		t.Fatalf("ParseState(Cancelled) = (%v, %v)", st, err)
	}
}

func TestWorkKindRelocate(t *testing.T) {
	k, err := ParseWorkKind("RELOCATE")
	if err != nil || k != WorkKindRelocate {
		t.Fatalf("ParseWorkKind(RELOCATE) = (%q, %v)", k, err)
	}
	if WorkKindRelocate.IsTransfer() {
		t.Fatal("RELOCATE is not a transfer leg")
	}
	for _, transfer := range []WorkKind{WorkKindTransferPick, WorkKindTransferDispatch, WorkKindTransferArrival} {
		if !transfer.IsTransfer() {
			t.Fatalf("%s must be a transfer leg", transfer)
		}
	}
	if WorkKind("").IsTransfer() {
		t.Fatal("the empty kind is not a transfer leg")
	}
}
