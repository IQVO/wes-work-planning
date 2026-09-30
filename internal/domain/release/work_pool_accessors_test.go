package release

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func TestWorkPool_Accessors(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 5, 3)

	if !pool.PathId().Equals(pathId) {
		t.Fatalf("got PathId %v, want %v", pool.PathId(), pathId)
	}
	if pool.Mode() != ReleaseFed {
		t.Fatalf("got Mode %v, want %v", pool.Mode(), ReleaseFed)
	}
	if pool.WIPLimit() != 5 {
		t.Fatalf("got WIPLimit %d, want 5", pool.WIPLimit())
	}
	if pool.AlarmThreshold() != 3 {
		t.Fatalf("got AlarmThreshold %d, want 3", pool.AlarmThreshold())
	}
}

func TestWorkPool_Accessors_FlowFed(t *testing.T) {
	pathId, _ := shared.NewPathId("pack-b")
	pool := NewWorkPool(pathId, FlowFed, 0, 7)

	if pool.Mode() != FlowFed {
		t.Fatalf("got Mode %v, want %v", pool.Mode(), FlowFed)
	}
	if pool.AlarmThreshold() != 7 {
		t.Fatalf("got AlarmThreshold %d, want 7", pool.AlarmThreshold())
	}
}

func TestWorkPool_Entries(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 10, 0)
	cpt1 := shared.NewCPT(time.Now())
	cpt2 := shared.NewCPT(time.Now().Add(time.Hour))

	// The cases build on one shared pool, in order, exactly like the
	// scenario they were extracted from.
	t.Run("empty pool reports no entries", func(t *testing.T) { entriesReportEmpty(t, pool) })
	t.Run("enqueued units appear pending in CPT order", func(t *testing.T) { entriesReportPendingInCPTOrder(t, pool, cpt1, cpt2) })
	t.Run("released entry reports Released while others stay pending", func(t *testing.T) { entriesReportReleased(t, pool) })

	// A Completed entry must still report Released == true (the postgres
	// repo's FindByPathId rehydrates a "completed" DB row by calling
	// wp.Release() then wp.Complete(), keyed off exactly this flag) as
	// well as Completed == true itself. Regression coverage for the
	// entry-state snapshot's OR condition, not just the plain-released
	// case above.
	t.Run("completed entry still reports Released and Completed", func(t *testing.T) { entriesReportCompleted(t, pool) })
}

func entriesReportEmpty(t *testing.T, pool *WorkPool) {
	t.Helper()
	if entries := pool.Entries(); len(entries) != 0 {
		t.Fatalf("got %d entries, want 0", len(entries))
	}
}

func entriesReportPendingInCPTOrder(t *testing.T, pool *WorkPool, cpt1, cpt2 shared.CPT) {
	t.Helper()
	if err := pool.Enqueue("wu-1", cpt1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-2", cpt2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries := pool.Entries()
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0].WorkUnitId != "wu-1" || entries[0].Released {
		t.Fatalf("got %+v, want wu-1 pending", entries[0])
	}
	if entries[1].WorkUnitId != "wu-2" || entries[1].Released {
		t.Fatalf("got %+v, want wu-2 pending", entries[1])
	}
	if !entries[0].CPT.Equals(cpt1) {
		t.Fatalf("got CPT %v, want %v", entries[0].CPT, cpt1)
	}
}

func entriesReportReleased(t *testing.T, pool *WorkPool) {
	t.Helper()
	if err := pool.Release("wu-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries := pool.Entries()
	if !entries[0].Released {
		t.Fatalf("got %+v, want wu-1 released", entries[0])
	}
	if entries[1].Released {
		t.Fatalf("got %+v, want wu-2 still pending", entries[1])
	}
}

func entriesReportCompleted(t *testing.T, pool *WorkPool) {
	t.Helper()
	if err := pool.Complete("wu-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries := pool.Entries()
	if !entries[0].Released {
		t.Fatalf("got %+v, want wu-1 still reported Released after Complete", entries[0])
	}
	if !entries[0].Completed {
		t.Fatalf("got %+v, want wu-1 Completed", entries[0])
	}
	if entries[1].Released || entries[1].Completed {
		t.Fatalf("got %+v, want wu-2 still pending (untouched)", entries[1])
	}
}

func TestWorkPool_Release_WIPLimitInvariant_ReleaseFed(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 1, 0)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-2", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := pool.Release("wu-1"); err != nil {
		t.Fatalf("unexpected error releasing first unit: %v", err)
	}

	if err := pool.Release("wu-2"); !errors.Is(err, ErrWIPLimitReached) {
		t.Fatalf("got err %v, want %v", err, ErrWIPLimitReached)
	}
}

func TestWorkPool_Release_WIPLimitNotEnforced_FlowFed(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, FlowFed, 1, 0)
	cpt := shared.NewCPT(time.Now())

	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-2", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := pool.Release("wu-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Release("wu-2"); err != nil {
		t.Fatalf("flow-fed pool should not enforce WIP limit on Release: %v", err)
	}
}

func TestWorkPool_RemainingCapacity_ReleaseFed_SubtractsWIPFromLimit(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 5, 3)
	cpt := shared.NewCPT(time.Now())

	for _, id := range []string{"wu-1", "wu-2", "wu-3"} {
		if err := pool.Enqueue(id, cpt); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if err := pool.Release("wu-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Release("wu-2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	remaining, known := pool.RemainingCapacity()
	if !known {
		t.Fatal("expected ReleaseFed with a positive WIP limit to report known=true")
	}
	if remaining != 3 {
		t.Fatalf("got remaining %d, want 3 (limit 5 - WIP 2)", remaining)
	}
}

func TestWorkPool_RemainingCapacity_ReleaseFed_SaturatedIsZero(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 1, 0)
	cpt := shared.NewCPT(time.Now())
	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Release("wu-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	remaining, known := pool.RemainingCapacity()
	if !known {
		t.Fatal("expected known=true")
	}
	if remaining != 0 {
		t.Fatalf("got remaining %d, want 0 (WIP equals wipLimit)", remaining)
	}
}

func TestWorkPool_RemainingCapacity_FlowFed_AlwaysUnknown(t *testing.T) {
	pathId, _ := shared.NewPathId("pack-b")
	pool := NewWorkPool(pathId, FlowFed, 0, 10)

	remaining, known := pool.RemainingCapacity()
	if known {
		t.Fatal("expected FlowFed pool to report known=false — no hard admission ceiling")
	}
	if remaining != 0 {
		t.Fatalf("got remaining %d, want 0 when unknown", remaining)
	}
}

func TestWorkPool_RemainingCapacity_ReleaseFed_UnsetWIPLimitIsUnknown(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	pool := NewWorkPool(pathId, ReleaseFed, 0, 0)

	remaining, known := pool.RemainingCapacity()
	if known {
		t.Fatal("expected an unset (zero) WIP limit to report known=false")
	}
	if remaining != 0 {
		t.Fatalf("got remaining %d, want 0 when unknown", remaining)
	}
}
