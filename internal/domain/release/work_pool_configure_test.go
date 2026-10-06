package release

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func configPool(t *testing.T, mode FeedMode, wipLimit int) *WorkPool {
	t.Helper()
	pathId, _ := shared.NewPathId("pick-a")
	return NewWorkPool(pathId, mode, wipLimit, 7)
}

func TestWorkPool_Configure_SetsModeAndLimit(t *testing.T) {
	pool := configPool(t, ReleaseFed, 10)

	changed, err := pool.Configure(FlowFed, 25)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if pool.Mode() != FlowFed {
		t.Fatalf("mode = %v, want FlowFed", pool.Mode())
	}
	if pool.WIPLimit() != 25 {
		t.Fatalf("wipLimit = %d, want 25", pool.WIPLimit())
	}
	if pool.AlarmThreshold() != 7 {
		t.Fatalf("alarmThreshold = %d, want it untouched (7)", pool.AlarmThreshold())
	}
}

func TestWorkPool_Configure_IsIdempotent(t *testing.T) {
	pool := configPool(t, ReleaseFed, 10)

	changed, err := pool.Configure(ReleaseFed, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Fatal("changed = true for the same mode and limit, want false")
	}
	changed, err = pool.Configure(ReleaseFed, 11)
	if err != nil || !changed {
		t.Fatalf("limit change: changed=%v err=%v, want true,nil", changed, err)
	}
	changed, err = pool.Configure(FlowFed, 11)
	if err != nil || !changed {
		t.Fatalf("mode change: changed=%v err=%v, want true,nil", changed, err)
	}
}

func TestWorkPool_Configure_RejectsNonPositiveLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		pool := configPool(t, ReleaseFed, 10)
		changed, err := pool.Configure(FlowFed, limit)
		if !errors.Is(err, ErrInvalidWIPLimit) {
			t.Fatalf("limit %d: err = %v, want ErrInvalidWIPLimit", limit, err)
		}
		if changed {
			t.Fatalf("limit %d: changed = true on rejection", limit)
		}
		if pool.Mode() != ReleaseFed || pool.WIPLimit() != 10 {
			t.Fatalf("limit %d: pool mutated on rejection: mode=%v limit=%d", limit, pool.Mode(), pool.WIPLimit())
		}
	}
}

func TestWorkPool_Configure_AcceptsLimitOfOne(t *testing.T) {
	pool := configPool(t, ReleaseFed, 10)
	if _, err := pool.Configure(ReleaseFed, 1); err != nil {
		t.Fatalf("limit 1 must be valid: %v", err)
	}
}

func TestWorkPool_Configure_RejectsUnknownMode(t *testing.T) {
	pool := configPool(t, ReleaseFed, 10)
	changed, err := pool.Configure(FeedMode(99), 5)
	if !errors.Is(err, ErrUnknownFeedMode) {
		t.Fatalf("err = %v, want ErrUnknownFeedMode", err)
	}
	if changed || pool.WIPLimit() != 10 {
		t.Fatalf("pool mutated on rejection: changed=%v limit=%d", changed, pool.WIPLimit())
	}
}

// Lowering the limit below the current WIP never evicts or cancels work: the
// outstanding entries stay released, and releases pause until WIP < limit.
func TestWorkPool_Configure_LoweringBelowWIPNeverEvicts(t *testing.T) {
	pool := configPool(t, ReleaseFed, 10)
	cpt := shared.NewCPT(time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC))
	for _, id := range []string{"wu-1", "wu-2", "wu-3", "wu-4"} {
		if err := pool.Enqueue(id, cpt); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := pool.ReleaseNext(); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
	}
	if pool.WIP() != 3 {
		t.Fatalf("setup: WIP = %d, want 3", pool.WIP())
	}

	if _, err := pool.Configure(ReleaseFed, 1); err != nil {
		t.Fatalf("lowering below WIP must be accepted: %v", err)
	}

	if pool.WIP() != 3 {
		t.Fatalf("WIP = %d after lowering, want 3 (no eviction)", pool.WIP())
	}
	if pool.BacklogDepth() != 1 {
		t.Fatalf("backlog = %d after lowering, want 1 (nothing cancelled)", pool.BacklogDepth())
	}
	if _, err := pool.ReleaseNext(); !errors.Is(err, ErrWIPLimitReached) {
		t.Fatalf("release while WIP(3) >= limit(1): err = %v, want ErrWIPLimitReached", err)
	}
	if remaining, known := pool.RemainingCapacity(); !known || remaining != 0 {
		t.Fatalf("remaining = %d known=%v, want 0,true (clamped)", remaining, known)
	}
}

// After lowering, releases resume only once completions bring WIP strictly
// below the new limit.
func TestWorkPool_Configure_ReleasesResumeOnlyWhenWIPBelowLimit(t *testing.T) {
	pool := configPool(t, ReleaseFed, 10)
	cpt := shared.NewCPT(time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC))
	for _, id := range []string{"wu-1", "wu-2", "wu-3", "wu-4"} {
		_ = pool.Enqueue(id, cpt)
	}
	for i := 0; i < 3; i++ {
		_, _ = pool.ReleaseNext()
	}
	_, _ = pool.Configure(ReleaseFed, 1)

	// WIP 3 -> 1 is still saturated (1 >= 1).
	_ = pool.Complete("wu-1")
	_ = pool.Complete("wu-2")
	if _, err := pool.ReleaseNext(); !errors.Is(err, ErrWIPLimitReached) {
		t.Fatalf("release at WIP(1) == limit(1): err = %v, want ErrWIPLimitReached", err)
	}
	_ = pool.Complete("wu-3")
	id, err := pool.ReleaseNext()
	if err != nil {
		t.Fatalf("release at WIP(0) < limit(1): %v", err)
	}
	if id != "wu-4" {
		t.Fatalf("released %q, want wu-4", id)
	}
}

// Raising the limit takes effect immediately.
func TestWorkPool_Configure_RaisingTakesEffectImmediately(t *testing.T) {
	pool := configPool(t, ReleaseFed, 1)
	cpt := shared.NewCPT(time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC))
	_ = pool.Enqueue("wu-1", cpt)
	_ = pool.Enqueue("wu-2", cpt)
	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if _, err := pool.ReleaseNext(); !errors.Is(err, ErrWIPLimitReached) {
		t.Fatalf("second release at limit: err = %v, want ErrWIPLimitReached", err)
	}

	if _, err := pool.Configure(ReleaseFed, 2); err != nil {
		t.Fatalf("raise: %v", err)
	}
	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("release after raising: %v", err)
	}
}

// Switching a saturated release-fed pool to flow-fed stops enforcing the
// limit (flow-fed pools only alarm); nothing is evicted.
func TestWorkPool_Configure_SwitchToFlowFedKeepsEntries(t *testing.T) {
	pool := configPool(t, ReleaseFed, 1)
	cpt := shared.NewCPT(time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC))
	_ = pool.Enqueue("wu-1", cpt)
	_ = pool.Enqueue("wu-2", cpt)
	_, _ = pool.ReleaseNext()

	if _, err := pool.Configure(FlowFed, 1); err != nil {
		t.Fatalf("switch: %v", err)
	}
	if len(pool.Entries()) != 2 || pool.WIP() != 1 {
		t.Fatalf("entries=%d WIP=%d, want 2 and 1", len(pool.Entries()), pool.WIP())
	}
	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("flow-fed pool must not enforce the limit: %v", err)
	}
}
