package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func TestConfigurePool_CreatesPoolWhenAbsent(t *testing.T) {
	f := newFixture()
	uc := usecases.NewConfigurePool(f.pools)
	pathId, _ := shared.NewPathId("pick-a")

	pool, err := uc.Execute(context.Background(), usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.FlowFed, WIPLimit: 40})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool.Mode() != release.FlowFed || pool.WIPLimit() != 40 {
		t.Fatalf("returned pool mode=%v limit=%d, want FlowFed/40", pool.Mode(), pool.WIPLimit())
	}
	stored, err := f.pools.FindByPathId(context.Background(), pathId)
	if err != nil {
		t.Fatalf("pool not persisted: %v", err)
	}
	if stored.Mode() != release.FlowFed || stored.WIPLimit() != 40 {
		t.Fatalf("stored mode=%v limit=%d, want FlowFed/40", stored.Mode(), stored.WIPLimit())
	}
}

// Configuring a path to exactly the fallback defaults must still create the
// pool (an explicit configuration is persisted even when it equals the
// fallback EnqueueWorkUnit would have used).
func TestConfigurePool_PersistsWhenAbsentEvenIfEqualToDefaults(t *testing.T) {
	f := newFixture()
	uc := usecases.NewConfigurePool(f.pools)
	pathId, _ := shared.NewPathId("pick-a")

	if _, err := uc.Execute(context.Background(), usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.ReleaseFed, WIPLimit: 1000}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := f.pools.FindByPathId(context.Background(), pathId); err != nil {
		t.Fatalf("pool not persisted: %v", err)
	}
}

func TestConfigurePool_UpdatesExistingPoolKeepingEntries(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	if _, err := enqueue.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: shared.NewCPT(f.clock.Now().Add(time.Hour)), Reference: "order-1",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	uc := usecases.NewConfigurePool(f.pools)
	if _, err := uc.Execute(context.Background(), usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.ReleaseFed, WIPLimit: 5}); err != nil {
		t.Fatalf("configure: %v", err)
	}

	stored, _ := f.pools.FindByPathId(context.Background(), pathId)
	if stored.WIPLimit() != 5 {
		t.Fatalf("limit = %d, want 5", stored.WIPLimit())
	}
	if len(stored.Entries()) != 1 {
		t.Fatalf("entries = %d, want the enqueued entry preserved", len(stored.Entries()))
	}
}

func TestConfigurePool_IsIdempotent_NoVersionBumpOnRepeat(t *testing.T) {
	f := newFixture()
	uc := usecases.NewConfigurePool(f.pools)
	pathId, _ := shared.NewPathId("pick-a")
	req := usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.ReleaseFed, WIPLimit: 7}

	first, err := uc.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := uc.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.WIPLimit() != second.WIPLimit() || first.Mode() != second.Mode() {
		t.Fatal("same request produced a different result")
	}
	stored, _ := f.pools.FindByPathId(context.Background(), pathId)
	if stored.Version() != 1 {
		t.Fatalf("version = %d, want 1: an identical repeat must not write", stored.Version())
	}
}

func TestConfigurePool_RejectsInvalidInput(t *testing.T) {
	f := newFixture()
	uc := usecases.NewConfigurePool(f.pools)
	pathId, _ := shared.NewPathId("pick-a")

	_, err := uc.Execute(context.Background(), usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.ReleaseFed, WIPLimit: 0})
	if !errors.Is(err, release.ErrInvalidWIPLimit) {
		t.Fatalf("limit 0: err = %v, want ErrInvalidWIPLimit", err)
	}
	_, err = uc.Execute(context.Background(), usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.FeedMode(9), WIPLimit: 3})
	if !errors.Is(err, release.ErrUnknownFeedMode) {
		t.Fatalf("bad mode: err = %v, want ErrUnknownFeedMode", err)
	}
	if _, err := f.pools.FindByPathId(context.Background(), pathId); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("a rejected command must not create a pool, got err=%v", err)
	}
}

// Lowering below the current WIP never evicts work; releases pause until
// completions bring WIP back under the new limit.
func TestConfigurePool_LoweringBelowWIPPausesReleaseUntilWorkCompletes(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	pathId, _ := shared.NewPathId("pick-a")
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	releaseNext := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	complete := usecases.NewRecordCompletion(f.workUnits, f.pools, f.publisher, f.clock)
	configure := usecases.NewConfigurePool(f.pools)

	for i, id := range []string{"wu-1", "wu-2", "wu-3"} {
		if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
			WorkUnitId: id, PathId: pathId, CPT: shared.NewCPT(f.clock.Now().Add(time.Duration(i+1) * time.Hour)), Reference: "order-" + id,
		}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
	}

	pool, err := configure.Execute(ctx, usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.ReleaseFed, WIPLimit: 1})
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	if pool.WIP() != 2 {
		t.Fatalf("WIP = %d after lowering, want 2 (no eviction)", pool.WIP())
	}
	if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); !errors.Is(err, release.ErrWIPLimitReached) {
		t.Fatalf("release while over the new limit: err = %v, want ErrWIPLimitReached", err)
	}

	if _, err := complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: "wu-1"}); err != nil {
		t.Fatalf("complete wu-1: %v", err)
	}
	if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); !errors.Is(err, release.ErrWIPLimitReached) {
		t.Fatalf("release at WIP == limit: err = %v, want ErrWIPLimitReached", err)
	}
	if _, err := complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: "wu-2"}); err != nil {
		t.Fatalf("complete wu-2: %v", err)
	}
	if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("release once WIP < limit: %v", err)
	}
}

// An unconfigured path keeps the ReleaseFed 1000/1000 fallback on enqueue.
func TestEnqueue_UnconfiguredPathStillFallsBackToReleaseFed1000(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	if _, err := enqueue.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: shared.NewCPT(f.clock.Now().Add(time.Hour)), Reference: "order-1",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	pool, _ := f.pools.FindByPathId(context.Background(), pathId)
	if pool.Mode() != release.ReleaseFed || pool.WIPLimit() != 1000 || pool.AlarmThreshold() != 1000 {
		t.Fatalf("fallback pool = %v/%d/%d, want ReleaseFed/1000/1000", pool.Mode(), pool.WIPLimit(), pool.AlarmThreshold())
	}
}
