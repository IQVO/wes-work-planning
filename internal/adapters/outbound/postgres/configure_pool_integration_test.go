//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ConfigurePool against real Postgres (testcontainers): the configuration
// is created when the pool is absent, survives a reload, never touches the
// entries, and is idempotent.
func TestConfigurePool_Postgres_CreateUpdateAndIdempotent(t *testing.T) {
	ctx := context.Background()
	db := outboxDB(t)
	pools := postgres.NewWorkPoolRepo(db)
	configure := usecases.NewConfigurePool(pools)
	pathId := mustPath(t, "pick-cfg")

	if _, err := pools.FindByPathId(ctx, pathId); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("precondition: pool must be absent, err=%v", err)
	}

	if _, err := configure.Execute(ctx, usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.FlowFed, WIPLimit: 40}); err != nil {
		t.Fatalf("configure (create): %v", err)
	}
	stored, err := pools.FindByPathId(ctx, pathId)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if stored.Mode() != release.FlowFed || stored.WIPLimit() != 40 || stored.Version() != 1 {
		t.Fatalf("stored mode=%v limit=%d version=%d, want FlowFed/40/1", stored.Mode(), stored.WIPLimit(), stored.Version())
	}
	if stored.AlarmThreshold() != 1000 {
		t.Fatalf("alarm threshold = %d, want the 1000 default", stored.AlarmThreshold())
	}

	// Identical repeat: same result, no write (version unchanged).
	if _, err := configure.Execute(ctx, usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.FlowFed, WIPLimit: 40}); err != nil {
		t.Fatalf("configure (repeat): %v", err)
	}
	again, _ := pools.FindByPathId(ctx, pathId)
	if again.Version() != 1 {
		t.Fatalf("version = %d after an identical repeat, want 1", again.Version())
	}

	// Change: persisted, version bumped.
	if _, err := configure.Execute(ctx, usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.ReleaseFed, WIPLimit: 9}); err != nil {
		t.Fatalf("configure (update): %v", err)
	}
	updated, _ := pools.FindByPathId(ctx, pathId)
	if updated.Mode() != release.ReleaseFed || updated.WIPLimit() != 9 || updated.Version() != 2 {
		t.Fatalf("updated mode=%v limit=%d version=%d, want ReleaseFed/9/2", updated.Mode(), updated.WIPLimit(), updated.Version())
	}
}

// Lowering the limit below the current WIP, persisted: every entry keeps its
// state, releases are refused until completions bring WIP under the limit.
func TestConfigurePool_Postgres_LoweringBelowWIPNeverEvicts(t *testing.T) {
	ctx := context.Background()
	db := outboxDB(t)
	pools := postgres.NewWorkPoolRepo(db)
	workUnits := postgres.NewWorkUnitRepo(db)
	uow := postgres.NewUnitOfWork(db)
	pub := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC)}
	pathId := mustPath(t, "pick-lower")

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, pub, clock).WithUnitOfWork(uow)
	releaseNext := usecases.NewReleaseNextWork(pools, workUnits, pub, clock).WithUnitOfWork(uow)
	complete := usecases.NewRecordCompletion(workUnits, pools, pub, clock).WithUnitOfWork(uow)
	configure := usecases.NewConfigurePool(pools)

	for i, id := range []string{"wu-1", "wu-2", "wu-3"} {
		if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
			WorkUnitId: id, PathId: pathId, CPT: shared.NewCPT(clock.Now().Add(time.Duration(i+1) * time.Hour)), Reference: "ref-" + id,
		}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
	}

	if _, err := configure.Execute(ctx, usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.ReleaseFed, WIPLimit: 1}); err != nil {
		t.Fatalf("lower below WIP: %v", err)
	}
	stored, _ := pools.FindByPathId(ctx, pathId)
	if stored.WIP() != 2 || stored.BacklogDepth() != 1 || len(stored.Entries()) != 3 {
		t.Fatalf("after lowering: WIP=%d backlog=%d entries=%d, want 2/1/3 (nothing evicted)", stored.WIP(), stored.BacklogDepth(), len(stored.Entries()))
	}
	if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); !errors.Is(err, release.ErrWIPLimitReached) {
		t.Fatalf("release over the new limit: err=%v, want ErrWIPLimitReached", err)
	}
	for _, id := range []string{"wu-1", "wu-2"} {
		if _, err := complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: id}); err != nil {
			t.Fatalf("complete %s: %v", id, err)
		}
	}
	if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("release once WIP(0) < limit(1): %v", err)
	}
}

// Configure racing enqueues on the same path: the optimistic-concurrency
// retry means neither side overwrites the other -- every enqueued entry and
// the configured mode/limit are all present at the end.
func TestConfigurePool_Postgres_ConcurrentWithEnqueueLosesNothing(t *testing.T) {
	ctx := context.Background()
	db := outboxDB(t)
	pools := postgres.NewWorkPoolRepo(db)
	workUnits := postgres.NewWorkUnitRepo(db)
	uow := postgres.NewUnitOfWork(db)
	pub := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC)}
	pathId := mustPath(t, "pick-race")

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, pub, clock).WithUnitOfWork(uow)
	configure := usecases.NewConfigurePool(pools)

	const enqueuers = 6
	errs := make(chan error, enqueuers+1)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < enqueuers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
				WorkUnitId: fmt.Sprintf("wu-%d", i), PathId: pathId, CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Reference: fmt.Sprintf("ref-%d", i),
			})
			errs <- err
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_, err := configure.Execute(ctx, usecases.ConfigurePoolRequest{PathId: pathId, Mode: release.FlowFed, WIPLimit: 77})
		errs <- err
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("a concurrent writer failed: %v", err)
		}
	}

	stored, err := pools.FindByPathId(ctx, pathId)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if stored.Mode() != release.FlowFed || stored.WIPLimit() != 77 {
		t.Fatalf("configuration lost: mode=%v limit=%d, want FlowFed/77", stored.Mode(), stored.WIPLimit())
	}
	if n := len(stored.Entries()); n != enqueuers {
		t.Fatalf("entries = %d, want %d (an enqueue was overwritten)", n, enqueuers)
	}
}
