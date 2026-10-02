package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// Regression for lost WorkPool updates. WorkPoolRepo.Save rewrites the
// whole pool, so before pools were versioned two concurrent writers each
// saved the pool they had loaded and silently reverted the other's change.
// Observed live (warehouse-day simulation): work units Completed in
// work_units but still 'pending' in work_pool_entries, after which every
// release on the path failed. Concurrent enqueues and releases on ONE
// path must now account for every unit exactly once.
func TestWorkPool_ConcurrentEnqueueAndRelease_NoLostUpdates(t *testing.T) {
	const units, releasers = 40, 6
	f := newFixture()
	ctx := context.Background()
	pathId, _ := shared.NewPathId("pick")
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	rel := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < units; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
				WorkUnitId: fmt.Sprintf("wu-%02d", i), PathId: pathId,
				CPT: shared.NewCPT(f.clock.Now().Add(time.Duration(i) * time.Minute)), Reference: fmt.Sprintf("ord-%02d", i),
			}); err != nil {
				t.Errorf("enqueue %d: %v", i, err)
			}
		}(i)
	}
	var mu sync.Mutex
	released := map[string]int{}
	for r := 0; r < releasers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < units; j++ {
				u, err := rel.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId})
				if err != nil {
					continue // empty pool / not created yet / lost race budget
				}
				mu.Lock()
				released[u.Id()]++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	drainReleases(t, rel, pathId, released)
	assertEachReleasedOnce(t, released, units)
	pool, err := f.pools.FindByPathId(ctx, pathId)
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	if len(pool.Entries()) != units {
		t.Fatalf("pool holds %d entries, want %d (an enqueue was lost)", len(pool.Entries()), units)
	}
	if pool.WIP() != units {
		t.Fatalf("pool WIP = %d, want %d (a release was reverted)", pool.WIP(), units)
	}
}

// Concurrent completions racing a release must all free their WIP slot.
func TestWorkPool_ConcurrentCompletions_AllFreeTheirSlot(t *testing.T) {
	const units = 30
	f := newFixture()
	ctx := context.Background()
	pathId, _ := shared.NewPathId("pick")
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	rel := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	complete := usecases.NewRecordCompletion(f.workUnits, f.pools, f.publisher, f.clock)
	for i := 0; i < units; i++ {
		if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{WorkUnitId: fmt.Sprintf("wu-%02d", i), PathId: pathId, CPT: shared.NewCPT(f.clock.Now().Add(time.Hour)), Reference: "r"}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, err := rel.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
			t.Fatalf("release: %v", err)
		}
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < units; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if _, err := complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: fmt.Sprintf("wu-%02d", i)}); err != nil {
				t.Errorf("complete %d: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	pool, _ := f.pools.FindByPathId(ctx, pathId)
	if pool.WIP() != 0 {
		t.Fatalf("WIP = %d after completing every unit, want 0 (a completion was lost)", pool.WIP())
	}
}

// The repository itself must refuse a stale save.
func TestWorkPoolRepo_StaleSaveIsRejected(t *testing.T) {
	f := newFixture()
	ctx := context.Background()
	pathId, _ := shared.NewPathId("pick")
	_ = f.pools.Save(ctx, release.NewWorkPool(pathId, release.ReleaseFed, 10, 0))
	a, _ := f.pools.FindByPathId(ctx, pathId)
	b, _ := f.pools.FindByPathId(ctx, pathId)
	_ = a.Enqueue("wu-a", shared.NewCPT(f.clock.Now()))
	_ = b.Enqueue("wu-b", shared.NewCPT(f.clock.Now()))
	if err := a.Enqueue("wu-a2", shared.NewCPT(f.clock.Now())); err != nil {
		t.Fatal(err)
	}
	if err := f.pools.Save(ctx, a); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if err := f.pools.Save(ctx, b); !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("stale save err = %v, want ErrConcurrentModification", err)
	}
}

// drainReleases releases whatever is still pending, single-threaded.
func drainReleases(t *testing.T, rel *usecases.ReleaseNextWork, pathId shared.PathId, released map[string]int) {
	t.Helper()
	for {
		u, err := rel.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
		if errors.Is(err, release.ErrEmptyPool) {
			return
		}
		if err != nil {
			t.Fatalf("drain release: %v", err)
		}
		released[u.Id()]++
	}
}

func assertEachReleasedOnce(t *testing.T, released map[string]int, units int) {
	t.Helper()
	for id, n := range released {
		if n != 1 {
			t.Fatalf("work unit %s released %d times, want exactly once", id, n)
		}
	}
	if len(released) != units {
		t.Fatalf("released %d distinct units, want all %d", len(released), units)
	}
}
