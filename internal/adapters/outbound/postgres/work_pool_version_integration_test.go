//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// TestWorkPoolRepo_OptimisticConcurrency proves the versioned save against
// real Postgres: N writers load the same pool, each adds its own entry,
// and they save concurrently. Exactly one may win per version; the losers
// get ErrConcurrentModification and nothing they wrote survives. Before the
// version column every save "won", each rewriting the whole pool and
// silently reverting the others (the lost-update bug).
func TestWorkPoolRepo_OptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	db := outboxDB(t)
	var err error
	repo := postgres.NewWorkPoolRepo(db)
	pathId, _ := shared.NewPathId(fmt.Sprintf("integration-occ-%d", time.Now().UnixNano()))
	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))

	if err := repo.Save(ctx, release.NewWorkPool(pathId, release.ReleaseFed, 100, 0)); err != nil {
		t.Fatalf("create pool: %v", err)
	}

	const writers = 10
	loaded := make([]*release.WorkPool, writers)
	for i := range loaded {
		if loaded[i], err = repo.FindByPathId(ctx, pathId); err != nil {
			t.Fatalf("load: %v", err)
		}
		if err := loaded[i].Enqueue(fmt.Sprintf("wu-%d", i), cpt); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		wins    int
		losers  int
		unknown []error
	)
	start := make(chan struct{})
	for i := range loaded {
		wg.Add(1)
		go func(p *release.WorkPool) {
			defer wg.Done()
			<-start
			err := repo.Save(ctx, p)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ports.ErrConcurrentModification):
				losers++
			default:
				unknown = append(unknown, err)
			}
		}(loaded[i])
	}
	close(start)
	wg.Wait()

	if len(unknown) > 0 {
		t.Fatalf("unexpected save errors: %v", unknown)
	}
	if wins != 1 || losers != writers-1 {
		t.Fatalf("wins=%d losers=%d, want exactly 1 winner and %d conflicts", wins, losers, writers-1)
	}
	stored, err := repo.FindByPathId(ctx, pathId)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if n := len(stored.Entries()); n != 1 {
		t.Fatalf("stored pool has %d entries, want only the winner's 1", n)
	}
	if stored.Version() != 2 {
		t.Fatalf("stored version %d, want 2 (create + one winning save)", stored.Version())
	}

	// A second pool created for the same path is a conflict, not an overwrite.
	if err := repo.Save(ctx, release.NewWorkPool(pathId, release.ReleaseFed, 5, 0)); !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("re-creating an existing pool: err = %v, want ErrConcurrentModification", err)
	}
}
