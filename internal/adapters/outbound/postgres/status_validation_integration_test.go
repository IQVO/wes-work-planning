//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// Persisted status/state strings are validated on the read path: a corrupt
// or unknown stored value must surface as a wrapped domain error naming the
// aggregate, never silently become a default state inside an aggregate.

func TestWorkUnitRepo_UnknownStoredStateIsRejectedOnEveryFinder(t *testing.T) {
	db := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(db)
	cpt := time.Now().Add(time.Hour).Truncate(time.Microsecond)

	if _, err := db.Exec(ctx, `
		INSERT INTO work_units (id, path_id, cpt, reference, sku, gift_wrap, state)
		VALUES ('wu-corrupt', 'pick-corrupt', $1, 'ref-corrupt', '', false, 'Cancelled')
	`, cpt); err != nil {
		t.Fatalf("insert corrupt row: %v", err)
	}

	if _, err := repo.FindById(ctx, "wu-corrupt"); !errors.Is(err, workunit.ErrUnknownState) {
		t.Fatalf("FindById error = %v, want ErrUnknownState", err)
	}
	if _, err := repo.FindByPathId(ctx, mustPath(t, "pick-corrupt")); !errors.Is(err, workunit.ErrUnknownState) {
		t.Fatalf("FindByPathId error = %v, want ErrUnknownState", err)
	}
	if _, err := repo.FindByReference(ctx, "ref-corrupt"); !errors.Is(err, workunit.ErrUnknownState) {
		t.Fatalf("FindByReference error = %v, want ErrUnknownState", err)
	}
}

// Every state the aggregate persists must still load: no valid row may
// start failing because of the stricter parse.
func TestWorkUnitRepo_EveryPersistedStateStillLoads(t *testing.T) {
	db := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(db)
	now := time.Now().UTC().Truncate(time.Microsecond)

	pending := newGiftWrapUnit(t, "wu-ok-pending", "ref-ok", false)
	released := newGiftWrapUnit(t, "wu-ok-released", "ref-ok", false)
	if err := released.Release(now); err != nil {
		t.Fatalf("release: %v", err)
	}
	completed := newGiftWrapUnit(t, "wu-ok-completed", "ref-ok", false)
	if err := completed.Release(now); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := completed.Complete(now.Add(time.Minute)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	for _, u := range []*workunit.WorkUnit{pending, released, completed} {
		if err := repo.Save(ctx, u); err != nil {
			t.Fatalf("save %s: %v", u.Id(), err)
		}
	}

	want := map[string]workunit.State{
		"wu-ok-pending":   workunit.Pending,
		"wu-ok-released":  workunit.Released,
		"wu-ok-completed": workunit.Completed,
	}
	for id, st := range want {
		got, err := repo.FindById(ctx, id)
		if err != nil {
			t.Fatalf("FindById(%s): %v", id, err)
		}
		if got.State() != st {
			t.Fatalf("FindById(%s) state=%v, want %v", id, got.State(), st)
		}
	}
}

func TestWorkPoolRepo_UnknownStoredModeIsRejected(t *testing.T) {
	db := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkPoolRepo(db)

	if _, err := db.Exec(ctx, `
		INSERT INTO work_pools (path_id, mode, wip_limit, alarm_threshold, version)
		VALUES ('pool-bad-mode', 'BatchFed', 10, 0, 1)
	`); err != nil {
		t.Fatalf("insert corrupt pool: %v", err)
	}

	if _, err := repo.FindByPathId(ctx, mustPath(t, "pool-bad-mode")); !errors.Is(err, release.ErrUnknownFeedMode) {
		t.Fatalf("FindByPathId error = %v, want ErrUnknownFeedMode", err)
	}
}

func TestWorkPoolRepo_UnknownStoredEntryStateIsRejected(t *testing.T) {
	db := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkPoolRepo(db)
	cpt := time.Now().Add(time.Hour).Truncate(time.Microsecond)

	if _, err := db.Exec(ctx, `
		INSERT INTO work_pools (path_id, mode, wip_limit, alarm_threshold, version)
		VALUES ('pool-bad-entry', 'ReleaseFed', 10, 0, 1)
	`); err != nil {
		t.Fatalf("insert pool: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO work_pool_entries (path_id, work_unit_id, cpt, state)
		VALUES ('pool-bad-entry', 'wu-bad-entry', $1, 'cancelled')
	`, cpt); err != nil {
		t.Fatalf("insert corrupt entry: %v", err)
	}

	if _, err := repo.FindByPathId(ctx, mustPath(t, "pool-bad-entry")); !errors.Is(err, release.ErrUnknownEntryState) {
		t.Fatalf("FindByPathId error = %v, want ErrUnknownEntryState", err)
	}
}

// Both feed modes and every entry state written by Save must still load.
func TestWorkPoolRepo_EveryPersistedModeAndEntryStateStillLoads(t *testing.T) {
	db := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkPoolRepo(db)
	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))

	for name, mode := range map[string]release.FeedMode{"pool-ok-release-fed": release.ReleaseFed, "pool-ok-flow-fed": release.FlowFed} {
		wp := release.NewWorkPool(mustPath(t, name), mode, 10, 5)
		for _, id := range []string{"a", "b", "c"} {
			if err := wp.Enqueue(name+"-"+id, cpt); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}
		if err := wp.Release(name + "-b"); err != nil {
			t.Fatalf("release: %v", err)
		}
		if err := wp.Release(name + "-c"); err != nil {
			t.Fatalf("release: %v", err)
		}
		if err := wp.Complete(name + "-c"); err != nil {
			t.Fatalf("complete: %v", err)
		}
		if err := repo.Save(ctx, wp); err != nil {
			t.Fatalf("save %s: %v", name, err)
		}

		got, err := repo.FindByPathId(ctx, mustPath(t, name))
		if err != nil {
			t.Fatalf("FindByPathId(%s): %v", name, err)
		}
		if got.Mode() != mode || got.BacklogDepth() != 1 || got.WIP() != 1 || len(got.Entries()) != 3 {
			t.Fatalf("%s round trip: mode=%v backlog=%d wip=%d entries=%d", name, got.Mode(), got.BacklogDepth(), got.WIP(), len(got.Entries()))
		}
	}
}
