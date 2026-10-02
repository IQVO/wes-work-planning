package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

type WorkPoolRepo struct {
	pool *pgxpool.Pool
}

func NewWorkPoolRepo(pool *pgxpool.Pool) *WorkPoolRepo {
	return &WorkPoolRepo{pool: pool}
}

func modeToString(m release.FeedMode) string {
	if m == release.FlowFed {
		return "FlowFed"
	}
	return "ReleaseFed"
}

func stringToMode(s string) release.FeedMode {
	if s == "FlowFed" {
		return release.FlowFed
	}
	return release.ReleaseFed
}

func (r *WorkPoolRepo) Save(ctx context.Context, wp *release.WorkPool) error {
	// Join the enclosing UnitOfWork transaction when there is one (so a
	// use case rollback also undoes this pool write), else run in a
	// transaction of our own — the entries rewrite must be atomic either way.
	tx, commit, rollback, err := beginOrJoin(ctx, r.pool)
	if err != nil {
		return err
	}
	defer func() { _ = rollback(ctx) }()

	if err := saveVersionedPoolRow(ctx, tx, wp); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM work_pool_entries WHERE path_id = $1`, wp.PathId().String()); err != nil {
		return err
	}

	for _, e := range wp.Entries() {
		state := "pending"
		switch {
		case e.Completed:
			state = "completed"
		case e.Released:
			state = "released"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO work_pool_entries (path_id, work_unit_id, cpt, state)
			VALUES ($1, $2, $3, $4)
		`, wp.PathId().String(), e.WorkUnitId, e.CPT.Time(), state); err != nil {
			return err
		}
	}

	if err := commit(ctx); err != nil {
		return err
	}
	wp.SetVersion(wp.Version() + 1)
	return nil
}

// saveVersionedPoolRow writes the work_pools row guarded by the version the
// aggregate was loaded at: a first save inserts version 1, and every later
// save only matches `version = loaded`. Zero rows affected means another
// writer saved this pool in between -- ports.ErrConcurrentModification,
// returned before any entry row is touched (the caller's transaction rolls
// back, nothing is lost, and the caller re-loads and retries).
func saveVersionedPoolRow(ctx context.Context, tx querier, wp *release.WorkPool) error {
	if wp.Version() == 0 {
		tag, err := tx.Exec(ctx, `
			INSERT INTO work_pools (path_id, mode, wip_limit, alarm_threshold, version)
			VALUES ($1, $2, $3, $4, 1)
			ON CONFLICT (path_id) DO NOTHING
		`, wp.PathId().String(), modeToString(wp.Mode()), wp.WIPLimit(), wp.AlarmThreshold())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ports.ErrConcurrentModification
		}
		return nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE work_pools SET mode = $2, wip_limit = $3, alarm_threshold = $4, version = version + 1
		WHERE path_id = $1 AND version = $5
	`, wp.PathId().String(), modeToString(wp.Mode()), wp.WIPLimit(), wp.AlarmThreshold(), wp.Version())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrConcurrentModification
	}
	return nil
}

func (r *WorkPoolRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (*release.WorkPool, error) {
	var modeStr string
	var wipLimit, alarmThreshold int
	var version int64
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `SELECT mode, wip_limit, alarm_threshold, version FROM work_pools WHERE path_id = $1`, pathId.String())
	if err := row.Scan(&modeStr, &wipLimit, &alarmThreshold, &version); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ports.ErrNotFound
		}
		return nil, err
	}

	wp := release.NewWorkPool(pathId, stringToMode(modeStr), wipLimit, alarmThreshold)
	wp.SetVersion(version)

	if err := hydratePoolEntries(ctx, querierFrom(ctx, r.pool), pathId, wp); err != nil {
		return nil, err
	}

	return wp, nil
}

// hydratePoolEntries rehydrates wp's entries (CPT-ascending) from the
// work_pool_entries rows of pathId, replaying each stored state onto the
// aggregate: every stored entry is enqueued, "released"/"completed" rows
// are released, and "completed" rows additionally completed.
func hydratePoolEntries(ctx context.Context, q querier, pathId shared.PathId, wp *release.WorkPool) error {
	rows, err := q.Query(ctx, `
		SELECT work_unit_id, cpt, state FROM work_pool_entries
		WHERE path_id = $1 ORDER BY cpt ASC
	`, pathId.String())
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var workUnitId, state string
		var cpt time.Time
		if err := rows.Scan(&workUnitId, &cpt, &state); err != nil {
			return err
		}
		if err := enqueueEntryState(wp, workUnitId, cpt, state); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

// enqueueEntryState replays one stored entry row onto wp: enqueue first,
// then release for "released"/"completed" states, then complete for
// "completed" — the inverse of Save's state column derivation.
func enqueueEntryState(wp *release.WorkPool, workUnitId string, cpt time.Time, state string) error {
	return wp.RestoreEntry(workUnitId, shared.NewCPT(cpt), state == "released" || state == "completed", state == "completed")
}
