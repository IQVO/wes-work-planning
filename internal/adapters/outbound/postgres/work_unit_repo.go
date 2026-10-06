package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

type WorkUnitRepo struct {
	pool *pgxpool.Pool
}

func NewWorkUnitRepo(pool *pgxpool.Pool) *WorkUnitRepo {
	return &WorkUnitRepo{pool: pool}
}

func stateToString(s workunit.State) string {
	return s.String()
}

func (r *WorkUnitRepo) Save(ctx context.Context, unit *workunit.WorkUnit) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO work_units (id, path_id, cpt, reference, sku, gift_wrap, state, released_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET
			path_id = $2, cpt = $3, reference = $4, sku = $5, gift_wrap = $6, state = $7, released_at = $8, completed_at = $9
	`, unit.Id(), unit.PathId().String(), unit.CPT().Time(), unit.Reference(), unit.SKU(), unit.GiftWrap(), stateToString(unit.State()), unit.ReleasedAt(), unit.CompletedAt())
	return err
}

func (r *WorkUnitRepo) scanWorkUnit(id, pathIdStr, reference, sku, state string, giftWrap bool, cpt time.Time, releasedAt, completedAt *time.Time) (*workunit.WorkUnit, error) {
	st, err := workunit.ParseState(state)
	if err != nil {
		return nil, fmt.Errorf("rehydrate work unit %q: state %q: %w", id, state, err)
	}

	pathId, err := shared.NewPathId(pathIdStr)
	if err != nil {
		return nil, err
	}

	unit, err := workunit.NewWorkUnit(id, pathId, shared.NewCPT(cpt), reference)
	if err != nil {
		return nil, err
	}
	unit.SetSKU(sku)
	unit.SetGiftWrap(giftWrap)

	switch st {
	case workunit.Pending:
		// NewWorkUnit already yields a Pending unit.
	case workunit.Released:
		if err := unit.Release(*releasedAt); err != nil {
			return nil, err
		}
	case workunit.Completed:
		if err := unit.Release(*releasedAt); err != nil {
			return nil, err
		}
		if err := unit.Complete(*completedAt); err != nil {
			return nil, err
		}
	}

	return unit, nil
}

func (r *WorkUnitRepo) FindById(ctx context.Context, id string) (*workunit.WorkUnit, error) {
	var pathIdStr, reference, sku, state string
	var giftWrap bool
	var cpt time.Time
	var releasedAt, completedAt *time.Time

	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT path_id, cpt, reference, sku, gift_wrap, state, released_at, completed_at
		FROM work_units WHERE id = $1
	`, id)
	if err := row.Scan(&pathIdStr, &cpt, &reference, &sku, &giftWrap, &state, &releasedAt, &completedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ports.ErrNotFound
		}
		return nil, err
	}

	return r.scanWorkUnit(id, pathIdStr, reference, sku, state, giftWrap, cpt, releasedAt, completedAt)
}

func (r *WorkUnitRepo) FindByPathId(ctx context.Context, pathId shared.PathId) ([]*workunit.WorkUnit, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT id, cpt, reference, sku, gift_wrap, state, released_at, completed_at
		FROM work_units WHERE path_id = $1
	`, pathId.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*workunit.WorkUnit
	for rows.Next() {
		var id, reference, sku, state string
		var giftWrap bool
		var cpt time.Time
		var releasedAt, completedAt *time.Time
		if err := rows.Scan(&id, &cpt, &reference, &sku, &giftWrap, &state, &releasedAt, &completedAt); err != nil {
			return nil, err
		}
		unit, err := r.scanWorkUnit(id, pathId.String(), reference, sku, state, giftWrap, cpt, releasedAt, completedAt)
		if err != nil {
			return nil, err
		}
		out = append(out, unit)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// FindByReference returns every WorkUnit carrying the given external
// reference (e.g. an order line). A reference can plausibly have more than
// one WorkUnit across retries/history, so this returns a slice; an empty
// slice (not ports.ErrNotFound) when nothing matches.
func (r *WorkUnitRepo) FindByReference(ctx context.Context, reference string) ([]*workunit.WorkUnit, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT id, path_id, cpt, sku, gift_wrap, state, released_at, completed_at
		FROM work_units WHERE reference = $1
	`, reference)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*workunit.WorkUnit
	for rows.Next() {
		var id, pathIdStr, sku, state string
		var giftWrap bool
		var cpt time.Time
		var releasedAt, completedAt *time.Time
		if err := rows.Scan(&id, &pathIdStr, &cpt, &sku, &giftWrap, &state, &releasedAt, &completedAt); err != nil {
			return nil, err
		}
		unit, err := r.scanWorkUnit(id, pathIdStr, reference, sku, state, giftWrap, cpt, releasedAt, completedAt)
		if err != nil {
			return nil, err
		}
		out = append(out, unit)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
