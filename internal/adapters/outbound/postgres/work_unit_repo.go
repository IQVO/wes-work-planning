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

// lineNoToColumn maps the aggregate's "0 = unknown" onto the nullable
// line_no column: an unknown line is stored as NULL, never as 0 (ADR-0036).
func lineNoToColumn(lineNo int) *int {
	if lineNo <= 0 || lineNo > workunit.MaxLineNo {
		return nil
	}
	return &lineNo
}

// lineNoFromColumn is the inverse: NULL rehydrates as 0 (unknown).
func lineNoFromColumn(lineNo *int) int {
	if lineNo == nil {
		return 0
	}
	return *lineNo
}

func (r *WorkUnitRepo) Save(ctx context.Context, unit *workunit.WorkUnit) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO work_units (id, path_id, cpt, reference, sku, gift_wrap, transfer_ref, work_kind, site_id, quantity, state, released_at, completed_at, line_no)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (id) DO UPDATE SET
			path_id = $2, cpt = $3, reference = $4, sku = $5, gift_wrap = $6, transfer_ref = $7, work_kind = $8, site_id = $9, quantity = $10, state = $11, released_at = $12, completed_at = $13, line_no = $14
	`, unit.Id(), unit.PathId().String(), unit.CPT().Time(), unit.Reference(), unit.SKU(), unit.GiftWrap(),
		unit.TransferRef(), unit.WorkKind().String(), unit.SiteId(), unit.Quantity(),
		stateToString(unit.State()), unit.ReleasedAt(), unit.CompletedAt(), lineNoToColumn(unit.LineNo()))
	return err
}

// missingTimestamp reports a Released/Completed row whose transition
// timestamp column is NULL, instead of dereferencing the nil pointer.
func missingTimestamp(id string, st workunit.State, column string) error {
	return fmt.Errorf("rehydrate work unit %q: state %s but %s is NULL: %w", id, st, column, workunit.ErrMissingTransitionTime)
}

func (r *WorkUnitRepo) scanWorkUnit(id, pathIdStr, reference, sku, state, transferRef, siteId, workKind string, giftWrap bool, quantity int, lineNo *int, cpt time.Time, releasedAt, completedAt *time.Time) (*workunit.WorkUnit, error) {
	st, err := workunit.ParseState(state)
	if err != nil {
		return nil, fmt.Errorf("rehydrate work unit %q: state %q: %w", id, state, err)
	}

	pathId, err := shared.NewPathId(pathIdStr)
	if err != nil {
		return nil, err
	}

	// work_kind is the one transfer column with an enum: validate it on the
	// way in (the inverse of the setter), so a corrupt stored value fails
	// loud instead of rehydrating an invalid WorkKind into the aggregate —
	// the same contract ParseState gives `state`.
	var kind workunit.WorkKind
	if workKind != "" {
		kind, err = workunit.ParseWorkKind(workKind)
		if err != nil {
			return nil, fmt.Errorf("rehydrate work unit %q: %w", id, err)
		}
	}

	unit, err := workunit.NewWorkUnit(id, pathId, shared.NewCPT(cpt), reference)
	if err != nil {
		return nil, err
	}
	unit.SetSKU(sku)
	unit.SetGiftWrap(giftWrap)
	unit.SetTransferRef(transferRef)
	unit.SetWorkKind(kind)
	unit.SetSiteId(siteId)
	unit.SetQuantity(quantity)
	unit.SetLineNo(lineNoFromColumn(lineNo))

	switch st {
	case workunit.Pending:
		// NewWorkUnit already yields a Pending unit.
	case workunit.Released:
		if releasedAt == nil {
			return nil, missingTimestamp(id, st, "released_at")
		}
		if err := unit.Release(*releasedAt); err != nil {
			return nil, err
		}
	case workunit.Completed:
		if releasedAt == nil {
			return nil, missingTimestamp(id, st, "released_at")
		}
		if completedAt == nil {
			return nil, missingTimestamp(id, st, "completed_at")
		}
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
	var pathIdStr, reference, sku, state, transferRef, siteId, workKind string
	var giftWrap bool
	var quantity int
	var lineNo *int
	var cpt time.Time
	var releasedAt, completedAt *time.Time

	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT path_id, cpt, reference, sku, gift_wrap, state, released_at, completed_at, transfer_ref, work_kind, site_id, quantity, line_no
		FROM work_units WHERE id = $1
	`, id)
	if err := row.Scan(&pathIdStr, &cpt, &reference, &sku, &giftWrap, &state, &releasedAt, &completedAt, &transferRef, &workKind, &siteId, &quantity, &lineNo); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ports.ErrNotFound
		}
		return nil, err
	}

	return r.scanWorkUnit(id, pathIdStr, reference, sku, state, transferRef, siteId, workKind, giftWrap, quantity, lineNo, cpt, releasedAt, completedAt)
}

func (r *WorkUnitRepo) FindByPathId(ctx context.Context, pathId shared.PathId) ([]*workunit.WorkUnit, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT id, cpt, reference, sku, gift_wrap, state, released_at, completed_at, transfer_ref, work_kind, site_id, quantity, line_no
		FROM work_units WHERE path_id = $1
	`, pathId.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*workunit.WorkUnit
	for rows.Next() {
		var id, reference, sku, state, transferRef, siteId, workKind string
		var giftWrap bool
		var quantity int
		var lineNo *int
		var cpt time.Time
		var releasedAt, completedAt *time.Time
		if err := rows.Scan(&id, &cpt, &reference, &sku, &giftWrap, &state, &releasedAt, &completedAt, &transferRef, &workKind, &siteId, &quantity, &lineNo); err != nil {
			return nil, err
		}
		unit, err := r.scanWorkUnit(id, pathId.String(), reference, sku, state, transferRef, siteId, workKind, giftWrap, quantity, lineNo, cpt, releasedAt, completedAt)
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
		SELECT id, path_id, cpt, sku, gift_wrap, state, released_at, completed_at, transfer_ref, work_kind, site_id, quantity, line_no
		FROM work_units WHERE reference = $1
	`, reference)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*workunit.WorkUnit
	for rows.Next() {
		var id, pathIdStr, sku, state, transferRef, siteId, workKind string
		var giftWrap bool
		var quantity int
		var lineNo *int
		var cpt time.Time
		var releasedAt, completedAt *time.Time
		if err := rows.Scan(&id, &pathIdStr, &cpt, &sku, &giftWrap, &state, &releasedAt, &completedAt, &transferRef, &workKind, &siteId, &quantity, &lineNo); err != nil {
			return nil, err
		}
		unit, err := r.scanWorkUnit(id, pathIdStr, reference, sku, state, transferRef, siteId, workKind, giftWrap, quantity, lineNo, cpt, releasedAt, completedAt)
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
