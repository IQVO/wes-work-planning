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

// WorkUnitRepo implements ports.WorkUnitRepo and ports.RelocationWorkRepo
// (ADR-0040) over the work_units table.
type WorkUnitRepo struct {
	pool *pgxpool.Pool
}

func NewWorkUnitRepo(pool *pgxpool.Pool) *WorkUnitRepo {
	return &WorkUnitRepo{pool: pool}
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
	move, _ := unit.Relocation()
	repl, _ := unit.Replenishment()
	pick, _ := unit.Pick()
	brk, _ := unit.CaseBreak()
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO work_units (id, path_id, cpt, reference, sku, gift_wrap, transfer_ref, work_kind, site_id, quantity, state, released_at, completed_at, line_no,
			relocation_plan_id, relocation_move_seq, relocation_kind, from_slot, to_slot, blocked_by, cancelled_at,
			replenishment_request_id, replenishment_from_bin, replenishment_to_bin, replenishment_priority,
			pick_unit, pick_from_bin, case_break_lpn, case_break_loose_bin, case_break_pick_task_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)
		ON CONFLICT (id) DO UPDATE SET
			path_id = $2, cpt = $3, reference = $4, sku = $5, gift_wrap = $6, transfer_ref = $7, work_kind = $8, site_id = $9, quantity = $10, state = $11, released_at = $12, completed_at = $13, line_no = $14,
			relocation_plan_id = $15, relocation_move_seq = $16, relocation_kind = $17, from_slot = $18, to_slot = $19, blocked_by = $20, cancelled_at = $21,
			replenishment_request_id = $22, replenishment_from_bin = $23, replenishment_to_bin = $24, replenishment_priority = $25,
			pick_unit = $26, pick_from_bin = $27, case_break_lpn = $28, case_break_loose_bin = $29, case_break_pick_task_id = $30
	`, unit.Id(), unit.PathId().String(), unit.CPT().Time(), unit.Reference(), unit.SKU(), unit.GiftWrap(),
		unit.TransferRef(), unit.WorkKind().String(), unit.SiteId(), unit.Quantity(),
		unit.State().String(), unit.ReleasedAt(), unit.CompletedAt(), lineNoToColumn(unit.LineNo()),
		move.PlanId, move.MoveSeq, move.Kind.String(), move.FromSlot, move.ToSlot, unit.BlockedBy(), unit.CancelledAt(),
		repl.RequestId, repl.FromBin, repl.ToBin, repl.Priority.String(),
		pick.Unit.String(), pick.FromBin, brk.LPN, brk.LooseBin, brk.PickTaskId)
	return err
}

// workUnitColumns is the SELECT list every read uses; workUnitRow.dest
// scans it in the same order.
const workUnitColumns = `id, path_id, cpt, reference, sku, gift_wrap, state, released_at, completed_at,
	transfer_ref, work_kind, site_id, quantity, line_no,
	relocation_plan_id, relocation_move_seq, relocation_kind, from_slot, to_slot, blocked_by, cancelled_at,
	replenishment_request_id, replenishment_from_bin, replenishment_to_bin, replenishment_priority,
	pick_unit, pick_from_bin, case_break_lpn, case_break_loose_bin, case_break_pick_task_id`

// workUnitRow is one work_units row as stored.
type workUnitRow struct {
	id, pathId, reference, sku, state    string
	transferRef, workKind, siteId        string
	giftWrap                             bool
	quantity                             int
	lineNo                               *int
	cpt                                  time.Time
	releasedAt, completedAt, cancelledAt *time.Time
	relocationPlanId, relocationKind     string
	relocationMoveSeq                    int
	fromSlot, toSlot, blockedBy          string
	// replenishment* are the ADR-0042 columns; replenishmentPriority is ""
	// for every unit that is not REPLENISH work.
	replenishmentRequestId, replenishmentFromBin string
	replenishmentToBin, replenishmentPriority    string
	// pick* and caseBreak* are the ADR-0045 columns; pickUnit / caseBreakLPN
	// are "" for every unit that is not pack-aware work.
	pickUnit, pickFromBin                                string
	caseBreakLPN, caseBreakLooseBin, caseBreakPickTaskId string
}

func (w *workUnitRow) dest() []any {
	return []any{&w.id, &w.pathId, &w.cpt, &w.reference, &w.sku, &w.giftWrap, &w.state, &w.releasedAt, &w.completedAt,
		&w.transferRef, &w.workKind, &w.siteId, &w.quantity, &w.lineNo,
		&w.relocationPlanId, &w.relocationMoveSeq, &w.relocationKind, &w.fromSlot, &w.toSlot, &w.blockedBy, &w.cancelledAt,
		&w.replenishmentRequestId, &w.replenishmentFromBin, &w.replenishmentToBin, &w.replenishmentPriority,
		&w.pickUnit, &w.pickFromBin, &w.caseBreakLPN, &w.caseBreakLooseBin, &w.caseBreakPickTaskId}
}

// missingTimestamp reports a Released/Completed/Cancelled row whose
// transition timestamp column is NULL, instead of dereferencing the nil
// pointer.
func missingTimestamp(id string, st workunit.State, column string) error {
	return fmt.Errorf("rehydrate work unit %q: state %s but %s is NULL: %w", id, st, column, workunit.ErrMissingTransitionTime)
}

// scanWorkUnit rehydrates one stored row into the aggregate, validating the
// enum columns (state, work_kind, relocation_kind) on the way in.
func (r *WorkUnitRepo) scanWorkUnit(row workUnitRow) (*workunit.WorkUnit, error) {
	st, err := workunit.ParseState(row.state)
	if err != nil {
		return nil, fmt.Errorf("rehydrate work unit %q: state %q: %w", row.id, row.state, err)
	}
	pathId, err := shared.NewPathId(row.pathId)
	if err != nil {
		return nil, err
	}
	unit, err := workunit.NewWorkUnit(row.id, pathId, shared.NewCPT(row.cpt), row.reference)
	if err != nil {
		return nil, err
	}
	if err := applyDescriptiveColumns(unit, row); err != nil {
		return nil, err
	}
	if err := replayLifecycle(unit, st, row); err != nil {
		return nil, err
	}
	return unit, nil
}

// applyDescriptiveColumns sets the additive, non-lifecycle fields.
func applyDescriptiveColumns(unit *workunit.WorkUnit, row workUnitRow) error {
	// work_kind is validated on the way in (the inverse of the setter), so
	// a corrupt stored value fails loud instead of rehydrating an invalid
	// WorkKind — the same contract ParseState gives `state`.
	if row.workKind != "" {
		kind, err := workunit.ParseWorkKind(row.workKind)
		if err != nil {
			return fmt.Errorf("rehydrate work unit %q: %w", row.id, err)
		}
		unit.SetWorkKind(kind)
	}
	unit.SetSKU(row.sku)
	unit.SetGiftWrap(row.giftWrap)
	unit.SetTransferRef(row.transferRef)
	unit.SetSiteId(row.siteId)
	unit.SetQuantity(row.quantity)
	unit.SetLineNo(lineNoFromColumn(row.lineNo))
	unit.SetBlockedBy(row.blockedBy)
	if err := applyRelocationColumns(unit, row); err != nil {
		return err
	}
	if err := applyReplenishmentColumns(unit, row); err != nil {
		return err
	}
	return applyPackAwareColumns(unit, row)
}

// applyPackAwareColumns rehydrates the pick plan of a CASE_PICK / EACH_PICK
// row and the case break of a BREAK_CASE row (ADR-0045) from their own
// columns plus the unit's sku and quantity; a corrupt unit or an incomplete
// value fails loud, like the other kinds.
func applyPackAwareColumns(unit *workunit.WorkUnit, row workUnitRow) error {
	if row.pickUnit != "" {
		plan, err := workunit.NewPickPlan(row.sku, row.quantity, row.pickUnit, row.pickFromBin)
		if err != nil {
			return fmt.Errorf("rehydrate work unit %q: %w", row.id, err)
		}
		unit.SetPick(plan)
	} else if unit.WorkKind().IsPickKind() {
		return fmt.Errorf("rehydrate work unit %q: %s row without its pick columns: %w", row.id, unit.WorkKind(), workunit.ErrInvalidPickPlan)
	}
	if row.caseBreakLPN != "" {
		brk, err := workunit.NewCaseBreak(row.caseBreakLPN, row.sku, row.caseBreakLooseBin, row.caseBreakPickTaskId)
		if err != nil {
			return fmt.Errorf("rehydrate work unit %q: %w", row.id, err)
		}
		unit.SetCaseBreak(brk)
	} else if unit.WorkKind() == workunit.WorkKindBreakCase {
		return fmt.Errorf("rehydrate work unit %q: BREAK_CASE row without its case break columns: %w", row.id, workunit.ErrInvalidCaseBreak)
	}
	return nil
}

// applyRelocationColumns rehydrates the slot-plan move of a RELOCATE row.
func applyRelocationColumns(unit *workunit.WorkUnit, row workUnitRow) error {
	if row.relocationKind == "" {
		return nil
	}
	move, err := workunit.NewRelocationMove(row.relocationPlanId, row.relocationMoveSeq, row.relocationKind, row.fromSlot, row.toSlot)
	if err != nil {
		return fmt.Errorf("rehydrate work unit %q: %w", row.id, err)
	}
	unit.SetRelocation(move)
	return nil
}

// applyReplenishmentColumns rehydrates the pick-face replenishment of a
// REPLENISH row (ADR-0042) from its own columns plus the unit's sku and
// quantity; a corrupt priority or an incomplete move fails loud.
func applyReplenishmentColumns(unit *workunit.WorkUnit, row workUnitRow) error {
	if row.replenishmentPriority == "" {
		if unit.WorkKind() == workunit.WorkKindReplenish {
			return fmt.Errorf("rehydrate work unit %q: REPLENISH row without its replenishment columns: %w", row.id, workunit.ErrInvalidReplenishmentMove)
		}
		return nil
	}
	move, err := workunit.NewReplenishmentMove(row.replenishmentRequestId, row.sku, row.replenishmentFromBin, row.replenishmentToBin, row.quantity, row.replenishmentPriority)
	if err != nil {
		return fmt.Errorf("rehydrate work unit %q: %w", row.id, err)
	}
	unit.SetReplenishment(move)
	return nil
}

// replayLifecycle replays the stored state onto a fresh Pending unit.
func replayLifecycle(unit *workunit.WorkUnit, st workunit.State, row workUnitRow) error {
	if st == workunit.Cancelled {
		if row.cancelledAt == nil {
			return missingTimestamp(row.id, st, "cancelled_at")
		}
		return unit.Cancel(*row.cancelledAt)
	}
	if st == workunit.Pending {
		return nil
	}
	if row.releasedAt == nil {
		return missingTimestamp(row.id, st, "released_at")
	}
	if err := unit.Release(*row.releasedAt); err != nil {
		return err
	}
	if st == workunit.Released {
		return nil
	}
	if row.completedAt == nil {
		return missingTimestamp(row.id, st, "completed_at")
	}
	return unit.Complete(*row.completedAt)
}

func (r *WorkUnitRepo) FindById(ctx context.Context, id string) (*workunit.WorkUnit, error) {
	var row workUnitRow
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `SELECT `+workUnitColumns+` FROM work_units WHERE id = $1`, id).Scan(row.dest()...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return r.scanWorkUnit(row)
}

func (r *WorkUnitRepo) FindByPathId(ctx context.Context, pathId shared.PathId) ([]*workunit.WorkUnit, error) {
	return r.query(ctx, `SELECT `+workUnitColumns+` FROM work_units WHERE path_id = $1`, pathId.String())
}

// FindByReference returns every WorkUnit carrying the given external
// reference (e.g. an order line, or a slot plan id). A reference can
// plausibly have more than one WorkUnit across retries/history, so this
// returns a slice; an empty slice (not ports.ErrNotFound) when nothing
// matches.
func (r *WorkUnitRepo) FindByReference(ctx context.Context, reference string) ([]*workunit.WorkUnit, error) {
	return r.query(ctx, `SELECT `+workUnitColumns+` FROM work_units WHERE reference = $1`, reference)
}

// PendingRelocationsAtSite returns every Pending RELOCATE work unit of
// siteId (ADR-0040), served by idx_work_units_relocate_site_state.
func (r *WorkUnitRepo) PendingRelocationsAtSite(ctx context.Context, siteId string) ([]*workunit.WorkUnit, error) {
	return r.query(ctx, `SELECT `+workUnitColumns+` FROM work_units
		WHERE work_kind = 'RELOCATE' AND site_id = $1 AND state = 'Pending' ORDER BY id`, siteId)
}

// PendingBlockedBy returns every Pending work unit waiting on workUnitId
// (ADR-0040), served by idx_work_units_blocked_by.
func (r *WorkUnitRepo) PendingBlockedBy(ctx context.Context, workUnitId string) ([]*workunit.WorkUnit, error) {
	return r.query(ctx, `SELECT `+workUnitColumns+` FROM work_units
		WHERE blocked_by = $1 AND state = 'Pending' ORDER BY id`, workUnitId)
}

// query runs a SELECT of workUnitColumns and rehydrates every row; an empty
// slice when nothing matches.
func (r *WorkUnitRepo) query(ctx context.Context, sql string, args ...any) ([]*workunit.WorkUnit, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []*workunit.WorkUnit{}
	for rows.Next() {
		var row workUnitRow
		if err := rows.Scan(row.dest()...); err != nil {
			return nil, err
		}
		unit, err := r.scanWorkUnit(row)
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
