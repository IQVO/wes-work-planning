package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/relocation"
)

// SlotPlanRepo implements ports.SlotPlanRepo over slot_plans (ADR-0040).
type SlotPlanRepo struct {
	pool *pgxpool.Pool
}

func NewSlotPlanRepo(pool *pgxpool.Pool) *SlotPlanRepo {
	return &SlotPlanRepo{pool: pool}
}

// Save records a plan, joining the enclosing UnitOfWork; the same plan id
// again is a no-op (ON CONFLICT DO NOTHING).
func (r *SlotPlanRepo) Save(ctx context.Context, receipt relocation.Receipt) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO slot_plans (plan_id, site_id, approved_at, supersedes_plan_id, move_count)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (plan_id) DO NOTHING
	`, receipt.PlanId, receipt.SiteId, receipt.ApprovedAt, receipt.SupersedesPlanId, receipt.MoveCount)
	return err
}

// LatestAtSite returns the site's latest-approved plan, ties broken by the
// greater plan id (relocation.Receipt.Supersedes), or ports.ErrNotFound.
func (r *SlotPlanRepo) LatestAtSite(ctx context.Context, siteId string) (relocation.Receipt, error) {
	var rec relocation.Receipt
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT plan_id, site_id, approved_at, supersedes_plan_id, move_count
		FROM slot_plans WHERE site_id = $1
		ORDER BY approved_at DESC, plan_id DESC LIMIT 1
	`, siteId).Scan(&rec.PlanId, &rec.SiteId, &rec.ApprovedAt, &rec.SupersedesPlanId, &rec.MoveCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return relocation.Receipt{}, ports.ErrNotFound
	}
	if err != nil {
		return relocation.Receipt{}, err
	}
	return rec, nil
}
