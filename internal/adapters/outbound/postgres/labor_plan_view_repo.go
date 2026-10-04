package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

type LaborPlanViewRepo struct {
	pool *pgxpool.Pool
}

func NewLaborPlanViewRepo(pool *pgxpool.Pool) *LaborPlanViewRepo {
	return &LaborPlanViewRepo{pool: pool}
}

func (r *LaborPlanViewRepo) Save(ctx context.Context, view laborview.LaborPlanObserved) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO labor_plan_view (path_id, planned_heads, planned_rate, planned_hours, observed_at, drift_heads, drift_detected_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (path_id) DO UPDATE SET
			planned_heads = $2, planned_rate = $3, planned_hours = $4, observed_at = $5,
			drift_heads = $6, drift_detected_at = $7
	`, view.PathId.String(), view.PlannedHeads, view.PlannedRate, view.PlannedHours, view.ObservedAt, view.DriftHeads, view.DriftDetectedAt)
	return err
}

// SaveDrift implements ports.LaborPlanViewRepo: it updates only the drift
// columns, so a commit of our own plan never overwrites the observed plan.
func (r *LaborPlanViewRepo) SaveDrift(ctx context.Context, pathId shared.PathId, driftHeads int, detectedAt *time.Time) error {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		UPDATE labor_plan_view SET drift_heads = $2, drift_detected_at = $3 WHERE path_id = $1
	`, pathId.String(), driftHeads, detectedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrNotFound
	}
	return nil
}

func (r *LaborPlanViewRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (laborview.LaborPlanObserved, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT planned_heads, planned_rate, planned_hours, observed_at, drift_heads, drift_detected_at
		FROM labor_plan_view WHERE path_id = $1
	`, pathId.String())

	var v laborview.LaborPlanObserved
	v.PathId = pathId
	if err := row.Scan(&v.PlannedHeads, &v.PlannedRate, &v.PlannedHours, &v.ObservedAt, &v.DriftHeads, &v.DriftDetectedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return laborview.LaborPlanObserved{}, ports.ErrNotFound
		}
		return laborview.LaborPlanObserved{}, err
	}
	return v, nil
}
