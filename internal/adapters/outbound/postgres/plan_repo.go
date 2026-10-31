package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/pickload"
	"github.com/claudioed/wes-work-planning/internal/domain/plan"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

type PlanRepo struct {
	pool *pgxpool.Pool
}

func NewPlanRepo(pool *pgxpool.Pool) *PlanRepo {
	return &PlanRepo{pool: pool}
}

func (r *PlanRepo) Save(ctx context.Context, pathId shared.PathId, shiftPlan *plan.ShiftPlan) error {
	pathPlan, ok := shiftPlan.PathPlan(pathId)
	if !ok {
		return errors.New("shift plan has no path plan for the given path id")
	}

	// ADR-0017: the optional travel-distance hint is stored as two nullable
	// columns; NULL travel_distance_m means "no hint recorded" (0 m is a
	// valid distance, so it cannot double as the sentinel).
	var travelM *float64
	var travelEstimated *bool
	if pathPlan.TravelDistanceKnown() {
		m, est := pathPlan.TravelDistanceM(), pathPlan.TravelDistanceEstimated()
		travelM, travelEstimated = &m, &est
	}

	// ADR-0045: the optional per-handle load, three nullable columns. NULL
	// handle_unit = none recorded; NULL weight / cube = unknown.
	var handleUnit *string
	var handleWeightG, handleCubeMm3 *int64
	if h, ok := pathPlan.HandleLoad(); ok {
		unit := h.Unit.String()
		handleUnit = &unit
		if h.WeightKnown {
			handleWeightG = &h.WeightG
		}
		if h.CubeKnown {
			handleCubeMm3 = &h.CubeMm3
		}
	}

	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO shift_plans (path_id, planned_heads, installed_stations, rate_units_per_hr, hours,
			travel_distance_m, travel_distance_estimated, handle_unit, handle_weight_g, handle_cube_mm3)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (path_id) DO UPDATE SET
			planned_heads = $2, installed_stations = $3, rate_units_per_hr = $4, hours = $5,
			travel_distance_m = $6, travel_distance_estimated = $7,
			handle_unit = $8, handle_weight_g = $9, handle_cube_mm3 = $10
	`, pathId.String(), pathPlan.PlannedHeads().Value(), pathPlan.InstalledStations().Value(), pathPlan.Rate().UnitsPerHour(), pathPlan.Hours(),
		travelM, travelEstimated, handleUnit, handleWeightG, handleCubeMm3)
	return err
}

func (r *PlanRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (*plan.ShiftPlan, error) {
	var plannedHeads, installedStations int
	var rateUnitsPerHr, hours float64
	var travelM *float64
	var travelEstimated *bool
	var handleUnit *string
	var handleWeightG, handleCubeMm3 *int64

	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT planned_heads, installed_stations, rate_units_per_hr, hours,
			travel_distance_m, travel_distance_estimated, handle_unit, handle_weight_g, handle_cube_mm3
		FROM shift_plans WHERE path_id = $1
	`, pathId.String())
	if err := row.Scan(&plannedHeads, &installedStations, &rateUnitsPerHr, &hours, &travelM, &travelEstimated, &handleUnit, &handleWeightG, &handleCubeMm3); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ports.ErrNotFound
		}
		return nil, err
	}

	heads, err := shared.NewStationCount(plannedHeads)
	if err != nil {
		return nil, err
	}
	installed, err := shared.NewStationCount(installedStations)
	if err != nil {
		return nil, err
	}
	rate, err := shared.NewRate(rateUnitsPerHr)
	if err != nil {
		return nil, err
	}

	pathPlan, err := plan.NewPathPlan(pathId, heads, installed, rate, hours)
	if err != nil {
		return nil, err
	}
	if travelM != nil {
		pathPlan.SetTravelDistance(*travelM, travelEstimated != nil && *travelEstimated)
	}

	if handleUnit != nil {
		pickUnit, err := workunit.ParsePickUnit(*handleUnit)
		if err != nil {
			return nil, fmt.Errorf("rehydrate shift plan %q: %w", pathId, err)
		}
		load := pickload.Load{Unit: pickUnit, Handles: 1}
		if handleWeightG != nil {
			load.WeightG, load.WeightKnown = *handleWeightG, true
		}
		if handleCubeMm3 != nil {
			load.CubeMm3, load.CubeKnown = *handleCubeMm3, true
		}
		pathPlan.SetHandleLoad(load)
	}

	return plan.NewShiftPlan([]plan.PathPlan{pathPlan})
}
