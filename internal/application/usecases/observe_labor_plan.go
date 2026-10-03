package usecases

import (
	"context"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ObserveLaborPlan is an additive projector use case: it records the latest
// labor plan Workforce Management reports for a path via its own
// ShiftPlanCommitted integration event. This is a read-only projection —
// it must NOT feed this service's own plan.ShiftPlan/CommitShiftPlan.
type ObserveLaborPlan struct {
	views     ports.LaborPlanViewRepo
	processed ports.ProcessedEventRepo
	uow       ports.UnitOfWork
}

func NewObserveLaborPlan(views ports.LaborPlanViewRepo, processed ports.ProcessedEventRepo) *ObserveLaborPlan {
	return &ObserveLaborPlan{views: views, processed: processed}
}

// WithUnitOfWork brackets the processed-event mark and the view Save in
// one atomic scope (ADR-0028), so a failed Save never leaves the event
// marked processed.
func (uc *ObserveLaborPlan) WithUnitOfWork(u ports.UnitOfWork) *ObserveLaborPlan {
	uc.uow = u
	return uc
}

type ObserveLaborPlanRequest struct {
	EventId      string
	PathId       shared.PathId
	PlannedHeads int
	PlannedRate  float64
	PlannedHours float64
	ObservedAt   time.Time
}

func (uc *ObserveLaborPlan) Execute(ctx context.Context, req ObserveLaborPlanRequest) error {
	_, err := onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.ObservedAt, func(ctx context.Context) error {
		return uc.views.Save(ctx, laborview.LaborPlanObserved{
			PathId:       req.PathId,
			PlannedHeads: req.PlannedHeads,
			PlannedRate:  req.PlannedRate,
			PlannedHours: req.PlannedHours,
			ObservedAt:   req.ObservedAt,
		})
	})
	return err
}
