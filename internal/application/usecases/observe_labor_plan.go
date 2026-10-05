package usecases

import (
	"context"
	"errors"
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
	plans     ports.PlanRepo
	publisher ports.EventPublisher
	clock     ports.Clock
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

// WithDriftReconciliation enables the ADR-0019 reconciliation: after the
// observed plan is saved it is compared against OUR committed PathPlan for the
// same path (if one exists), and a PathPlanDriftDetected event is published in
// the same UnitOfWork scope — i.e. through the outbox — when they disagree.
// Optional: without it ObserveLaborPlan stays a pure projection.
func (uc *ObserveLaborPlan) WithDriftReconciliation(plans ports.PlanRepo, publisher ports.EventPublisher, clock ports.Clock) *ObserveLaborPlan {
	uc.plans, uc.publisher, uc.clock = plans, publisher, clock
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
		view := laborview.LaborPlanObserved{
			PathId:       req.PathId,
			PlannedHeads: req.PlannedHeads,
			PlannedRate:  req.PlannedRate,
			PlannedHours: req.PlannedHours,
			ObservedAt:   req.ObservedAt,
		}
		view, drift, err := uc.reconcile(ctx, view)
		if err != nil {
			return err
		}
		if err := uc.views.Save(ctx, view); err != nil {
			return err
		}
		if drift == nil {
			return nil
		}
		return uc.publisher.Publish(ctx, *drift)
	})
	return err
}

// reconcile is the "Workforce's plan committed second" trigger of ADR-0019:
// it compares the freshly observed plan against our committed PathPlan. With
// reconciliation disabled, or no PathPlan committed for the path yet, there is
// nothing to compare — the view is returned untouched with no event (no
// fabricated drift against an absent fact).
func (uc *ObserveLaborPlan) reconcile(ctx context.Context, view laborview.LaborPlanObserved) (laborview.LaborPlanObserved, *shared.PathPlanDriftDetected, error) {
	if uc.plans == nil {
		return view, nil, nil
	}
	shiftPlan, err := uc.plans.FindByPathId(ctx, view.PathId)
	if errors.Is(err, ports.ErrNotFound) {
		return view, nil, nil
	}
	if err != nil {
		return view, nil, err
	}
	pathPlan, ok := shiftPlan.PathPlan(view.PathId)
	if !ok {
		return view, nil, nil
	}
	view, drift := reconcileHeads(view, pathPlan.PlannedHeads().Value(), uc.clock.Now())
	return view, drift, nil
}
