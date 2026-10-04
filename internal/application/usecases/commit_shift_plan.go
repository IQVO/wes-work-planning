package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/plan"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// CommitShiftPlan commits the rate x heads x hours split for a process path,
// validating the plannedHeads <= installedStations invariant.
type CommitShiftPlan struct {
	plans      ports.PlanRepo
	publisher  ports.EventPublisher
	clock      ports.Clock
	uow        ports.UnitOfWork
	travelDist ports.TravelDistanceLookup
	laborViews ports.LaborPlanViewRepo
}

func NewCommitShiftPlan(plans ports.PlanRepo, publisher ports.EventPublisher, clock ports.Clock) *CommitShiftPlan {
	return &CommitShiftPlan{plans: plans, publisher: publisher, clock: clock}
}

// WithUnitOfWork brackets Save + Publish in one atomic scope (ADR-0014).
// Optional: nil keeps the two calls running back to back.
func (uc *CommitShiftPlan) WithUnitOfWork(u ports.UnitOfWork) *CommitShiftPlan {
	uc.uow = u
	return uc
}

// WithTravelDistanceLookup wires the outbound facility-layout travel-graph
// read (ADR-0017, Phase B3). Optional: a nil (or never-called) lookup
// means Execute never attempts the enrichment and every committed
// PathPlan simply has TravelDistanceKnown()==false, identical to this
// use case's behaviour before this feature existed.
func (uc *CommitShiftPlan) WithTravelDistanceLookup(l ports.TravelDistanceLookup) *CommitShiftPlan {
	uc.travelDist = l
	return uc
}

// WithLaborPlanViews enables the ADR-0019 reconciliation: after our PathPlan
// is saved, it is compared against Workforce's LaborPlanObserved for the same
// path (if one has been observed), and a PathPlanDriftDetected event is
// published inside the same UnitOfWork scope when they disagree. Optional:
// nil keeps CommitShiftPlan exactly as it was before ADR-0019.
func (uc *CommitShiftPlan) WithLaborPlanViews(v ports.LaborPlanViewRepo) *CommitShiftPlan {
	uc.laborViews = v
	return uc
}

type CommitShiftPlanRequest struct {
	PathId            shared.PathId
	PlannedHeads      shared.StationCount
	InstalledStations shared.StationCount
	Rate              shared.Rate
	Hours             float64
	// FromLocationCode/ToLocationCode are an OPTIONAL pair of
	// facility-layout LocationCodes (e.g. a path's staging aisle and its
	// pack station) used to enrich the committed PathPlan with a real
	// travel-distance hint (ADR-0017, Phase B3). Both must be set
	// together to attempt the lookup; either empty skips it entirely —
	// this is a caller-opt-in enrichment, not a required field.
	FromLocationCode string
	ToLocationCode   string
}

func (uc *CommitShiftPlan) Execute(ctx context.Context, req CommitShiftPlanRequest) (*plan.ShiftPlan, error) {
	pathPlan, err := plan.NewPathPlan(req.PathId, req.PlannedHeads, req.InstalledStations, req.Rate, req.Hours)
	if err != nil {
		return nil, err
	}

	uc.enrichWithTravelDistance(ctx, &pathPlan, req.FromLocationCode, req.ToLocationCode)

	shiftPlan, err := plan.NewShiftPlan([]plan.PathPlan{pathPlan})
	if err != nil {
		return nil, err
	}

	now := uc.clock.Now()
	event := shared.NewShiftPlanCommitted(req.PathId, now)
	err = atomically(ctx, uc.uow, func(ctx context.Context) error {
		if err := uc.plans.Save(ctx, req.PathId, shiftPlan); err != nil {
			return err
		}
		if err := uc.publisher.Publish(ctx, event); err != nil {
			return err
		}
		return uc.reconcileDrift(ctx, req.PathId, pathPlan.PlannedHeads().Value(), now)
	})
	if err != nil {
		return nil, err
	}

	return shiftPlan, nil
}

// reconcileDrift is the "our plan committed second" trigger of ADR-0019. If
// Workforce has not committed a plan for this path yet there is nothing to
// compare — it raises nothing and records nothing (no fabricated drift against
// an absent fact). Otherwise it records the outcome on the view WITHOUT
// rewriting the observed plan, and publishes PathPlanDriftDetected when the
// plans disagree. It runs inside the caller's UnitOfWork scope.
func (uc *CommitShiftPlan) reconcileDrift(ctx context.Context, pathId shared.PathId, wesPlannedHeads int, now time.Time) error {
	if uc.laborViews == nil {
		return nil
	}
	view, err := uc.laborViews.FindByPathId(ctx, pathId)
	if errors.Is(err, ports.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	view, drift := reconcileHeads(view, wesPlannedHeads, now)
	if err := uc.laborViews.SaveDrift(ctx, pathId, *view.DriftHeads, view.DriftDetectedAt); err != nil {
		return err
	}
	if drift == nil {
		return nil
	}
	return uc.publisher.Publish(ctx, *drift)
}

// enrichWithTravelDistance looks up the travel distance between from and
// to once, at commit time, and stamps it onto pathPlan when known. This
// is fail-open, always — never blocks committing the plan. A missing
// from/to, a nil TravelDistanceLookup (mirrors PermissiveLookup's own
// Known=false), an unknown location, a cross-zone pair (facility-layout
// refuses those rather than guessing, see ADR-0017 there), or a lookup
// error (timeout, 5xx, facility-layout down) are all treated identically:
// no hint, and Execute still succeeds. Mirrors ReleaseNextWork's
// classificationHints (ADR-0009) exactly.
func (uc *CommitShiftPlan) enrichWithTravelDistance(ctx context.Context, pathPlan *plan.PathPlan, from, to string) {
	if from == "" || to == "" || uc.travelDist == nil {
		return
	}
	view, err := uc.travelDist.GetDistance(ctx, from, to)
	if err != nil || !view.Known {
		return
	}
	pathPlan.SetTravelDistance(view.MetresM, view.Estimated)
}
