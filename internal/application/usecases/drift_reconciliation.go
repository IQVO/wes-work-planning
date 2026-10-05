package usecases

import (
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// reconcileHeads is the ONE comparison both ADR-0019 triggers share
// (CommitShiftPlan, when our plan commits; ObserveLaborPlan, when Workforce's
// does): it compares wesPlannedHeads against the observed view with
// laborview.CompareHeads, returns the view stamped with the outcome, and — only
// when the plans disagree — the PathPlanDriftDetected event to publish.
//
// DriftHeads is always recorded once a comparison has been computed (0 = the
// plans agree); DriftDetectedAt only when they disagree. An agreeing pair
// raises no event, mirroring RebalanceDecision's NoActionNeeded.
func reconcileHeads(view laborview.LaborPlanObserved, wesPlannedHeads int, now time.Time) (laborview.LaborPlanObserved, *shared.PathPlanDriftDetected) {
	drift := laborview.CompareHeads(wesPlannedHeads, view)
	heads := drift.Heads
	view.DriftHeads = &heads
	view.DriftDetectedAt = nil
	if !drift.Detected() {
		return view, nil
	}
	detectedAt := now
	view.DriftDetectedAt = &detectedAt
	ev := shared.NewPathPlanDriftDetected(view.PathId, drift.WesPlannedHeads, drift.ObservedPlannedHeads, view.ObservedAt, now)
	return view, &ev
}
