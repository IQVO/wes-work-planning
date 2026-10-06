package laborview

// Drift is the outcome of comparing this service's committed PathPlan against
// Workforce's LaborPlanObserved for one path (ADR-0019). It is a plain value:
// the comparison reports a fact and never decides which side is right.
type Drift struct {
	WesPlannedHeads      int
	ObservedPlannedHeads int
	// Heads is signed: ObservedPlannedHeads - WesPlannedHeads.
	Heads int
}

// Detected reports whether the two plans disagree. An agreeing pair is not a
// drift and must raise no event.
func (d Drift) Detected() bool { return d.Heads != 0 }

// CompareHeads is THE comparison both triggers (CommitShiftPlan and
// ObserveLaborPlan) call, so whichever side commits second can never compute
// a different answer for the same pair of facts. It is pure.
func CompareHeads(wesPlannedHeads int, observed LaborPlanObserved) Drift {
	return Drift{
		WesPlannedHeads:      wesPlannedHeads,
		ObservedPlannedHeads: observed.PlannedHeads,
		Heads:                observed.PlannedHeads - wesPlannedHeads,
	}
}
