// Package laborview holds the read-only projection of labor plans observed
// from Workforce Management's ShiftPlanCommitted events. This is NOT the
// same model as this service's own plan.ShiftPlan aggregate — same term,
// different bounded context.
package laborview

import (
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// LaborPlanObserved is the latest labor plan observed for a path, as
// reported by Workforce Management. Plain read-model value, not an
// aggregate with invariants.
//
// DriftHeads/DriftDetectedAt carry the outcome of the ADR-0019
// reconciliation against this service's own committed PathPlan. Both are nil
// until a comparison has actually been computed for the path (no committed
// PathPlan yet means nothing to compare — never a fabricated drift).
// DriftHeads is signed (observed - ours) and may be 0 (the plans agree);
// DriftDetectedAt is set only when DriftHeads != 0.
type LaborPlanObserved struct {
	PathId          shared.PathId
	PlannedHeads    int
	PlannedRate     float64
	PlannedHours    float64
	ObservedAt      time.Time
	DriftHeads      *int
	DriftDetectedAt *time.Time
}
