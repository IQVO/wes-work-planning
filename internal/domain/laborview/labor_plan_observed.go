// Package laborview holds the read-only projection of labor plans observed
// from Workforce Management's ShiftPlanCommitted events. Since ADR-0049 this
// context holds no shift plan of its own: planning is warehouse-planning's.
package laborview

import (
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// LaborPlanObserved is the latest labor plan observed for a path, as
// reported by Workforce Management. Plain read-model value, not an
// aggregate with invariants.
//
// DriftHeads/DriftDetectedAt are no longer computed (ADR-0049: the ADR-0019
// reconciliation moved to warehouse-planning with the shift plan). They only
// carry a drift recorded before that, read back from the kept
// labor_plan_view columns until the path's next observed plan clears them,
// so GET /paths/{pathId}/labor-plan-view answers byte-identically for one
// release; stage E removes them. DriftHeads is signed (observed - ours);
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
