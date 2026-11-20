package workunit

import (
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// Snapshot is the persisted shape of a WorkUnit: everything Rehydrate
// needs. It exists so the Postgres and memory repositories can rebuild the
// aggregate without poking at unexported fields (the fleet pattern, see
// network-inventory-planning's transfer saga snapshot).
type Snapshot struct {
	ID          string
	PathId      shared.PathId
	CPT         shared.CPT
	Reference   string
	SKU         string
	GiftWrap    bool
	TransferRef string
	WorkKind    WorkKind
	SiteId      string
	Quantity    int
	LineNo      int
	Relocation  *RelocationMove
	Replenish   *ReplenishmentMove
	Pick        *PickPlan
	CaseBreak   *CaseBreak
	BlockedBy   string
	State       State
	ReleasedAt  *time.Time
	CompletedAt *time.Time
	CancelledAt *time.Time
}

// Rehydrate rebuilds an aggregate from its persisted snapshot. It trusts
// the database (the enum columns are validated by the repository on the way
// in) and performs NO transitions: the lifecycle state and its timestamps
// come back exactly as persisted.
func Rehydrate(s Snapshot) *WorkUnit {
	w := &WorkUnit{
		id:          s.ID,
		pathId:      s.PathId,
		cpt:         s.CPT,
		reference:   s.Reference,
		sku:         s.SKU,
		giftWrap:    s.GiftWrap,
		transferRef: s.TransferRef,
		workKind:    s.WorkKind,
		siteId:      s.SiteId,
		quantity:    s.Quantity,
		lineNo:      s.LineNo,
		blockedBy:   s.BlockedBy,
		state:       s.State,
		releasedAt:  s.ReleasedAt,
		completedAt: s.CompletedAt,
		cancelledAt: s.CancelledAt,
	}
	if s.Relocation != nil {
		move := *s.Relocation
		w.relocation = &move
	}
	if s.Replenish != nil {
		move := *s.Replenish
		w.replenish = &move
	}
	if s.Pick != nil {
		plan := *s.Pick
		w.pick = &plan
	}
	if s.CaseBreak != nil {
		brk := *s.CaseBreak
		w.caseBreak = &brk
	}
	return w
}
