package memory

import (
	"context"
	"sync"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/relocation"
)

// SlotPlanRepo is the in-memory ports.SlotPlanRepo (ADR-0040).
type SlotPlanRepo struct {
	mu     sync.RWMutex
	byPlan map[string]relocation.Receipt
}

func NewSlotPlanRepo() *SlotPlanRepo {
	return &SlotPlanRepo{byPlan: make(map[string]relocation.Receipt)}
}

// Save records a plan; the same plan id again is a no-op.
func (r *SlotPlanRepo) Save(ctx context.Context, receipt relocation.Receipt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byPlan[receipt.PlanId]; !ok {
		r.byPlan[receipt.PlanId] = receipt
	}
	return nil
}

// LatestAtSite returns the site's latest-approved plan (ties: greater plan
// id), or ports.ErrNotFound.
func (r *SlotPlanRepo) LatestAtSite(ctx context.Context, siteId string) (relocation.Receipt, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var latest *relocation.Receipt
	for _, rec := range r.byPlan {
		if rec.SiteId != siteId {
			continue
		}
		if latest == nil || rec.Supersedes(*latest) {
			cp := rec
			latest = &cp
		}
	}
	if latest == nil {
		return relocation.Receipt{}, ports.ErrNotFound
	}
	return *latest, nil
}
