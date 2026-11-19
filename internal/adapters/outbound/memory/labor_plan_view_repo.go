package memory

import (
	"context"
	"sync"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// LaborPlanViewRepo is a thread-safe in-memory ports.LaborPlanViewRepo.
type LaborPlanViewRepo struct {
	mu     sync.RWMutex
	byPath map[string]laborview.LaborPlanObserved
}

func NewLaborPlanViewRepo() *LaborPlanViewRepo {
	return &LaborPlanViewRepo{byPath: make(map[string]laborview.LaborPlanObserved)}
}

func (r *LaborPlanViewRepo) Save(ctx context.Context, view laborview.LaborPlanObserved) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byPath[view.PathId.String()] = view
	return nil
}

func (r *LaborPlanViewRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (laborview.LaborPlanObserved, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.byPath[pathId.String()]
	if !ok {
		return laborview.LaborPlanObserved{}, ports.ErrNotFound
	}
	return v, nil
}
