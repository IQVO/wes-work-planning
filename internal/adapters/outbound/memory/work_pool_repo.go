package memory

import (
	"context"
	"sync"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

type WorkPoolRepo struct {
	mu     sync.RWMutex
	byPath map[string]*release.WorkPool
}

func NewWorkPoolRepo() *WorkPoolRepo {
	return &WorkPoolRepo{byPath: make(map[string]*release.WorkPool)}
}

// Save applies the same optimistic-concurrency rule as the Postgres
// adapter: it only succeeds against the version the pool was loaded at,
// and stores a copy so a caller's later in-memory mutations can never leak
// into (or race with) the stored aggregate.
func (r *WorkPoolRepo) Save(ctx context.Context, pool *release.WorkPool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := pool.PathId().String()
	var stored int64
	if cur, ok := r.byPath[key]; ok {
		stored = cur.Version()
	}
	if pool.Version() != stored {
		return ports.ErrConcurrentModification
	}
	pool.SetVersion(stored + 1)
	r.byPath[key] = clonePool(pool)
	return nil
}

func (r *WorkPoolRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (*release.WorkPool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.byPath[pathId.String()]
	if !ok {
		return nil, ports.ErrNotFound
	}
	return clonePool(p), nil
}

// clonePool returns an independent copy of p, the way every Postgres read
// rehydrates a fresh aggregate.
func clonePool(p *release.WorkPool) *release.WorkPool {
	cp := release.NewWorkPool(p.PathId(), p.Mode(), p.WIPLimit(), p.AlarmThreshold())
	for _, e := range p.Entries() {
		_ = cp.RestoreEntry(e.WorkUnitId, e.CPT, e.Released, e.Completed)
	}
	cp.SetVersion(p.Version())
	return cp
}
