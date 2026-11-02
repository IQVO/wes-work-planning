package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

type WorkUnitRepo struct {
	mu   sync.RWMutex
	byId map[string]*workunit.WorkUnit
}

func NewWorkUnitRepo() *WorkUnitRepo {
	return &WorkUnitRepo{byId: make(map[string]*workunit.WorkUnit)}
}

func (r *WorkUnitRepo) Save(ctx context.Context, unit *workunit.WorkUnit) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byId[unit.Id()] = cloneWorkUnit(unit)
	return nil
}

func (r *WorkUnitRepo) FindById(ctx context.Context, id string) (*workunit.WorkUnit, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.byId[id]
	if !ok {
		return nil, ports.ErrNotFound
	}
	return cloneWorkUnit(u), nil
}

func (r *WorkUnitRepo) FindByPathId(ctx context.Context, pathId shared.PathId) ([]*workunit.WorkUnit, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*workunit.WorkUnit
	for _, u := range r.byId {
		if u.PathId().Equals(pathId) {
			out = append(out, cloneWorkUnit(u))
		}
	}
	return out, nil
}

// FindByReference returns every WorkUnit carrying the given external
// reference. A reference can plausibly match more than one WorkUnit across
// retries/history, so this returns a slice; an empty slice (not an error)
// when nothing matches.
func (r *WorkUnitRepo) FindByReference(ctx context.Context, reference string) ([]*workunit.WorkUnit, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*workunit.WorkUnit
	for _, u := range r.byId {
		if u.Reference() == reference {
			out = append(out, cloneWorkUnit(u))
		}
	}
	return out, nil
}

// FindByReferencePage is the keyset-paged FindByReference: ordered by
// (cpt, id), strictly after `after`, at most `limit` rows (limit <= 0 means
// unbounded).
func (r *WorkUnitRepo) FindByReferencePage(ctx context.Context, reference string, after *workunit.Cursor, limit int) ([]*workunit.WorkUnit, error) {
	all, err := r.FindByReference(ctx, reference)
	if err != nil {
		return nil, err
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].CPT().Time().Equal(all[j].CPT().Time()) {
			return all[i].CPT().Time().Before(all[j].CPT().Time())
		}
		return all[i].Id() < all[j].Id()
	})
	var out []*workunit.WorkUnit
	for _, u := range all {
		if after != nil && !keysetAfter(u, after) {
			continue
		}
		out = append(out, u)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}

// PendingRelocationsAtSite returns every Pending RELOCATE work unit of
// siteId (ADR-0040).
func (r *WorkUnitRepo) PendingRelocationsAtSite(ctx context.Context, siteId string) ([]*workunit.WorkUnit, error) {
	return r.filter(func(u *workunit.WorkUnit) bool {
		return u.WorkKind() == workunit.WorkKindRelocate && u.SiteId() == siteId && u.State() == workunit.Pending
	}), nil
}

// PendingBlockedBy returns every Pending work unit waiting on workUnitId.
func (r *WorkUnitRepo) PendingBlockedBy(ctx context.Context, workUnitId string) ([]*workunit.WorkUnit, error) {
	return r.filter(func(u *workunit.WorkUnit) bool {
		return u.BlockedBy() == workUnitId && u.State() == workunit.Pending
	}), nil
}

func (r *WorkUnitRepo) filter(keep func(*workunit.WorkUnit) bool) []*workunit.WorkUnit {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []*workunit.WorkUnit{}
	for _, u := range r.byId {
		if keep(u) {
			out = append(out, cloneWorkUnit(u))
		}
	}
	return out
}

// cloneWorkUnit returns an independent copy of u, rebuilt exactly the way
// the Postgres adapter rehydrates a row. Handing out the stored pointer let
// concurrent use cases mutate one aggregate in place -- a data race no
// real database exhibits, which hid lost-update bugs from the tests.
func cloneWorkUnit(u *workunit.WorkUnit) *workunit.WorkUnit {
	cp, err := workunit.NewWorkUnit(u.Id(), u.PathId(), u.CPT(), u.Reference())
	if err != nil {
		return u // unreachable: u was itself built by NewWorkUnit
	}
	cp.SetSKU(u.SKU())
	cp.SetGiftWrap(u.GiftWrap())
	cp.SetTransferRef(u.TransferRef())
	cp.SetWorkKind(u.WorkKind())
	cp.SetSiteId(u.SiteId())
	cp.SetQuantity(u.Quantity())
	cp.SetLineNo(u.LineNo())
	if m, ok := u.Relocation(); ok {
		cp.SetRelocation(m)
	}
	if m, ok := u.Replenishment(); ok {
		cp.SetReplenishment(m)
	}
	if p, ok := u.Pick(); ok {
		cp.SetPick(p)
	}
	if b, ok := u.CaseBreak(); ok {
		cp.SetCaseBreak(b)
	}
	cp.SetBlockedBy(u.BlockedBy())
	if at := u.ReleasedAt(); at != nil {
		_ = cp.Release(*at)
	}
	if at := u.CompletedAt(); at != nil {
		_ = cp.Complete(*at)
	}
	if at := u.CancelledAt(); at != nil {
		_ = cp.Cancel(*at)
	}
	return cp
}

// keysetAfter reports whether u sorts strictly after the cursor in (cpt, id) order.
func keysetAfter(u *workunit.WorkUnit, c *workunit.Cursor) bool {
	cpt := u.CPT().Time()
	return cpt.After(c.CPT) || (cpt.Equal(c.CPT) && u.Id() > c.ID)
}
