package usecases

import (
	"context"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ReleaseNextWork applies the release policy to a path's work pool,
// admitting the next highest-priority (earliest CPT) pending work unit.
type ReleaseNextWork struct {
	pools     ports.WorkPoolRepo
	workUnits ports.WorkUnitRepo
	publisher ports.EventPublisher
	clock     ports.Clock
	policy    release.ReleasePolicy
	metrics   ports.ReleaseMetrics
	uow       ports.UnitOfWork
}

func NewReleaseNextWork(pools ports.WorkPoolRepo, workUnits ports.WorkUnitRepo, publisher ports.EventPublisher, clock ports.Clock) *ReleaseNextWork {
	return &ReleaseNextWork{
		pools:     pools,
		workUnits: workUnits,
		publisher: publisher,
		clock:     clock,
		policy:    release.NewReleasePolicy(),
	}
}

type ReleaseNextWorkRequest struct {
	PathId shared.PathId
}

// WithMetrics attaches the business-metric port (ADR-0013 Tier 2). Optional:
// nil records nothing.
func (uc *ReleaseNextWork) WithMetrics(m ports.ReleaseMetrics) *ReleaseNextWork {
	uc.metrics = m
	return uc
}

// WithUnitOfWork brackets both Saves + Publish in one atomic scope
// (ADR-0014). Optional: nil keeps the calls running back to back.
func (uc *ReleaseNextWork) WithUnitOfWork(u ports.UnitOfWork) *ReleaseNextWork {
	uc.uow = u
	return uc
}

// Execute releases the next pending work unit of the path. The pool is
// loaded, advanced and saved inside retryOnPoolConflict, so two concurrent
// releases (or a release racing an enqueue/completion) can never write a
// stale pool over a newer one.
func (uc *ReleaseNextWork) Execute(ctx context.Context, req ReleaseNextWorkRequest) (*workunit.WorkUnit, error) {
	var unit *workunit.WorkUnit
	err := retryOnPoolConflict(ctx, func(ctx context.Context) error {
		var err error
		unit, err = uc.releaseOnce(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}

	// Recorded here rather than in the HTTP handler so the metric tracks the
	// real domain event — a work unit actually released — not the request.
	if uc.metrics != nil {
		uc.metrics.WorkUnitReleased(ctx, req.PathId)
	}
	return unit, nil
}

// releaseOnce is one optimistic attempt. A pool entry still "pending" whose
// work unit has already moved on (left behind by a lost update before this
// pool was versioned) is healed in the same save and skipped, rather than
// blocking every future release with 409 work-unit-already-released.
func (uc *ReleaseNextWork) releaseOnce(ctx context.Context, req ReleaseNextWorkRequest) (*workunit.WorkUnit, error) {
	pool, err := uc.pools.FindByPathId(ctx, req.PathId)
	if err != nil {
		return nil, err
	}
	unit, err := uc.nextReleasableUnit(ctx, pool)
	if err != nil {
		return nil, err
	}
	now := uc.clock.Now()
	if err := unit.Release(now); err != nil {
		return nil, err
	}
	// The WorkUnit Save precedes Publish INSIDE the scope on purpose: the
	// integration publisher enriches WorkReleased by reading the work unit
	// back, and under the outbox that read must see this transaction's row.
	event := shared.NewWorkReleased(unit.Id(), req.PathId, now)
	err = atomically(ctx, uc.uow, func(ctx context.Context) error {
		if err := uc.pools.Save(ctx, pool); err != nil {
			return err
		}
		if err := uc.workUnits.Save(ctx, unit); err != nil {
			return err
		}
		return uc.publisher.Publish(ctx, event)
	})
	if err != nil {
		return nil, err
	}
	return unit, nil
}

// nextReleasableUnit advances the pool to its next pending entry whose
// work unit is still Pending. A pending entry whose unit already moved on
// (left behind by a lost update before pools were versioned) is healed in
// place and skipped; the healed entries are persisted by the same save as
// the release itself.
func (uc *ReleaseNextWork) nextReleasableUnit(ctx context.Context, pool *release.WorkPool) (*workunit.WorkUnit, error) {
	for {
		workUnitId, err := uc.policy.Apply(pool)
		if err != nil {
			return nil, err
		}
		unit, err := uc.workUnits.FindById(ctx, workUnitId)
		if err != nil {
			return nil, err
		}
		if unit.State() == workunit.Pending {
			return unit, nil
		}
		if err := pool.Reconcile(workUnitId, true, unit.State() == workunit.Completed); err != nil {
			return nil, err
		}
	}
}
