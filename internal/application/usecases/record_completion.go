package usecases

import (
	"context"
	"errors"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// RecordCompletion finishes a released work unit and publishes
// WorkUnitCompleted so telemetry projections can update. It also frees the
// unit's WIP slot on its release-fed work pool (WorkPool.Complete) — the
// other half of the release/complete cycle. Without this, a release-fed
// pool's WIP count only ever rises: ReleaseNextWork marks an entry
// "released" and it never leaves that state, so once wipLimit entries have
// EVER been released the pool is permanently wedged shut regardless of how
// much of that work has since finished downstream (found via the e2e
// soak_backlog_ramp scenario: a 1h sustained run hit this ceiling in ~18
// minutes and every /release call 409'd for the rest of the run even
// though fulfillment-execution kept completing tasks with zero Kafka lag).
type RecordCompletion struct {
	workUnits ports.WorkUnitRepo
	pools     ports.WorkPoolRepo
	publisher ports.EventPublisher
	clock     ports.Clock
	uow       ports.UnitOfWork
}

func NewRecordCompletion(workUnits ports.WorkUnitRepo, pools ports.WorkPoolRepo, publisher ports.EventPublisher, clock ports.Clock) *RecordCompletion {
	return &RecordCompletion{workUnits: workUnits, pools: pools, publisher: publisher, clock: clock}
}

// WithUnitOfWork brackets both Saves + Publish in one atomic scope
// (ADR-0014). Optional: nil keeps the calls running back to back.
func (uc *RecordCompletion) WithUnitOfWork(u ports.UnitOfWork) *RecordCompletion {
	uc.uow = u
	return uc
}

type RecordCompletionRequest struct {
	WorkUnitId string
}

func (uc *RecordCompletion) Execute(ctx context.Context, req RecordCompletionRequest) (*workunit.WorkUnit, error) {
	var unit *workunit.WorkUnit
	err := retryOnPoolConflict(ctx, func(ctx context.Context) error {
		var err error
		unit, err = uc.completeOnce(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return unit, nil
}

// completeOnce is one optimistic attempt. The pool entry is RECONCILED to
// completed (not just Complete()d): an entry a lost update left at
// "pending" used to make pool.Complete fail with ErrNotReleased, which was
// silently ignored -- the entry then stayed pending forever and wedged
// every later release on the path. The pool is saved first so a lost race
// fails before the work unit is written.
func (uc *RecordCompletion) completeOnce(ctx context.Context, req RecordCompletionRequest) (*workunit.WorkUnit, error) {
	unit, err := uc.workUnits.FindById(ctx, req.WorkUnitId)
	if err != nil {
		return nil, err
	}
	now := uc.clock.Now()
	if err := unit.Complete(now); err != nil {
		return nil, err
	}
	event := shared.NewWorkUnitCompleted(unit.Id(), unit.PathId(), now)
	err = atomically(ctx, uc.uow, func(ctx context.Context) error {
		if err := uc.completePoolEntry(ctx, unit); err != nil {
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

// completePoolEntry frees the unit's WIP slot. A path with no pool, or a
// pool that never held this unit, is not an error (best-effort secondary
// aggregate, as before); a lost optimistic race is, so it gets retried.
func (uc *RecordCompletion) completePoolEntry(ctx context.Context, unit *workunit.WorkUnit) error {
	pool, err := uc.pools.FindByPathId(ctx, unit.PathId())
	if errors.Is(err, ports.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := pool.Reconcile(unit.Id(), true, true); err != nil {
		return nil // ErrUnknownEntry: this pool never held the unit
	}
	return uc.pools.Save(ctx, pool)
}
