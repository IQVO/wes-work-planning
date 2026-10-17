package usecases

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ApplyReplenishmentCancelledRequest is one ReplenishmentCancelled
// occurrence, as published by pick-face-replenishment.
type ApplyReplenishmentCancelledRequest struct {
	EventId    string
	OccurredAt time.Time
	RequestId  string
	// Reason is OPERATOR, ASSIGNMENT_CHANGED or POLICY_REMOVED; it is not
	// interpreted here, only carried for logs.
	Reason string
}

// ApplyReplenishmentCancelled is the inbound-event use case behind
// pick-face-replenishment's ReplenishmentCancelled (ADR-0042). It does for
// a cancelled request exactly what ADR-0040 does for a superseded RELOCATE
// move: a unit still Pending is cancelled and its pool entry withdrawn;
// released or completed work runs to completion. No event is published (a
// Pending unit was never released, so nothing downstream holds work for it).
type ApplyReplenishmentCancelled struct {
	workUnits ports.WorkUnitRepo
	pools     ports.WorkPoolRepo
	processed ports.ProcessedEventRepo
	clock     ports.Clock
	uow       ports.UnitOfWork
}

func NewApplyReplenishmentCancelled(workUnits ports.WorkUnitRepo, pools ports.WorkPoolRepo, processed ports.ProcessedEventRepo, clock ports.Clock) *ApplyReplenishmentCancelled {
	return &ApplyReplenishmentCancelled{workUnits: workUnits, pools: pools, processed: processed, clock: clock}
}

// WithUnitOfWork brackets the processed-event mark and every write in one
// atomic scope (ADR-0014).
func (uc *ApplyReplenishmentCancelled) WithUnitOfWork(u ports.UnitOfWork) *ApplyReplenishmentCancelled {
	uc.uow = u
	return uc
}

// Execute applies one cancellation. A blank request id is returned as an
// error with nothing committed (deterministic poison).
func (uc *ApplyReplenishmentCancelled) Execute(ctx context.Context, req ApplyReplenishmentCancelledRequest) (ReplenishmentOutcome, error) {
	outcome := ReplenishmentPlanned
	already, err := onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.OccurredAt, func(ctx context.Context) error {
		requestId := strings.TrimSpace(req.RequestId)
		if requestId == "" {
			return workunit.ErrInvalidReplenishmentMove
		}
		return retryOnPoolConflict(ctx, func(ctx context.Context) error {
			var err error
			outcome, err = uc.cancelOnce(ctx, workunit.ReplenishmentWorkUnitId(requestId))
			return err
		})
	})
	if err != nil {
		return ReplenishmentPlanned, err
	}
	if already {
		return ReplenishmentAlreadyProcessed, nil
	}
	return outcome, nil
}

// cancelOnce is one optimistic attempt. The pool is saved FIRST so a lost
// race fails before the unit is written.
func (uc *ApplyReplenishmentCancelled) cancelOnce(ctx context.Context, unitId string) (ReplenishmentOutcome, error) {
	unit, err := uc.workUnits.FindById(ctx, unitId)
	if errors.Is(err, ports.ErrNotFound) {
		return ReplenishmentUnknownRequest, nil
	}
	if err != nil {
		return ReplenishmentPlanned, err
	}
	if unit.State() != workunit.Pending {
		return ReplenishmentNotCancellable, nil
	}
	if err := unit.Cancel(uc.clock.Now()); err != nil {
		return ReplenishmentPlanned, err
	}
	return ReplenishmentPlanned, atomically(ctx, uc.uow, func(ctx context.Context) error {
		if err := uc.withdraw(ctx, unit); err != nil {
			return err
		}
		return uc.workUnits.Save(ctx, unit)
	})
}

// withdraw removes the unit's pool entry. A path with no pool, or a pool
// that never held the unit, is not an error.
func (uc *ApplyReplenishmentCancelled) withdraw(ctx context.Context, unit *workunit.WorkUnit) error {
	pool, err := uc.pools.FindByPathId(ctx, unit.PathId())
	if errors.Is(err, ports.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := pool.Withdraw(unit.Id()); err != nil {
		if errors.Is(err, release.ErrUnknownEntry) {
			return nil
		}
		return err
	}
	return uc.pools.Save(ctx, pool)
}
