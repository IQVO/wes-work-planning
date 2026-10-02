package usecases

import (
	"context"
	"errors"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// defaultWIPLimit and defaultAlarmThreshold seed a path's WorkPool the first
// time work is enqueued against it, if no pool has been provisioned yet.
const (
	defaultWIPLimit       = 1000
	defaultAlarmThreshold = 1000
)

// EnqueueWorkUnit adds a releasable unit of work to a path's work pool.
type EnqueueWorkUnit struct {
	workUnits ports.WorkUnitRepo
	pools     ports.WorkPoolRepo
	publisher ports.EventPublisher
	clock     ports.Clock
	uow       ports.UnitOfWork
}

func NewEnqueueWorkUnit(workUnits ports.WorkUnitRepo, pools ports.WorkPoolRepo, publisher ports.EventPublisher, clock ports.Clock) *EnqueueWorkUnit {
	return &EnqueueWorkUnit{workUnits: workUnits, pools: pools, publisher: publisher, clock: clock}
}

// WithUnitOfWork brackets both Saves + Publish in one atomic scope
// (ADR-0014). Optional: nil keeps the calls running back to back.
func (uc *EnqueueWorkUnit) WithUnitOfWork(u ports.UnitOfWork) *EnqueueWorkUnit {
	uc.uow = u
	return uc
}

type EnqueueWorkUnitRequest struct {
	WorkUnitId string
	PathId     shared.PathId
	CPT        shared.CPT
	Reference  string
	// SKU is optional: the inventory SKU this order line corresponds to,
	// if known. Threaded through to the WorkUnit so the outbound
	// WorkReleased publisher can look up its ProductClassification once,
	// at release time, and stamp derived hazmat/fragile hints onto the
	// published event (see ADR-0009). Empty is a valid value — not every
	// caller knows a SKU.
	SKU string
	// GiftWrap is optional: whether the requester asked the warehouse to
	// produce a gift package for this work unit, stated at enqueue time.
	// Threaded through to the WorkUnit so the outbound WorkReleased
	// publisher can stamp it directly onto the published event's data
	// payload (see ADR-0010) — unlike SKU, this is never looked up from
	// another service; it is exactly what the caller supplied. False by
	// default — not every caller requests gift wrap.
	GiftWrap bool
}

func (uc *EnqueueWorkUnit) Execute(ctx context.Context, req EnqueueWorkUnitRequest) (*workunit.WorkUnit, error) {
	var unit *workunit.WorkUnit
	err := retryOnPoolConflict(ctx, func(ctx context.Context) error {
		var err error
		unit, err = uc.enqueueOnce(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return unit, nil
}

// enqueueOnce is one optimistic attempt: load (or create) the pool, add
// the entry, and save the POOL FIRST so a lost race fails before anything
// else is written -- the retry then starts clean even without a
// transactional UnitOfWork.
func (uc *EnqueueWorkUnit) enqueueOnce(ctx context.Context, req EnqueueWorkUnitRequest) (*workunit.WorkUnit, error) {
	unit, err := workunit.NewWorkUnit(req.WorkUnitId, req.PathId, req.CPT, req.Reference)
	if err != nil {
		return nil, err
	}
	unit.SetSKU(req.SKU)
	unit.SetGiftWrap(req.GiftWrap)

	pool, err := uc.pools.FindByPathId(ctx, req.PathId)
	if errors.Is(err, ports.ErrNotFound) {
		pool = release.NewWorkPool(req.PathId, release.ReleaseFed, defaultWIPLimit, defaultAlarmThreshold)
	} else if err != nil {
		return nil, err
	}
	if err := pool.Enqueue(unit.Id(), unit.CPT()); err != nil {
		return nil, err
	}

	event := shared.NewWorkUnitCreated(unit.Id(), req.PathId, uc.clock.Now())
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
