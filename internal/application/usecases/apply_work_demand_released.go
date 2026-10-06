package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ApplyWorkDemandReleased is the inbound-event use case behind
// network-inventory-planning's WorkDemandReleased: one released transfer
// demand becomes one WorkUnit, enqueued through the existing
// EnqueueWorkUnit exactly like ApplyOrderAllocated enqueues an order line
// (ADR-0033). It sits beside ApplyOrderAllocated, not inside it — a
// demand is a different upstream fact with a different payload, but the
// same enqueue path, the same catalogue validation and the same
// idempotency contract.
//
// The WorkUnitId is the demand_id itself (deterministic), so:
//
//   - a redelivery of the same CloudEvents id is caught by the
//     processed-event mark (onceAtomically, ADR-0028);
//   - a DIFFERENT CloudEvents id carrying the same demand_id hits
//     release.ErrDuplicateEntry and is a benign no-op — by construction
//     the same logical work unit, never a reason to stall the partition.
type ApplyWorkDemandReleased struct {
	enqueueWorkUnit *EnqueueWorkUnit
	processed       ports.ProcessedEventRepo
	catalogue       ports.PathCatalogue
	uow             ports.UnitOfWork
}

func NewApplyWorkDemandReleased(enqueueWorkUnit *EnqueueWorkUnit, processed ports.ProcessedEventRepo, catalogue ports.PathCatalogue) *ApplyWorkDemandReleased {
	return &ApplyWorkDemandReleased{enqueueWorkUnit: enqueueWorkUnit, processed: processed, catalogue: catalogue}
}

// WithUnitOfWork brackets the processed-event mark and the enqueue in one
// atomic scope (the EnqueueWorkUnit scope joins it).
func (uc *ApplyWorkDemandReleased) WithUnitOfWork(u ports.UnitOfWork) *ApplyWorkDemandReleased {
	uc.uow = u
	return uc
}

type ApplyWorkDemandReleasedRequest struct {
	EventId     string
	OccurredAt  time.Time
	DemandId    string
	WorkKind    workunit.WorkKind
	TransferRef string
	PathId      string
	SiteId      string
	SKU         string
	Quantity    int
	CPT         time.Time
}

// Execute enqueues one WorkUnit under the deterministic id demand_id,
// keeping reference=demand_id as the generic operator ref. The path_id is
// validated against the declared process-path catalogue and the work_kind
// against the declared transfer legs BEFORE anything is enqueued: an
// unrecognized value must fail loud (and end in the DLQ) rather than seed
// a WorkPool nothing downstream will ever service — the same fail-loud
// contract ApplyOrderAllocated inherited from fulfillment-execution's
// ADR-0017.
func (uc *ApplyWorkDemandReleased) Execute(ctx context.Context, req ApplyWorkDemandReleasedRequest) (alreadyProcessed bool, err error) {
	return onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.OccurredAt, func(ctx context.Context) error {
		enqueue, err := uc.enqueueRequest(req)
		if err != nil {
			return err
		}
		_, err = uc.enqueueWorkUnit.Execute(ctx, enqueue)
		// release.ErrDuplicateEntry: this deterministic WorkUnitId is
		// already in the pool, so it is by construction the SAME logical
		// work unit (a replay of the same demand under a new event id). A
		// benign no-op, never a reason to stall the partition — identical
		// to ApplyOrderAllocated's per-line duplicate handling.
		if errors.Is(err, release.ErrDuplicateEntry) {
			return nil
		}
		return err
	})
}

func (uc *ApplyWorkDemandReleased) enqueueRequest(req ApplyWorkDemandReleasedRequest) (EnqueueWorkUnitRequest, error) {
	if _, err := workunit.ParseWorkKind(req.WorkKind.String()); err != nil {
		return EnqueueWorkUnitRequest{}, err
	}
	pathId, err := shared.NewPathId(req.PathId)
	if err != nil {
		return EnqueueWorkUnitRequest{}, err
	}
	if _, err := uc.catalogue.Lookup(pathId.String()); err != nil {
		return EnqueueWorkUnitRequest{}, err
	}
	return EnqueueWorkUnitRequest{
		WorkUnitId:  req.DemandId,
		PathId:      pathId,
		CPT:         shared.NewCPT(req.CPT),
		Reference:   req.DemandId,
		SKU:         req.SKU,
		TransferRef: req.TransferRef,
		WorkKind:    req.WorkKind,
		SiteId:      req.SiteId,
		Quantity:    req.Quantity,
	}, nil
}
