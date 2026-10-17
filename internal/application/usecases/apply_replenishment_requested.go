package usecases

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ReplenishPathId is the process path every pick-face replenishment is
// released on (ADR-0042): fulfillment-execution derives its REPLENISH task
// type from it, and only accepts the `replenishment` object there.
const ReplenishPathId = "REPLENISH"

// Default CPT horizons of REPLENISH work (ADR-0042): a request's CPT is its
// requested_at plus the horizon of its priority. URGENT is the front band:
// the shorter horizon sorts it ahead of NORMAL in every earliest-CPT-first
// queue (this context's pool and fulfillment-execution's claimNext).
const (
	DefaultReplenishmentCPTHorizon       = 8 * time.Hour
	DefaultReplenishmentUrgentCPTHorizon = time.Hour
)

// ErrInvalidReplenishmentRequest marks a ReplenishmentRequested that cannot
// be planned because a field this context needs is missing (site id or
// request time). Deterministic: a retry can never fix it.
var ErrInvalidReplenishmentRequest = errors.New("invalid replenishment request")

// ReplenishmentOutcome reports what a replenishment use case did with one
// event.
type ReplenishmentOutcome int

const (
	// ReplenishmentPlanned: a REPLENISH work unit was planned (or, for a
	// cancellation, a Pending unit was cancelled).
	ReplenishmentPlanned ReplenishmentOutcome = iota
	// ReplenishmentAlreadyProcessed: a redelivery of the same CloudEvents id.
	ReplenishmentAlreadyProcessed
	// ReplenishmentAlreadyPlanned: a different event for a request id whose
	// work unit already exists. Nothing was written.
	ReplenishmentAlreadyPlanned
	// ReplenishmentUnknownRequest: a cancellation for a request this context
	// never planned. Nothing was written.
	ReplenishmentUnknownRequest
	// ReplenishmentNotCancellable: a cancellation for a unit that is no
	// longer Pending (released, completed or already cancelled): it runs to
	// completion. Nothing was written.
	ReplenishmentNotCancellable
)

// String names the outcome for logs.
func (o ReplenishmentOutcome) String() string {
	switch o {
	case ReplenishmentPlanned:
		return "planned"
	case ReplenishmentAlreadyProcessed:
		return "already_processed"
	case ReplenishmentAlreadyPlanned:
		return "already_planned"
	case ReplenishmentUnknownRequest:
		return "unknown_request"
	case ReplenishmentNotCancellable:
		return "not_cancellable"
	default:
		return "unknown"
	}
}

// ApplyReplenishmentRequestedRequest is one ReplenishmentRequested
// occurrence, as published by pick-face-replenishment.
type ApplyReplenishmentRequestedRequest struct {
	EventId     string
	OccurredAt  time.Time
	RequestId   string
	SiteId      string
	SKU         string
	FromBin     string // the event's source_bin (reserve)
	ToBin       string // the event's forward_bin (pick face)
	Quantity    int
	Priority    string
	RequestedAt time.Time
}

// ApplyReplenishmentRequested is the inbound-event use case behind
// pick-face-replenishment's ReplenishmentRequested (ADR-0042): ONE REPLENISH
// work unit per request id, pooled on the REPLENISH path, with the
// processed-event mark and every write in ONE atomic scope (ADR-0028).
// Nothing leaves except through the transactional outbox (ADR-0014).
//
// The work unit id is "replenish-<request_id>", so:
//
//   - a redelivery of the same CloudEvents id is caught by the
//     processed-event mark;
//   - a DIFFERENT event for an already-planned request id finds the unit and
//     is a no-op, never a second unit.
type ApplyReplenishmentRequested struct {
	workUnits ports.WorkUnitRepo
	pools     ports.WorkPoolRepo
	processed ports.ProcessedEventRepo
	catalogue ports.PathCatalogue
	publisher ports.EventPublisher
	clock     ports.Clock
	uow       ports.UnitOfWork
	normal    time.Duration
	urgent    time.Duration
}

func NewApplyReplenishmentRequested(workUnits ports.WorkUnitRepo, pools ports.WorkPoolRepo, processed ports.ProcessedEventRepo, catalogue ports.PathCatalogue, publisher ports.EventPublisher, clock ports.Clock) *ApplyReplenishmentRequested {
	return &ApplyReplenishmentRequested{
		workUnits: workUnits,
		pools:     pools,
		processed: processed,
		catalogue: catalogue,
		publisher: publisher,
		clock:     clock,
		normal:    DefaultReplenishmentCPTHorizon,
		urgent:    DefaultReplenishmentUrgentCPTHorizon,
	}
}

// WithUnitOfWork brackets the processed-event mark and every write in one
// atomic scope (ADR-0014).
func (uc *ApplyReplenishmentRequested) WithUnitOfWork(u ports.UnitOfWork) *ApplyReplenishmentRequested {
	uc.uow = u
	return uc
}

// WithCPTHorizons sets how far after requested_at a NORMAL and an URGENT
// request's CPT lies (REPLENISHMENT_CPT_HORIZON,
// REPLENISHMENT_URGENT_CPT_HORIZON). A non-positive value keeps the current
// one; an urgent horizon above the normal one is capped at it, so URGENT can
// never sort behind NORMAL.
func (uc *ApplyReplenishmentRequested) WithCPTHorizons(normal, urgent time.Duration) *ApplyReplenishmentRequested {
	if normal > 0 {
		uc.normal = normal
	}
	if urgent > 0 {
		uc.urgent = urgent
	}
	uc.urgent = min(uc.urgent, uc.normal)
	return uc
}

// Execute plans one request. A broken request, an unknown REPLENISH path or
// a broken contract is returned as an error with nothing committed (the
// consumer retries or dead-letters it).
func (uc *ApplyReplenishmentRequested) Execute(ctx context.Context, req ApplyReplenishmentRequestedRequest) (ReplenishmentOutcome, error) {
	outcome := ReplenishmentPlanned
	already, err := onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.OccurredAt, func(ctx context.Context) error {
		move, err := replenishmentMoveOf(req)
		if err != nil {
			return err
		}
		pathId, err := uc.replenishPath()
		if err != nil {
			return err
		}
		return retryOnPoolConflict(ctx, func(ctx context.Context) error {
			var err error
			outcome, err = uc.planOnce(ctx, req, move, pathId)
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

// replenishmentMoveOf validates the request into the domain move and checks
// the two fields only this context needs.
func replenishmentMoveOf(req ApplyReplenishmentRequestedRequest) (workunit.ReplenishmentMove, error) {
	move, err := workunit.NewReplenishmentMove(req.RequestId, req.SKU, req.FromBin, req.ToBin, req.Quantity, req.Priority)
	if err != nil {
		return workunit.ReplenishmentMove{}, err
	}
	if strings.TrimSpace(req.SiteId) == "" {
		return workunit.ReplenishmentMove{}, fmt.Errorf("%w: site_id is required", ErrInvalidReplenishmentRequest)
	}
	if req.RequestedAt.IsZero() {
		return workunit.ReplenishmentMove{}, fmt.Errorf("%w: requested_at is required", ErrInvalidReplenishmentRequest)
	}
	return move, nil
}

// replenishPath validates REPLENISH against the catalogue (ADR-0012). The
// catalogue declares no replenishment intake value (process-path-management
// defines CUSTOMER_ORDER, STOCK_TRANSFER, SLOT_RELOCATION and
// INVENTORY_CONTROL), so no intake is evaluated: like every path reached by
// explicit id, REPLENISH is accepted whatever it declares.
func (uc *ApplyReplenishmentRequested) replenishPath() (shared.PathId, error) {
	if _, err := uc.catalogue.Lookup(ReplenishPathId); err != nil {
		return shared.PathId{}, err
	}
	return shared.NewPathId(ReplenishPathId)
}

// cptFor is the CPT of a request: requested_at plus the horizon of its
// priority (URGENT: the shorter one).
func (uc *ApplyReplenishmentRequested) cptFor(requestedAt time.Time, priority workunit.ReplenishmentPriority) shared.CPT {
	if priority == workunit.ReplenishmentPriorityUrgent {
		return shared.NewCPT(requestedAt.Add(uc.urgent))
	}
	return shared.NewCPT(requestedAt.Add(uc.normal))
}

// planOnce is one optimistic attempt: a request whose unit exists is a
// no-op, otherwise the unit is pooled and saved. The pool is saved FIRST so
// a lost race fails before anything else is written.
func (uc *ApplyReplenishmentRequested) planOnce(ctx context.Context, req ApplyReplenishmentRequestedRequest, move workunit.ReplenishmentMove, pathId shared.PathId) (ReplenishmentOutcome, error) {
	unitId := workunit.ReplenishmentWorkUnitId(move.RequestId)
	_, err := uc.workUnits.FindById(ctx, unitId)
	if err == nil {
		return ReplenishmentAlreadyPlanned, nil
	}
	if !errors.Is(err, ports.ErrNotFound) {
		return ReplenishmentPlanned, err
	}
	cpt := uc.cptFor(req.RequestedAt, move.Priority)
	unit, err := workunit.NewWorkUnit(unitId, pathId, cpt, move.RequestId)
	if err != nil {
		return ReplenishmentPlanned, err
	}
	unit.SetWorkKind(workunit.WorkKindReplenish)
	unit.SetSiteId(strings.TrimSpace(req.SiteId))
	unit.SetReplenishment(move)

	pool, err := loadOrNewPool(ctx, uc.pools, pathId)
	if err != nil {
		return ReplenishmentPlanned, err
	}
	if err := pool.Enqueue(unit.Id(), cpt); err != nil {
		if errors.Is(err, release.ErrDuplicateEntry) {
			return ReplenishmentAlreadyPlanned, nil
		}
		return ReplenishmentPlanned, err
	}
	event := shared.NewWorkUnitCreated(unit.Id(), pathId, uc.clock.Now()).WithOrigin(unit.Reference(), unit.LineNo())
	return ReplenishmentPlanned, atomically(ctx, uc.uow, func(ctx context.Context) error {
		if err := uc.pools.Save(ctx, pool); err != nil {
			return err
		}
		if err := uc.workUnits.Save(ctx, unit); err != nil {
			return err
		}
		return uc.publisher.Publish(ctx, event)
	})
}
