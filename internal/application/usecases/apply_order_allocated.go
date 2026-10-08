package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ApplyOrderAllocated is the inbound-event use case behind
// order-management's OrderAllocated / OrderPartiallyAllocated. It feeds
// every allocated line into the existing EnqueueWorkUnit, with the
// processed-event mark and all the enqueues committed atomically
// (ADR-0028): if any line fails, the mark rolls back with it and the
// event is retried rather than silently dropped.
type ApplyOrderAllocated struct {
	enqueueWorkUnit *EnqueueWorkUnit
	processed       ports.ProcessedEventRepo
	catalogue       ports.PathCatalogue
	uow             ports.UnitOfWork
}

func NewApplyOrderAllocated(enqueueWorkUnit *EnqueueWorkUnit, processed ports.ProcessedEventRepo, catalogue ports.PathCatalogue) *ApplyOrderAllocated {
	return &ApplyOrderAllocated{enqueueWorkUnit: enqueueWorkUnit, processed: processed, catalogue: catalogue}
}

// WithUnitOfWork brackets the processed-event mark and every line's
// enqueue in one atomic scope (each EnqueueWorkUnit scope joins it).
func (uc *ApplyOrderAllocated) WithUnitOfWork(u ports.UnitOfWork) *ApplyOrderAllocated {
	uc.uow = u
	return uc
}

// OrderAllocatedLine is one allocated-and-released order line.
type OrderAllocatedLine struct {
	LineNo   int
	SKU      string
	PathId   string
	GiftWrap bool
}

type ApplyOrderAllocatedRequest struct {
	EventId     string
	OccurredAt  time.Time
	OrderId     string
	PromiseDate time.Time
	Lines       []OrderAllocatedLine
}

// Execute enqueues one WorkUnit per line under the deterministic id
// "{order_id}-line-{line_no}". It reports alreadyProcessed for a
// redelivery. Every line's path_id is validated against the declared
// process-path catalogue BEFORE anything is enqueued: an unrecognized
// path must fail loud (and end in the DLQ) rather than seed a WorkPool
// nothing downstream will ever service (fulfillment-execution ADR-0017).
func (uc *ApplyOrderAllocated) Execute(ctx context.Context, req ApplyOrderAllocatedRequest) (alreadyProcessed bool, err error) {
	return onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.OccurredAt, func(ctx context.Context) error {
		enqueues, err := uc.enqueueRequests(req)
		if err != nil {
			return err
		}
		for _, enqueue := range enqueues {
			_, err := uc.enqueueWorkUnit.Execute(ctx, enqueue)
			// release.ErrDuplicateEntry: this deterministic WorkUnitId is
			// already in the pool, so it is by construction the SAME
			// logical work unit (e.g. a prior partial-allocation event for
			// the same line, or operator replay under a new event id). A
			// benign no-op, never a reason to stall the partition.
			if errors.Is(err, release.ErrDuplicateEntry) {
				continue
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (uc *ApplyOrderAllocated) enqueueRequests(req ApplyOrderAllocatedRequest) ([]EnqueueWorkUnitRequest, error) {
	cpt := shared.NewCPT(req.PromiseDate)
	out := make([]EnqueueWorkUnitRequest, 0, len(req.Lines))
	for _, line := range req.Lines {
		pathId, err := shared.NewPathId(line.PathId)
		if err != nil {
			return nil, err
		}
		if _, err := uc.catalogue.Lookup(pathId.String()); err != nil {
			return nil, err
		}
		// The line number is stored explicitly (ADR-0036) as well as
		// staying in the id. A non-positive or out-of-range (> 2147483647,
		// the 32-bit column limit) value from upstream is "unknown"
		// (0): the id is still built exactly as before, so a malformed line
		// is never newly rejected (and never dead-lettered).
		lineNo := line.LineNo
		if workunit.ValidateLineNo(lineNo) != nil {
			lineNo = 0
		}
		out = append(out, EnqueueWorkUnitRequest{
			WorkUnitId: fmt.Sprintf("%s-line-%d", req.OrderId, line.LineNo),
			PathId:     pathId,
			CPT:        cpt,
			Reference:  req.OrderId,
			SKU:        line.SKU,
			GiftWrap:   line.GiftWrap,
			LineNo:     lineNo,
		})
	}
	return out, nil
}
