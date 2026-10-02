package usecases

import (
	"context"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/inventoryview"
)

// ObserveInventoryChange is an additive projector use case: it applies
// Inventory's StockReserved/ReservationRevoked integration events to the
// UsableInventoryObserved read model, keyed by SKU.
type ObserveInventoryChange struct {
	views     ports.InventoryViewRepo
	processed ports.ProcessedEventRepo
	uow       ports.UnitOfWork
}

func NewObserveInventoryChange(views ports.InventoryViewRepo, processed ports.ProcessedEventRepo) *ObserveInventoryChange {
	return &ObserveInventoryChange{views: views, processed: processed}
}

// WithUnitOfWork brackets the processed-event mark and the delta in one
// atomic scope (ADR-0028), so a failed ApplyDelta never leaves the event
// marked processed.
func (uc *ObserveInventoryChange) WithUnitOfWork(u ports.UnitOfWork) *ObserveInventoryChange {
	uc.uow = u
	return uc
}

type ObserveInventoryChangeRequest struct {
	EventId    string
	SKU        string
	Quantity   int
	Delta      int // -Quantity for StockReserved, +Quantity for ReservationRevoked
	ObservedAt time.Time
}

func (uc *ObserveInventoryChange) Execute(ctx context.Context, req ObserveInventoryChangeRequest) (inventoryview.UsableInventoryObserved, error) {
	var view inventoryview.UsableInventoryObserved
	alreadyProcessed, err := onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.ObservedAt, func(ctx context.Context) error {
		var err error
		view, err = uc.views.ApplyDelta(ctx, req.SKU, req.Delta, req.ObservedAt)
		return err
	})
	if err != nil {
		return inventoryview.UsableInventoryObserved{}, err
	}
	if alreadyProcessed {
		return uc.views.FindBySKU(ctx, req.SKU)
	}
	return view, nil
}
