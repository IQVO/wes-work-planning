package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func TestObserveLaborPlan_ProjectsLatestPlan(t *testing.T) {
	views := memory.NewLaborPlanViewRepo()
	processed := memory.NewProcessedEventRepo()
	uc := usecases.NewObserveLaborPlan(views, processed)

	pathId, _ := shared.NewPathId("pick-a")
	now := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)

	err := uc.Execute(context.Background(), usecases.ObserveLaborPlanRequest{
		EventId: "evt-1", PathId: pathId, PlannedHeads: 5, PlannedRate: 120, PlannedHours: 8, ObservedAt: now,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got, err := views.FindByPathId(context.Background(), pathId)
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	if got.PlannedHeads != 5 || got.PlannedRate != 120 || got.PlannedHours != 8 {
		t.Fatalf("unexpected view: %+v", got)
	}
}

func TestObserveLaborPlan_DuplicateEventIdIsIdempotent(t *testing.T) {
	views := memory.NewLaborPlanViewRepo()
	processed := memory.NewProcessedEventRepo()
	uc := usecases.NewObserveLaborPlan(views, processed)

	pathId, _ := shared.NewPathId("pick-a")
	now := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)

	req := usecases.ObserveLaborPlanRequest{
		EventId: "evt-dup", PathId: pathId, PlannedHeads: 5, PlannedRate: 120, PlannedHours: 8, ObservedAt: now,
	}
	if err := uc.Execute(context.Background(), req); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	// Redelivery with different values but the same event_id must not
	// re-apply — the view must remain exactly what it was.
	dup := req
	dup.PlannedHeads = 999
	if err := uc.Execute(context.Background(), dup); err != nil {
		t.Fatalf("second Execute: %v", err)
	}

	got, err := views.FindByPathId(context.Background(), pathId)
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	if got.PlannedHeads != 5 {
		t.Fatalf("expected duplicate delivery to have no effect, got PlannedHeads=%d", got.PlannedHeads)
	}
}

func TestObserveInventoryChange_StockReservedDecrementsAndRevokedIncrements(t *testing.T) {
	views := memory.NewInventoryViewRepo()
	processed := memory.NewProcessedEventRepo()
	uc := usecases.NewObserveInventoryChange(views, processed)

	now := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)

	if _, err := uc.Execute(context.Background(), usecases.ObserveInventoryChangeRequest{
		EventId: "evt-reserve-1", SKU: "sku-1", Quantity: 10, Delta: -10, ObservedAt: now,
	}); err != nil {
		t.Fatalf("reserve Execute: %v", err)
	}

	view, err := views.FindBySKU(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("FindBySKU: %v", err)
	}
	if view.UsableQuantity != -10 {
		t.Fatalf("expected usable quantity -10 after reservation, got %d", view.UsableQuantity)
	}

	if _, err := uc.Execute(context.Background(), usecases.ObserveInventoryChangeRequest{
		EventId: "evt-revoke-1", SKU: "sku-1", Quantity: 4, Delta: 4, ObservedAt: now,
	}); err != nil {
		t.Fatalf("revoke Execute: %v", err)
	}

	view, err = views.FindBySKU(context.Background(), "sku-1")
	if err != nil {
		t.Fatalf("FindBySKU: %v", err)
	}
	if view.UsableQuantity != -6 {
		t.Fatalf("expected usable quantity -6 after revocation, got %d", view.UsableQuantity)
	}
}

func TestObserveInventoryChange_DuplicateEventIdDoesNotDoubleDecrement(t *testing.T) {
	views := memory.NewInventoryViewRepo()
	processed := memory.NewProcessedEventRepo()
	uc := usecases.NewObserveInventoryChange(views, processed)

	now := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)
	req := usecases.ObserveInventoryChangeRequest{
		EventId: "evt-reserve-dup", SKU: "sku-2", Quantity: 7, Delta: -7, ObservedAt: now,
	}

	if _, err := uc.Execute(context.Background(), req); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if _, err := uc.Execute(context.Background(), req); err != nil {
		t.Fatalf("second (redelivered) Execute: %v", err)
	}

	view, err := views.FindBySKU(context.Background(), "sku-2")
	if err != nil {
		t.Fatalf("FindBySKU: %v", err)
	}
	if view.UsableQuantity != -7 {
		t.Fatalf("expected redelivery to have no additional effect, got usable quantity %d", view.UsableQuantity)
	}
}

func TestInventoryView_UnknownSKUReturnsNotFound(t *testing.T) {
	views := memory.NewInventoryViewRepo()
	uc := usecases.NewInventoryView(views)

	if _, err := uc.Execute(context.Background(), usecases.InventoryViewRequest{SKU: "does-not-exist"}); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLaborPlanView_UnknownPathReturnsNotFound(t *testing.T) {
	views := memory.NewLaborPlanViewRepo()
	uc := usecases.NewLaborPlanView(views)

	pathId, _ := shared.NewPathId("unknown-path")
	if _, err := uc.Execute(context.Background(), usecases.LaborPlanViewRequest{PathId: pathId}); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
