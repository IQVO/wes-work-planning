package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func TestGetWorkUnit_ReturnsTheUnitWithThatId(t *testing.T) {
	f := newFixture()
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	uc := usecases.NewGetWorkUnit(f.workUnits)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	for _, req := range []usecases.EnqueueWorkUnitRequest{
		{WorkUnitId: "order-77213-line-1", PathId: pathId, CPT: cpt, Reference: "order-77213", SKU: "sku-482910"},
		{WorkUnitId: "order-77213-line-2", PathId: pathId, CPT: cpt, Reference: "order-77213"},
	} {
		if _, err := enqueue.Execute(context.Background(), req); err != nil {
			t.Fatalf("setup: unexpected error: %v", err)
		}
	}

	unit, err := uc.Execute(context.Background(), usecases.GetWorkUnitRequest{WorkUnitId: "order-77213-line-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if unit.Id() != "order-77213-line-1" {
		t.Fatalf("got id %q, want order-77213-line-1", unit.Id())
	}
	if unit.Reference() != "order-77213" {
		t.Fatalf("got reference %q, want order-77213", unit.Reference())
	}
	if unit.SKU() != "sku-482910" {
		t.Fatalf("got sku %q, want sku-482910", unit.SKU())
	}
	if !unit.PathId().Equals(pathId) {
		t.Fatalf("got pathId %q, want pick-a", unit.PathId().String())
	}
}

func TestGetWorkUnit_UnknownIdReturnsNotFound(t *testing.T) {
	f := newFixture()
	uc := usecases.NewGetWorkUnit(f.workUnits)

	unit, err := uc.Execute(context.Background(), usecases.GetWorkUnitRequest{WorkUnitId: "no-such-unit"})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("got err %v, want ports.ErrNotFound", err)
	}
	if unit != nil {
		t.Fatalf("got unit %v, want nil", unit)
	}
}
