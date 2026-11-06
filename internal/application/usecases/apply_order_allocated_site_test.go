package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ADR-0048: ApplyOrderAllocated threads the event's additive order-level
// site onto every enqueued line unit; "" (a payload that predates the
// field) keeps the exact pre-ADR-0048 behaviour.

func orderSiteCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{{Id: "PICK", MatchPrefix: "pick"}})
}

func TestApplyOrderAllocated_ThreadsSiteOntoEveryLineUnit(t *testing.T) {
	f := newFixture()
	uc := usecases.NewApplyOrderAllocated(
		usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock),
		memory.NewProcessedEventRepo(), orderSiteCatalogue())

	_, err := uc.Execute(context.Background(), usecases.ApplyOrderAllocatedRequest{
		EventId:     "evt-order-site-1",
		OccurredAt:  f.clock.Now(),
		OrderId:     "order-site",
		SiteId:      "WH1",
		PromiseDate: f.clock.Now().Add(15 * time.Hour),
		Lines: []usecases.OrderAllocatedLine{
			{LineNo: 1, SKU: "SKU-1", PathId: "pick-a"},
			{LineNo: 2, SKU: "SKU-2", PathId: "pick-a"},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	for _, id := range []string{"order-site-line-1", "order-site-line-2"} {
		unit, err := f.workUnits.FindById(context.Background(), id)
		if err != nil {
			t.Fatalf("FindById %s: %v", id, err)
		}
		if unit.SiteId() != "WH1" {
			t.Fatalf("%s siteId = %q, want WH1", id, unit.SiteId())
		}
	}
}

func TestApplyOrderAllocated_EmptySiteKeepsUnitsSiteless(t *testing.T) {
	f := newFixture()
	uc := usecases.NewApplyOrderAllocated(
		usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock),
		memory.NewProcessedEventRepo(), orderSiteCatalogue())

	_, err := uc.Execute(context.Background(), usecases.ApplyOrderAllocatedRequest{
		EventId:     "evt-order-nosite-1",
		OccurredAt:  f.clock.Now(),
		OrderId:     "order-nosite",
		PromiseDate: f.clock.Now().Add(15 * time.Hour),
		Lines:       []usecases.OrderAllocatedLine{{LineNo: 1, SKU: "SKU-1", PathId: "pick-a"}},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "order-nosite-line-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.SiteId() != "" {
		t.Fatalf("siteId = %q, want empty", unit.SiteId())
	}
	if _, ok := unit.Pick(); ok {
		t.Fatal("an order unit must stay plan-less: wes-work-planning does not know the line quantity (ADR-0048)")
	}
}

// The enqueued order unit keeps its deterministic id and CPT shape — the
// site is additive, nothing else about the enqueue moves.
func TestApplyOrderAllocated_SiteChangesNothingElse(t *testing.T) {
	f := newFixture()
	cpt := shared.NewCPT(f.clock.Now().Add(15 * time.Hour))
	uc := usecases.NewApplyOrderAllocated(
		usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock),
		memory.NewProcessedEventRepo(), orderSiteCatalogue())

	_, err := uc.Execute(context.Background(), usecases.ApplyOrderAllocatedRequest{
		EventId:     "evt-order-shape-1",
		OccurredAt:  f.clock.Now(),
		OrderId:     "order-shape",
		SiteId:      "WH1",
		PromiseDate: cpt.Time(),
		Lines:       []usecases.OrderAllocatedLine{{LineNo: 7, SKU: "SKU-7", PathId: "pick-a", GiftWrap: true}},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "order-shape-line-7")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if !unit.CPT().Time().Equal(cpt.Time()) {
		t.Fatalf("CPT = %v, want %v", unit.CPT().Time(), cpt.Time())
	}
	if unit.Reference() != "order-shape" || unit.LineNo() != 7 || !unit.GiftWrap() {
		t.Fatalf("unit shape changed: %+v", unit)
	}
}
