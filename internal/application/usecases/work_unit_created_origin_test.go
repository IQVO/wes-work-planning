package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// These tests pin ADR-0041: the WorkUnitCreated fact raised for an order
// line carries the originating reference (the order id) and the line number
// as explicit attributes, so order-management confirms a release by matching
// them instead of parsing the "{order_id}-line-{line_no}" work-unit id.

type capturePublisher struct{ events []shared.DomainEvent }

func (c *capturePublisher) Publish(_ context.Context, evs ...shared.DomainEvent) error {
	c.events = append(c.events, evs...)
	return nil
}

func createdEvents(t *testing.T, pub *capturePublisher) []shared.WorkUnitCreated {
	t.Helper()
	var out []shared.WorkUnitCreated
	for _, e := range pub.events {
		if created, ok := e.(shared.WorkUnitCreated); ok {
			out = append(out, created)
		}
	}
	return out
}

func TestApplyOrderAllocated_WorkUnitCreatedCarriesReferenceAndLineNo(t *testing.T) {
	pub := &capturePublisher{}
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	enqueue := usecases.NewEnqueueWorkUnit(memory.NewWorkUnitRepo(), memory.NewWorkPoolRepo(), pub, clock)
	uc := usecases.NewApplyOrderAllocated(enqueue, memory.NewProcessedEventRepo(), orderCatalogue())

	req := orderRequest("evt-origin",
		usecases.OrderAllocatedLine{LineNo: 1, SKU: "SKU-1", PathId: "pick-a"},
		usecases.OrderAllocatedLine{LineNo: 3, SKU: "SKU-3", PathId: "pick-a"},
	)
	if _, err := uc.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got := createdEvents(t, pub)
	if len(got) != 2 {
		t.Fatalf("got %d WorkUnitCreated, want 2", len(got))
	}
	for i, want := range []struct {
		id     string
		lineNo int
	}{{"order-1-line-1", 1}, {"order-1-line-3", 3}} {
		if got[i].WorkUnitId != want.id || got[i].Reference != "order-1" || got[i].LineNo != want.lineNo {
			t.Fatalf("event %d = {id %q ref %q line %d}, want {id %q ref %q line %d}",
				i, got[i].WorkUnitId, got[i].Reference, got[i].LineNo, want.id, "order-1", want.lineNo)
		}
	}
}

func TestApplyOrderAllocated_UnknownLineNoIsNotAnnounced(t *testing.T) {
	pub := &capturePublisher{}
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	enqueue := usecases.NewEnqueueWorkUnit(memory.NewWorkUnitRepo(), memory.NewWorkPoolRepo(), pub, clock)
	uc := usecases.NewApplyOrderAllocated(enqueue, memory.NewProcessedEventRepo(), orderCatalogue())

	// A malformed upstream line number is stored as 0 ("unknown", ADR-0036);
	// the fact must say so by carrying 0, so the encoder omits line_no.
	req := orderRequest("evt-unknown", usecases.OrderAllocatedLine{LineNo: 0, SKU: "SKU-1", PathId: "pick-a"})
	if _, err := uc.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got := createdEvents(t, pub)
	if len(got) != 1 || got[0].Reference != "order-1" || got[0].LineNo != 0 {
		t.Fatalf("got %+v, want one event with reference order-1 and line no 0", got)
	}
}
