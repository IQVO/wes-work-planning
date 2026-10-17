package kafka

import (
	"context"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// A second, DISTINCT TaskCompleted (new CloudEvents id) for a work unit
// that is already completed must never reach the dead-letter path: the
// handler returns nil — nothing to retry, nothing to publish to
// warehouse.fulfillment.events.dlq — the event is marked processed, and
// the unit stays Completed. The classification itself lives in
// ApplyTaskCompleted (ADR-0043); this test pins it at the consumer seam
// that dispatch() would otherwise dead-letter through.
func TestHandleFulfillmentEvent_AlreadyCompletedWorkUnitIsNotDeadLettered(t *testing.T) {
	f := newFulfillmentFixture()

	if err := f.consumer.handleFulfillmentEvent(context.Background(), taskCompletedEnvelope(t, "evt-task-1", "wu-1")); err != nil {
		t.Fatalf("first handleFulfillmentEvent: %v", err)
	}

	// A distinct event id for the same, already-completed work unit.
	if err := f.consumer.handleFulfillmentEvent(context.Background(), taskCompletedEnvelope(t, "evt-task-2", "wu-1")); err != nil {
		t.Fatalf("a stale completion must not be returned as an error (dispatch would retry it and then dead-letter): %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "wu-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.State() != workunit.Completed {
		t.Fatalf("expected work unit to stay Completed, got %v", unit.State())
	}

	// Redelivery of the stale event stays a cheap no-op.
	if err := f.consumer.handleFulfillmentEvent(context.Background(), taskCompletedEnvelope(t, "evt-task-2", "wu-1")); err != nil {
		t.Fatalf("redelivered stale completion: %v", err)
	}
}
