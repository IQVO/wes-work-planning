package kafka

import (
	"context"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// demandFixture wires just enough of the real stack (in-memory repos, the
// existing EnqueueWorkUnit use case) to exercise handleNetworkDemandEvent
// without touching a broker.
type demandFixture struct {
	workUnits *memory.WorkUnitRepo
	processed *memory.ProcessedEventRepo
	consumer  *Consumer
}

func newDemandFixture() demandFixture {
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)}
	processed := memory.NewProcessedEventRepo()

	enqueueWorkUnit := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)

	return demandFixture{
		workUnits: workUnits,
		processed: processed,
		consumer: &Consumer{
			applyWorkDemandReleased: usecases.NewApplyWorkDemandReleased(enqueueWorkUnit, processed, testCatalogue()),
			catalogue:               testCatalogue(),
		},
	}
}

func workDemandEnvelope(t *testing.T, eventId, ceType string, data workDemandReleasedData) ce.Event {
	t.Helper()
	return testEvent(t, eventId, ceType, "/warehouse/network-inventory-planning", data.DemandId, data)
}

func sampleDemandData(demandId string) workDemandReleasedData {
	return workDemandReleasedData{
		DemandId:    demandId,
		WorkKind:    "TRANSFER_PICK",
		TransferRef: "TRF-2026-042",
		PathId:      "pick-transfer-a",
		SiteId:      "site-north-1",
		CPT:         time.Date(2026, 10, 6, 23, 0, 0, 0, time.UTC),
		SKU:         "SKU-T1",
		Quantity:    17,
	}
}

func TestHandleNetworkDemandEvent_EnqueuesTransferWorkUnit(t *testing.T) {
	f := newDemandFixture()
	env := workDemandEnvelope(t, "evt-demand-1", cloudevents.TypeWorkDemandReleased, sampleDemandData("demand-1"))

	if err := f.consumer.handleNetworkDemandEvent(context.Background(), env); err != nil {
		t.Fatalf("handleNetworkDemandEvent: %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "demand-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.Reference() != "demand-1" {
		t.Errorf("Reference = %q, want demand-1", unit.Reference())
	}
	if unit.SKU() != "SKU-T1" {
		t.Errorf("SKU = %q, want SKU-T1", unit.SKU())
	}
	if unit.TransferRef() != "TRF-2026-042" {
		t.Errorf("TransferRef = %q, want TRF-2026-042", unit.TransferRef())
	}
	if unit.WorkKind() != workunit.WorkKindTransferPick {
		t.Errorf("WorkKind = %q, want TRANSFER_PICK", unit.WorkKind())
	}
	if unit.SiteId() != "site-north-1" {
		t.Errorf("SiteId = %q, want site-north-1", unit.SiteId())
	}
	if unit.Quantity() != 17 {
		t.Errorf("Quantity = %d, want 17", unit.Quantity())
	}
}

func TestHandleNetworkDemandEvent_IgnoresOtherEventTypes(t *testing.T) {
	f := newDemandFixture()
	env := workDemandEnvelope(t, "evt-demand-other", "SomethingElse", sampleDemandData("demand-2"))

	if err := f.consumer.handleNetworkDemandEvent(context.Background(), env); err != nil {
		t.Fatalf("handleNetworkDemandEvent: %v", err)
	}

	if _, err := f.workUnits.FindById(context.Background(), "demand-2"); err == nil {
		t.Fatalf("expected no work unit to be enqueued for an unknown event type")
	}
}

func TestHandleNetworkDemandEvent_UnknownPathIsError(t *testing.T) {
	f := newDemandFixture()
	data := sampleDemandData("demand-3")
	data.PathId = "not-a-real-path"
	env := workDemandEnvelope(t, "evt-demand-badpath", cloudevents.TypeWorkDemandReleased, data)

	// An unknown path fails the handler (and would DLQ on a broker) —
	// never a silent skip.
	if err := f.consumer.handleNetworkDemandEvent(context.Background(), env); err == nil {
		t.Fatalf("expected an error for an unknown path_id")
	}
	if _, err := f.workUnits.FindById(context.Background(), "demand-3"); err == nil {
		t.Fatalf("no work unit may be created for an unknown path")
	}
}

func TestHandleNetworkDemandEvent_RedeliveryIsNoOp(t *testing.T) {
	f := newDemandFixture()
	env := workDemandEnvelope(t, "evt-demand-dup", cloudevents.TypeWorkDemandReleased, sampleDemandData("demand-4"))

	if err := f.consumer.handleNetworkDemandEvent(context.Background(), env); err != nil {
		t.Fatalf("first handleNetworkDemandEvent: %v", err)
	}
	if err := f.consumer.handleNetworkDemandEvent(context.Background(), env); err != nil {
		t.Fatalf("redelivered handleNetworkDemandEvent: %v", err)
	}
}
