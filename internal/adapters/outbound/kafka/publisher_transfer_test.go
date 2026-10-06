package kafka_test

import (
	"context"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// newReleasedTransferUnit stores a released work unit carrying full
// transfer metadata, the way ApplyWorkDemandReleased would have enqueued
// and ReleaseNextWork released it.
func newReleasedTransferUnit(t *testing.T, id string) *memory.WorkUnitRepo {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	pathId, err := shared.NewPathId("pick-transfer-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cpt := shared.NewCPT(time.Date(2026, 10, 6, 23, 0, 0, 0, time.UTC))
	unit, err := workunit.NewWorkUnit(id, pathId, cpt, "demand-"+id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	unit.SetSKU("sku-transfer")
	unit.SetTransferRef("TRF-2026-042")
	unit.SetWorkKind(workunit.WorkKindTransferPick)
	unit.SetSiteId("site-north-1")
	unit.SetQuantity(17)
	if err := unit.Release(time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return workUnits
}

func publishAndDecode(t *testing.T, workUnits *memory.WorkUnitRepo, workUnitId string) map[string]any {
	t.Helper()
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-xfer" })

	pathId, _ := shared.NewPathId("pick-transfer-a")
	event := shared.NewWorkReleased(workUnitId, pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return decodeWorkReleasedData(t, writer.msgs[0])
}

func TestPublisher_WorkReleased_TransferUnitCarriesTransferFields(t *testing.T) {
	data := publishAndDecode(t, newReleasedTransferUnit(t, "wu-xfer-1"), "wu-xfer-1")

	if data["transfer_ref"] != "TRF-2026-042" {
		t.Errorf("transfer_ref = %v, want TRF-2026-042", data["transfer_ref"])
	}
	if data["work_kind"] != "TRANSFER_PICK" {
		t.Errorf("work_kind = %v, want TRANSFER_PICK", data["work_kind"])
	}
	if data["site_id"] != "site-north-1" {
		t.Errorf("site_id = %v, want site-north-1", data["site_id"])
	}
	if qty, ok := data["quantity"].(float64); !ok || qty != 17 {
		t.Errorf("quantity = %v, want 17", data["quantity"])
	}
	// The base required fields still hold (sku is deliberately NOT part of
	// the published payload — it is read only to derive classification
	// hints, ADR-0009).
	if data["ref"] != "demand-wu-xfer-1" || data["cpt"] != "2026-10-06T23:00:00Z" {
		t.Errorf("base fields regressed: %v", data)
	}
}

func TestPublisher_WorkReleased_NonTransferUnitOmitsTransferFields(t *testing.T) {
	data := publishAndDecode(t, newReleasedWorkUnitWithGiftWrap(t, "wu-xfer-plain", "sku-plain", false), "wu-xfer-plain")

	for _, field := range []string{"transfer_ref", "work_kind", "site_id", "quantity"} {
		if _, ok := data[field]; ok {
			t.Errorf("did not expect %s on a non-transfer unit, got %v", field, data)
		}
	}
}
