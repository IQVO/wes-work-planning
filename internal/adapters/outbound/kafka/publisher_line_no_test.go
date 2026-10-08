package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// Decision 18 (ADR-0036): WorkReleased v1 `data` gains an OPTIONAL line_no on
// BOTH topics (integration and analytics), read off the WorkUnit at encode
// time exactly like ref/cpt. Omitted when the line is unknown.

// newReleasedLineUnit stores a released, order-driven work unit for line
// lineNo (0 = unknown) of an order, the way ApplyOrderAllocated would have
// enqueued it and ReleaseNextWork released it.
func newReleasedLineUnit(t *testing.T, id string, lineNo int) *memory.WorkUnitRepo {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	cpt := shared.NewCPT(time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC))
	unit, err := workunit.NewWorkUnit(id, pathId, cpt, "ref-1")
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	unit.SetSKU("sku-plain")
	unit.SetGiftWrap(true)
	unit.SetLineNo(lineNo)
	if err := unit.Release(time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return workUnits
}

func releasedEvent(t *testing.T, id string) shared.WorkReleased {
	t.Helper()
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	return shared.NewWorkReleased(id, pathId, time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC))
}

func integrationData(t *testing.T, workUnits *memory.WorkUnitRepo, id string) map[string]any {
	t.Helper()
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-line" })
	if err := pub.Publish(context.Background(), releasedEvent(t, id)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return decodeWorkReleasedData(t, writer.msgs[0])
}

func analyticsData(t *testing.T, p *outboundkafka.AnalyticsPublisher, w *fakeWriter, id string) map[string]any {
	t.Helper()
	if err := p.Publish(context.Background(), releasedEvent(t, id)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return decodeWorkReleasedData(t, w.msgs[len(w.msgs)-1])
}

func TestPublisher_WorkReleased_CarriesLineNoWhenKnown(t *testing.T) {
	data := integrationData(t, newReleasedLineUnit(t, "order-1-line-3", 3), "order-1-line-3")

	if data["line_no"] != float64(3) {
		t.Fatalf("line_no = %v (%T), want the integer 3; payload %v", data["line_no"], data["line_no"], data)
	}
	// The required base fields are untouched.
	if data["work_unit_id"] != "order-1-line-3" || data["ref"] != "ref-1" {
		t.Errorf("base fields regressed: %v", data)
	}
}

func TestPublisher_WorkReleased_OmitsLineNoWhenUnknown(t *testing.T) {
	data := integrationData(t, newReleasedLineUnit(t, "wu-legacy", 0), "wu-legacy")

	if _, ok := data["line_no"]; ok {
		t.Fatalf("line_no must be omitted for a unit with no stored line (created before ADR-0036), got %v", data)
	}
}

func TestPublisher_WorkReleased_TransferUnitHasNoLineNo(t *testing.T) {
	data := publishAndDecode(t, newReleasedTransferUnit(t, "wu-xfer-line"), "wu-xfer-line")

	if _, ok := data["line_no"]; ok {
		t.Fatalf("a transfer-referenced unit must not carry line_no, got %v", data)
	}
}

func TestAnalyticsPublisher_WorkReleased_CarriesLineNoWhenKnown(t *testing.T) {
	w := &fakeWriter{}
	p := outboundkafka.NewAnalyticsPublisherWithWriter(w, newAnalyticsID()).WithWorkUnits(newReleasedLineUnit(t, "order-1-line-3", 3))

	data := analyticsData(t, p, w, "order-1-line-3")

	if data["line_no"] != float64(3) {
		t.Fatalf("analytics line_no = %v, want 3; payload %v", data["line_no"], data)
	}
	if data["path_id"] != "pick-a" || data["work_unit_id"] != "order-1-line-3" {
		t.Errorf("identity fields regressed: %v", data)
	}
}

func TestAnalyticsPublisher_WorkReleased_OmitsLineNoWhenUnknownOrUnwired(t *testing.T) {
	t.Run("unit has no line", func(t *testing.T) {
		w := &fakeWriter{}
		p := outboundkafka.NewAnalyticsPublisherWithWriter(w, newAnalyticsID()).WithWorkUnits(newReleasedLineUnit(t, "wu-legacy", 0))
		if data := analyticsData(t, p, w, "wu-legacy"); data["line_no"] != nil {
			t.Fatalf("line_no must be omitted, got %v", data)
		}
	})
	t.Run("unit not found", func(t *testing.T) {
		w := &fakeWriter{}
		p := outboundkafka.NewAnalyticsPublisherWithWriter(w, newAnalyticsID()).WithWorkUnits(memory.NewWorkUnitRepo())
		if data := analyticsData(t, p, w, "wu-missing"); data["line_no"] != nil {
			t.Fatalf("a failed lookup must only omit the optional field, got %v", data)
		}
	})
	t.Run("publisher built without a work unit repo", func(t *testing.T) {
		w := &fakeWriter{}
		p := outboundkafka.NewAnalyticsPublisherWithWriter(w, newAnalyticsID())
		if data := analyticsData(t, p, w, "order-1-line-3"); data["line_no"] != nil {
			t.Fatalf("line_no must be omitted, got %v", data)
		}
	})
}

// The wire shape for a unit WITH a line, on both topics. The pre-existing
// goldens (a unit with no line) are deliberately untouched: they are the
// proof that nothing changes for a legacy unit.
func TestCloudEvents_Golden_WorkReleased_WithLineNo(t *testing.T) {
	tc := goldenCase{
		name:    "WorkReleased",
		event:   releasedEvent(t, "wu-gold"),
		ceType:  "com.warehouse.wes.work-planning.workunit.WorkReleased",
		subject: "wu-gold",
	}
	workUnits := newReleasedLineUnit(t, "wu-gold", 3)

	t.Run("integration", func(t *testing.T) {
		pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "11111111-1111-4111-8111-111111111111" })
		encoded, err := pub.Encode(context.Background(), tc.event)
		if err != nil || len(encoded) != 1 {
			t.Fatalf("Encode: %d encoded, err %v", len(encoded), err)
		}
		assertGolden(t, encoded[0], tc, cloudevents.StreamEvents, "events_WorkReleasedLineNo")
	})
	t.Run("analytics", func(t *testing.T) {
		pub := outboundkafka.NewAnalyticsPublisherWithWriter(&fakeWriter{}, func() string { return "22222222-2222-4222-8222-222222222222" }).WithWorkUnits(workUnits)
		encoded, err := pub.Encode(context.Background(), tc.event)
		if err != nil || len(encoded) != 1 {
			t.Fatalf("Encode: %d encoded, err %v", len(encoded), err)
		}
		assertGolden(t, encoded[0], tc, cloudevents.StreamAnalytics, "analytics_WorkReleasedLineNo")
	})
}
