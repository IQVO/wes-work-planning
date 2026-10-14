package kafka

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0040: the slotting stream decodes slotting-optimization's
// SlotPlanApproved EXACTLY as its apis/asyncapi.yaml example publishes it
// (testdata/slotting_SlotPlanApproved.golden.json is that example, byte for
// byte), plans one RELOCATE unit per move, ignores the other SlotPlan
// events, dead-letters deterministic poison after the retries, and never
// dead-letters a message only shutdown cancellation interrupted.

const goldenPlanId = "plan-3b0e8d2a-7c41-4f6a-9d15-2a8b6c4e1f07"

type slotConsumerFixture struct {
	consumer  *Consumer
	workUnits *memory.WorkUnitRepo
	dlq       *fakeDLQ
}

func newSlotConsumerFixture(t *testing.T) slotConsumerFixture {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "RELOCATE", MatchPrefix: "relocate", Intake: []string{pathcatalog.IntakeSlotRelocation}}})
	uc := usecases.NewApplySlotPlanApproved(workUnits, workUnits, memory.NewWorkPoolRepo(), memory.NewSlotPlanRepo(), memory.NewProcessedEventRepo(),
		catalogue, events.NewLogPublisher(nil), memory.FixedClock{At: time.Date(2026, 10, 8, 13, 0, 0, 0, time.UTC)})
	dlq := &fakeDLQ{}
	return slotConsumerFixture{
		consumer: &Consumer{
			dlqWriters:            map[string]dlqWriter{cloudevents.TopicSlottingEvents: dlq},
			applySlotPlanApproved: uc,
		},
		workUnits: workUnits,
		dlq:       dlq,
	}
}

func goldenSlotPlanApproved(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "slotting_SlotPlanApproved.golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	return raw
}

func (f slotConsumerFixture) dispatch(ctx context.Context, raw []byte) error {
	return f.consumer.dispatch(ctx, ctx, trace.SpanFromContext(ctx), cloudevents.TopicSlottingEvents, kafkago.Message{Key: []byte(goldenPlanId), Value: raw}, f.consumer.handleSlottingEvent)
}

func TestHandleSlottingEvent_GoldenSlotPlanApprovedDecodesIntoRelocateUnits(t *testing.T) {
	f := newSlotConsumerFixture(t)
	env, err := cloudevents.Decode(goldenSlotPlanApproved(t))
	if err != nil {
		t.Fatalf("the producer's example is not a valid CloudEvent: %v", err)
	}
	if err := f.consumer.handleSlottingEvent(context.Background(), env); err != nil {
		t.Fatalf("handleSlottingEvent: %v", err)
	}
	want := []struct {
		seq      string
		sku      string
		kind     workunit.RelocationKind
		from, to string
	}{
		{"0", "SKU-1", workunit.RelocationKindAssign, "", "WH1-FWD-A01-01-01-A"},
		{"1", "SKU-2", workunit.RelocationKindRelocate, "WH1-FWD-A02-03-01-A", "WH1-FWD-A01-01-02-A"},
		{"2", "SKU-3", workunit.RelocationKindVacate, "WH1-FWD-A01-01-03-A", ""},
	}
	for _, w := range want {
		unit, err := f.workUnits.FindById(context.Background(), goldenPlanId+"-move-"+w.seq)
		if err != nil {
			t.Fatalf("move %s not planned: %v", w.seq, err)
		}
		m, ok := unit.Relocation()
		if !ok || unit.SKU() != w.sku || m.Kind != w.kind || m.FromSlot != w.from || m.ToSlot != w.to || m.PlanId != goldenPlanId {
			t.Fatalf("move %s = sku %s %+v", w.seq, unit.SKU(), m)
		}
		if unit.SiteId() != "SITE-1" || unit.Reference() != goldenPlanId {
			t.Fatalf("move %s site=%s ref=%s", w.seq, unit.SiteId(), unit.Reference())
		}
		if !unit.CPT().Time().Equal(time.Date(2026, 10, 9, 12, 30, 0, 0, time.UTC)) {
			t.Fatalf("move %s cpt = %v, want approved_at + 24h", w.seq, unit.CPT().Time())
		}
	}
}

func TestHandleSlottingEvent_IgnoresOtherSlotPlanEvents(t *testing.T) {
	f := newSlotConsumerFixture(t)
	for _, typ := range []string{
		"com.warehouse.wms.slotting-optimization.slotplan.SlotPlanGenerated",
		"com.warehouse.wms.slotting-optimization.slotplan.SlotPlanRejected",
	} {
		env := testEvent(t, "evt-"+typ, typ, "/warehouse/slotting-optimization", "plan-1", map[string]any{"plan_id": "plan-1", "site_id": "SITE-1"})
		if err := f.consumer.handleSlottingEvent(context.Background(), env); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
	}
	if units, _ := f.workUnits.FindByReference(context.Background(), "plan-1"); len(units) != 0 {
		t.Fatalf("a non-approval event planned work: %d units", len(units))
	}
}

func TestDispatch_SlotPlanApproved_IsAppliedAndNotDeadLettered(t *testing.T) {
	f := newSlotConsumerFixture(t)
	if err := f.dispatch(context.Background(), goldenSlotPlanApproved(t)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("a valid plan was dead-lettered (%d)", f.dlq.count())
	}
}

func TestDispatch_SlotPlanApproved_PoisonIsDeadLetteredAfterRetries(t *testing.T) {
	f := newSlotConsumerFixture(t)
	poison := strings.Replace(string(goldenSlotPlanApproved(t)), `"kind": "Vacate"`, `"kind": "Swap"`, 1)
	if err := f.dispatch(context.Background(), []byte(poison)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if f.dlq.count() != 1 {
		t.Fatalf("DLQ messages = %d, want 1", f.dlq.count())
	}
	if got := headerValue(f.dlq.msgs[0].Headers, "x-dlq-source-topic"); got != cloudevents.TopicSlottingEvents {
		t.Fatalf("x-dlq-source-topic = %q", got)
	}
	if got := headerValue(f.dlq.msgs[0].Headers, "x-dlq-error"); !strings.Contains(got, "unknown relocation kind") {
		t.Fatalf("x-dlq-error = %q", got)
	}
}

// cancelAwareProcessed fails the processed-event claim once ctx is
// cancelled, the way the Postgres adapter does mid-shutdown.
type cancelAwareProcessed struct{ *memory.ProcessedEventRepo }

func (r cancelAwareProcessed) TryMarkProcessed(ctx context.Context, eventId string, at time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return r.ProcessedEventRepo.TryMarkProcessed(ctx, eventId, at)
}

func TestDispatch_SlotPlanApproved_CancellationIsNeverDeadLettered(t *testing.T) {
	f := newSlotConsumerFixture(t)
	workUnits := memory.NewWorkUnitRepo()
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "RELOCATE", MatchPrefix: "relocate"}})
	f.consumer.applySlotPlanApproved = usecases.NewApplySlotPlanApproved(workUnits, workUnits, memory.NewWorkPoolRepo(), memory.NewSlotPlanRepo(),
		cancelAwareProcessed{memory.NewProcessedEventRepo()}, catalogue, events.NewLogPublisher(nil), memory.FixedClock{At: time.Now()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.dispatch(ctx, goldenSlotPlanApproved(t)); err == nil {
		t.Fatal("dispatch on a cancelled context must return the cancellation, not commit")
	}
	if f.dlq.count() != 0 {
		t.Fatalf("a message interrupted by cancellation was dead-lettered (%d)", f.dlq.count())
	}
}

func TestConsumer_WithSlotPlansAddsTheSixthStream(t *testing.T) {
	c := newConsumer([]string{"localhost:1"}, "g", cloudevents.TopicFulfillmentEvents, nil, nil, nil, nil, nil, testCatalogue(), nil)
	if got := len(c.readers()); got != 5 {
		t.Fatalf("streams without slot plans = %d, want 5", got)
	}
	c.WithSlotPlans(nil)
	streams := c.readers()
	if len(streams) != 6 || streams[5].reader.Config().Topic != cloudevents.TopicSlottingEvents || streams[5].reader.Config().GroupID != "g" {
		t.Fatalf("streams = %d, want the slotting topic under the shared group", len(streams))
	}
	if _, ok := c.dlqWriters[cloudevents.TopicSlottingEvents]; !ok {
		t.Fatal("the slotting stream needs its own <topic>.dlq writer")
	}
	_ = c.Close()
}
