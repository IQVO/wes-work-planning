package kafka

// Event consumer contract tests (E1-E4) for the slotting-optimization
// edge: this repo's Consumer reads slotting-optimization's topic
// warehouse.slotting-optimization.events (cloudevents.TopicSlottingEvents
// wired into kafka.ReaderConfig by WithSlotPlans) and acts only on
// com.warehouse.wms.slotting-optimization.slotplan.SlotPlanApproved
// (handleSlottingEvent -> ApplySlotPlanApproved, ADR-0040). The contract
// is the PINNED copy of slotting-optimization's apis/asyncapi.yaml
// (testdata/slotting-optimization.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/slotting-optimization@develop).
//
// Golden fixtures come from the producer spec's OWN examples — the spec
// declares no payload-schema type consts, so the examples' own `type`
// attributes are the documented type list, which documentedTypes reads.
//
// Documented WITHOUT routing here: SlotPlanGenerated, SlotPlanRejected
// — E1-only (the handler's doc comment names them as ignored).

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
)

const pinnedSlottingSpec = "testdata/slotting-optimization.asyncapi.yaml"

func newSlottingContractFixture(t *testing.T) (*Consumer, *memory.WorkUnitRepo) {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "RELOCATE", MatchPrefix: "relocate", Intake: []string{pathcatalog.IntakeSlotRelocation}}})
	uc := usecases.NewApplySlotPlanApproved(workUnits, workUnits, memory.NewWorkPoolRepo(), memory.NewSlotPlanRepo(),
		memory.NewProcessedEventRepo(), catalogue, events.NewLogPublisher(nil), memory.FixedClock{At: time.Date(2026, 10, 8, 13, 0, 0, 0, time.UTC)})
	return &Consumer{applySlotPlanApproved: uc}, workUnits
}

func slottingGoldenExamples(t *testing.T) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedSlottingSpec)
	msg := doc.message(t, "SlotPlanApproved")
	if len(msg.Examples) == 0 {
		t.Fatal("pinned slotting-optimization spec documents SlotPlanApproved without examples")
	}
	return msg.Examples
}

// E1: the topic this repo's reader subscribes to is documented by the
// producer's pinned spec, AND the channel's message list documents the
// exact CloudEvents type handleSlottingEvent routes on.
func TestPinnedAsyncAPICoversSubscriptions_SlottingEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedSlottingSpec)
	if _, how := doc.channelMessageNames(t, cloudevents.TopicSlottingEvents); how == "" {
		t.Fatalf("no message list found for %s", cloudevents.TopicSlottingEvents)
	}
	documented := doc.documentedTypes(t, cloudevents.TopicSlottingEvents)
	if !documented[cloudevents.TypeSlotPlanApproved] {
		t.Errorf("pinned slotting-optimization spec does not document %s on %s (documented: %v)",
			cloudevents.TypeSlotPlanApproved, cloudevents.TopicSlottingEvents, documented)
	}
}

// E2: every SlotPlanApproved example the producer's pinned spec
// documents decodes through the REAL cloudevents.Decode and routes
// through the REAL handleSlottingEvent -> ApplySlotPlanApproved path:
// exactly one RELOCATE work unit per move, keyed plan-move-N, and a
// replayed CloudEvents id plans nothing more (unit set size compared
// before/after, never specific ids).
func TestGoldenEventsDecode_SlotPlanApproved(t *testing.T) {
	for _, ex := range slottingGoldenExamples(t) {
		t.Run(ex.Name, func(t *testing.T) {
			evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode golden example: %v", err)
			}
			if evt.Type() != cloudevents.TypeSlotPlanApproved {
				t.Fatalf("golden example type %q is not the routed type", evt.Type())
			}
			consumer, workUnits := newSlottingContractFixture(t)
			if err := consumer.handleSlottingEvent(context.Background(), evt); err != nil {
				t.Fatalf("handleSlottingEvent: %v", err)
			}
			data := ex.Payload["data"].(map[string]any)
			planId := data["plan_id"].(string)
			moves := data["moves"].([]any)
			assertSlottingPlanned(t, workUnits, planId, moves)
			before := slottingUnitCount(t, workUnits, planId)
			if err := consumer.handleSlottingEvent(context.Background(), evt); err != nil {
				t.Fatalf("replayed handleSlottingEvent: %v", err)
			}
			if after := slottingUnitCount(t, workUnits, planId); after != before {
				t.Errorf("replayed CloudEvents id changed the planned set: %d -> %d", before, after)
			}
		})
	}
}

func assertSlottingPlanned(t *testing.T, workUnits *memory.WorkUnitRepo, planId string, moves []any) {
	t.Helper()
	for i, raw := range moves {
		seq := strconv.Itoa(i)
		unit, err := workUnits.FindById(context.Background(), planId+"-move-"+seq)
		if err != nil {
			t.Fatalf("move %s not planned: %v", seq, err)
		}
		move := raw.(map[string]any)
		if sku, _ := move["sku"].(string); unit.SKU() != sku {
			t.Errorf("move %s sku %q, wire says %q", seq, unit.SKU(), sku)
		}
		if unit.Reference() != planId {
			t.Errorf("move %s reference %q, want the wire's plan_id %q", seq, unit.Reference(), planId)
		}
	}
}

func slottingUnitCount(t *testing.T, workUnits *memory.WorkUnitRepo, planId string) int {
	t.Helper()
	units, err := workUnits.FindByReference(context.Background(), planId)
	if err != nil {
		t.Fatalf("find units by plan reference: %v", err)
	}
	return len(units)
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope and the producer's dataschema urn, and the pinned spec's
// envelope schema requires those attributes.
func TestCloudEventsEnvelopeInvariants_Slotting(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedSlottingSpec)
	for _, ex := range slottingGoldenExamples(t) {
		evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
		if err != nil {
			t.Fatalf("decode golden example %s: %v", ex.Name, err)
		}
		if evt.SpecVersion() != cloudevents.SpecVersion {
			t.Errorf("%s: specversion %q, want %q", ex.Name, evt.SpecVersion(), cloudevents.SpecVersion)
		}
		if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
			t.Errorf("%s: id/source/subject/time must all be present", ex.Name)
		}
		if evt.Source() != "/warehouse/slotting-optimization" {
			t.Errorf("%s: source %q, want /warehouse/slotting-optimization", ex.Name, evt.Source())
		}
		if evt.DataMediaType() != cloudevents.DataContentType {
			t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
		}
		if evt.DataSchema() != "urn:warehouse:slotting-optimization:events:SlotPlanApproved:v1" {
			t.Errorf("%s: dataschema %q, want the producer's SlotPlanApproved v1 urn", ex.Name, evt.DataSchema())
		}
	}
	assertEnvelopeRequired(t, doc, "SlotPlanApproved")
}

// E4: unknown ADDITIVE data keys (the producer's own `assignments`
// forward map is the live example — this DTO reads only `moves`) must
// not fail decode or dispatch; an unknown event TYPE on the topic is
// silently skipped (SlotPlanGenerated/Rejected); and a non-CloudEvent
// message is a deterministic ErrNotCloudEvent — the signal
// Consumer.dispatch dead-letters once and never retries.
func TestUnknownAttributesTolerated_Slotting(t *testing.T) {
	golden := slottingGoldenExamples(t)[0].Payload

	tolerant := freshGoldenID(t, golden, "tolerated")
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	evt, err := cloudevents.Decode(goldenJSON(t, tolerant))
	if err != nil {
		t.Fatalf("decode with unknown data attributes: %v", err)
	}
	consumer, workUnits := newSlottingContractFixture(t)
	if err := consumer.handleSlottingEvent(context.Background(), evt); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if count := slottingUnitCount(t, workUnits, data["plan_id"].(string)); count == 0 {
		t.Error("unknown data attributes changed the outcome: nothing planned")
	}

	otherType := freshGoldenID(t, golden, "othertype")
	otherType["type"] = "com.warehouse.wms.slotting-optimization.slotplan.SlotPlanGenerated"
	evt, err = cloudevents.Decode(goldenJSON(t, otherType))
	if err != nil {
		t.Fatalf("decode unknown-type example: %v", err)
	}
	if err := consumer.handleSlottingEvent(context.Background(), evt); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	if _, err = cloudevents.Decode(goldenJSON(t, map[string]any{
		"plan_id": "plan-flat", "site_id": "SITE-1", "moves": []any{},
	})); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("flat payload decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
