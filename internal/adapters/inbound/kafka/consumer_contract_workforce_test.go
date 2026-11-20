package kafka

// Event consumer contract tests (E1-E4) for the workforce-management
// edge: this repo's Consumer reads workforce-management's shared topic
// warehouse.workforce.events (cloudevents.TopicWorkforceEvents wired
// into kafka.ReaderConfig in newConsumer) and acts only on
// com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted
// (handleWorkforceEvent). The contract is the PINNED copy of
// workforce-management's apis/asyncapi.yaml
// (testdata/workforce-management.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/workforce-management@develop).
//
// Golden fixtures come from the producer spec's OWN examples — every
// ShiftPlanCommitted example here is a real wire document
// workforce-management documents for this channel.

import (
	"context"
	"errors"
	"testing"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

const pinnedWorkforceSpec = "testdata/workforce-management.asyncapi.yaml"

func newWorkforceContractFixture() (*Consumer, *memory.LaborPlanViewRepo) {
	views := memory.NewLaborPlanViewRepo()
	consumer := &Consumer{
		observeLabor: usecases.NewObserveLaborPlan(views, memory.NewProcessedEventRepo()),
		catalogue:    testCatalogue(),
	}
	return consumer, views
}

func workforceGoldenExamples(t *testing.T) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedWorkforceSpec)
	msg := doc.message(t, "ShiftPlanCommitted")
	if len(msg.Examples) == 0 {
		t.Fatal("pinned workforce-management spec documents ShiftPlanCommitted without examples")
	}
	return msg.Examples
}

// E1: the topic this repo's reader actually subscribes to (the
// cloudevents.TopicWorkforceEvents constant newConsumer wires into
// kafka.ReaderConfig) is documented by the producer's pinned spec, AND
// the message list on that channel documents the exact CloudEvents type
// handleWorkforceEvent routes on.
func TestPinnedAsyncAPICoversSubscriptions_WorkforceEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedWorkforceSpec)
	if _, how := doc.channelMessageNames(t, cloudevents.TopicWorkforceEvents); how == "" {
		t.Fatalf("no message list found for %s", cloudevents.TopicWorkforceEvents)
	}
	documented := doc.documentedTypes(t, cloudevents.TopicWorkforceEvents)
	if !documented[cloudevents.TypeShiftPlanCommitted] {
		t.Errorf("pinned workforce-management spec does not document %s on %s (documented: %v)",
			cloudevents.TypeShiftPlanCommitted, cloudevents.TopicWorkforceEvents, documented)
	}
}

// E2: every ShiftPlanCommitted example the producer's pinned spec
// documents decodes through the REAL cloudevents.Decode and routes
// through the REAL handleWorkforceEvent -> ObserveLaborPlan path: the
// labor-plan view observes the wire's own path/heads/rate/hours, and a
// replayed CloudEvents id observes nothing new (the same row again,
// never a second one).
func TestGoldenEventsDecode_WorkforceShiftPlanCommitted(t *testing.T) {
	for _, ex := range workforceGoldenExamples(t) {
		t.Run(ex.Name, func(t *testing.T) {
			evt := decodeGoldenWorkforce(t, ex)
			consumer, views := newWorkforceContractFixture()
			if err := consumer.handleWorkforceEvent(context.Background(), evt); err != nil {
				t.Fatalf("handleWorkforceEvent: %v", err)
			}
			assertWorkforceObserved(t, ex, evt, views)
			if err := consumer.handleWorkforceEvent(context.Background(), evt); err != nil {
				t.Fatalf("replayed handleWorkforceEvent: %v", err)
			}
			assertWorkforceObserved(t, ex, evt, views) // idempotent: same single row, same facts
		})
	}
}

func decodeGoldenWorkforce(t *testing.T, ex pinnedExample) (evt ce.Event) {
	t.Helper()
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode golden example: %v", err)
	}
	if evt.Type() != cloudevents.TypeShiftPlanCommitted {
		t.Fatalf("golden example type %q is not the routed type", evt.Type())
	}
	return evt
}

// assertWorkforceObserved asserts exactly one labor-plan row exists for
// the wire's path, carrying the wire's own heads/rate.
func assertWorkforceObserved(t *testing.T, ex pinnedExample, evt ce.Event, views *memory.LaborPlanViewRepo) {
	t.Helper()
	data := ex.Payload["data"].(map[string]any)
	pathId := mustPathId(t, data["path_id"].(string))
	view, err := views.FindByPathId(context.Background(), pathId)
	if err != nil {
		t.Fatalf("no labor plan observed for the golden path: %v", err)
	}
	if want := goldenInt(t, data["planned_heads"]); view.PlannedHeads != want {
		t.Errorf("observed planned_heads %d, wire says %d", view.PlannedHeads, want)
	}
	if want, ok := data["planned_rate"].(float64); ok && view.PlannedRate != want {
		t.Errorf("observed planned_rate %v, wire says %v", view.PlannedRate, want)
	}
	if !view.ObservedAt.Equal(evt.Time()) {
		t.Errorf("observed at %v, CloudEvents time is %v", view.ObservedAt, evt.Time())
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, source, type, subject, time,
// datacontenttype application/json, and the producer's dataschema urn —
// and the pinned spec's envelope schema requires those attributes.
func TestCloudEventsEnvelopeInvariants_Workforce(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedWorkforceSpec)
	for _, ex := range workforceGoldenExamples(t) {
		evt := decodeGoldenWorkforce(t, ex)
		if evt.SpecVersion() != cloudevents.SpecVersion {
			t.Errorf("%s: specversion %q, want %q", ex.Name, evt.SpecVersion(), cloudevents.SpecVersion)
		}
		if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
			t.Errorf("%s: id/source/subject/time must all be present (got %q/%q/%q/%v)",
				ex.Name, evt.ID(), evt.Source(), evt.Subject(), evt.Time())
		}
		if evt.Source() != "/warehouse/workforce-management" {
			t.Errorf("%s: source %q, want /warehouse/workforce-management", ex.Name, evt.Source())
		}
		if evt.DataMediaType() != cloudevents.DataContentType {
			t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
		}
		if evt.DataSchema() != "urn:warehouse:workforce-management:events:ShiftPlanCommitted:v1" {
			t.Errorf("%s: dataschema %q, want the producer's ShiftPlanCommitted v1 urn", ex.Name, evt.DataSchema())
		}
	}
	assertEnvelopeRequired(t, doc, "ShiftPlanCommitted")
}

// E4: a Conformist consumer on a shared topic — unknown ADDITIVE data
// keys (the example already carries site_code, which this DTO does not
// read) must not fail decode or dispatch; an unknown event TYPE is
// silently skipped (nil error, no row); and a message that is not a
// CloudEvent at all is a deterministic ErrNotCloudEvent — the exact
// signal Consumer.dispatch dead-letters once and never retries.
func TestUnknownAttributesTolerated_Workforce(t *testing.T) {
	golden := workforceGoldenExamples(t)[0].Payload

	tolerant := freshGoldenID(t, golden, "tolerated")
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	evt, err := cloudevents.Decode(goldenJSON(t, tolerant))
	if err != nil {
		t.Fatalf("decode with unknown data attributes: %v", err)
	}
	consumer, views := newWorkforceContractFixture()
	if err := consumer.handleWorkforceEvent(context.Background(), evt); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if _, err := views.FindByPathId(context.Background(), mustPathId(t, data["path_id"].(string))); err != nil {
		t.Errorf("unknown data attributes changed the outcome: %v", err)
	}

	otherType := freshGoldenID(t, golden, "othertype")
	otherType["type"] = "com.warehouse.wes.workforce-management.shiftplan.ShiftPlanRevoked"
	evt, err = cloudevents.Decode(goldenJSON(t, otherType))
	if err != nil {
		t.Fatalf("decode unknown-type example: %v", err)
	}
	if err := consumer.handleWorkforceEvent(context.Background(), evt); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	if _, err = cloudevents.Decode(goldenJSON(t, map[string]any{
		"building_id": "wh1", "shift_id": "shift-1", "path_id": "pack",
		"planned_heads": 3, "planned_rate": 50, "planned_hours": 24,
	})); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("flat payload decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}

// assertEnvelopeRequired asserts the pinned spec's envelope schema for
// one message requires the fleet-mandatory CloudEvents attributes.
func assertEnvelopeRequired(t *testing.T, doc *pinnedAsyncAPI, message string) {
	t.Helper()
	required := doc.envelopeRequired(t, doc.message(t, message).Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's %s envelope does not require %q (required: %v)", message, attr, required)
		}
	}
}
