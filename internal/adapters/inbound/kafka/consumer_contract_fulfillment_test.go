package kafka

// Event consumer contract tests (E1-E4) for the fulfillment-execution
// edge: this repo's Consumer reads fulfillment-execution's shared,
// fan-out topic warehouse.fulfillment.events (the fulfillmentTopic
// argument newConsumer wires into kafka.ReaderConfig, defaulted to
// cloudevents.TopicFulfillmentEvents) and acts only on
// com.warehouse.wes.fulfillment-execution.task.TaskCompleted
// (handleFulfillmentEvent). The contract is the PINNED copy of
// fulfillment-execution's apis/asyncapi.yaml
// (testdata/fulfillment-execution.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/fulfillment-execution@develop).
//
// Golden fixtures come from the producer spec's OWN examples — including
// the RELOCATE and REPLENISH task examples whose work units this context
// itself planned (slot-plan-7-move-3, replenish-req-9): each is run
// against a released work unit under that exact id, and a TaskCompleted
// for a work unit this context never planned is the documented
// INFO-logged skip, asserted here too.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

const pinnedFulfillmentSpec = "testdata/fulfillment-execution.asyncapi.yaml"

func newFulfillmentContractFixture(t *testing.T, workUnitId string) (*Consumer, *memory.WorkUnitRepo) {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	processed := memory.NewProcessedEventRepo()

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)
	release := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock)
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("path id: %v", err)
	}
	cpt := shared.NewCPT(clock.At.Add(2 * time.Hour))
	if _, err := enqueue.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{WorkUnitId: workUnitId, PathId: pathId, CPT: cpt, Reference: "contract-fixture"}); err != nil {
		t.Fatalf("enqueue released unit: %v", err)
	}
	if _, err := release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("release unit: %v", err)
	}

	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, publisher, clock)
	return &Consumer{
		applyTaskCompleted: usecases.NewApplyTaskCompleted(recordCompletion, processed),
		catalogue:          testCatalogue(),
	}, workUnits
}

func fulfillmentGoldenExamples(t *testing.T) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	msg := doc.message(t, "TaskCompleted")
	if len(msg.Examples) == 0 {
		t.Fatal("pinned fulfillment-execution spec documents TaskCompleted without examples")
	}
	return msg.Examples
}

// E1: the topic this repo's reader subscribes to is documented by the
// producer's pinned spec, AND the channel's message list documents the
// exact CloudEvents type handleFulfillmentEvent routes on.
func TestPinnedAsyncAPICoversSubscriptions_FulfillmentEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	if _, how := doc.channelMessageNames(t, cloudevents.TopicFulfillmentEvents); how == "" {
		t.Fatalf("no message list found for %s", cloudevents.TopicFulfillmentEvents)
	}
	documented := doc.documentedTypes(t, cloudevents.TopicFulfillmentEvents)
	if !documented[cloudevents.TypeTaskCompleted] {
		t.Errorf("pinned fulfillment-execution spec does not document %s on %s (documented: %v)",
			cloudevents.TypeTaskCompleted, cloudevents.TopicFulfillmentEvents, documented)
	}
}

// E2: every TaskCompleted example the producer's pinned spec documents
// decodes through the REAL cloudevents.Decode and routes through the
// REAL handleFulfillmentEvent -> ApplyTaskCompleted -> RecordCompletion
// path: the named work unit ends Completed, and a replayed CloudEvents
// id completes nothing further (same state, never AlreadyCompleted).
func TestGoldenEventsDecode_FulfillmentTaskCompleted(t *testing.T) {
	for _, ex := range fulfillmentGoldenExamples(t) {
		t.Run(ex.Name, func(t *testing.T) {
			runFulfillmentGolden(t, ex)
		})
	}
}

// runFulfillmentGolden is one TaskCompleted golden example through the
// REAL decode + dispatch path, extracted per-example for the
// complexity gate.
func runFulfillmentGolden(t *testing.T, ex pinnedExample) {
	t.Helper()
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode golden example: %v", err)
	}
	if evt.Type() != cloudevents.TypeTaskCompleted {
		t.Fatalf("golden example type %q is not the routed type", evt.Type())
	}
	workUnitId := ex.Payload["data"].(map[string]any)["work_unit_id"].(string)
	consumer, workUnits := newFulfillmentContractFixture(t, workUnitId)
	if err := consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}
	unit, err := workUnits.FindById(context.Background(), workUnitId)
	if err != nil {
		t.Fatalf("work unit %s vanished: %v", workUnitId, err)
	}
	if unit.State() != workunit.Completed {
		t.Fatalf("golden TaskCompleted left the work unit %v, want Completed", unit.State())
	}
	if err := consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("replayed handleFulfillmentEvent: %v", err)
	}
	unit, err = workUnits.FindById(context.Background(), workUnitId)
	if err != nil {
		t.Fatalf("replayed work unit lookup: %v", err)
	}
	if unit.State() != workunit.Completed {
		t.Errorf("replayed CloudEvents id changed the outcome: unit is %v", unit.State())
	}
}

// E2b: a TaskCompleted naming a work unit this context never planned is
// the documented INFO-logged skip (nil error, still marked processed) —
// the shared-topic fact the handler's own doc comment spells out.
func TestGoldenEventsDecode_FulfillmentUnknownWorkUnitSkips(t *testing.T) {
	ex := fulfillmentGoldenExamples(t)[0]
	payload := freshGoldenID(t, ex.Payload, "unknown-wu")
	data := payload["data"].(map[string]any)
	data["work_unit_id"] = "a-unit-this-context-never-planned"
	evt, err := cloudevents.Decode(goldenJSON(t, payload))
	if err != nil {
		t.Fatalf("decode golden example: %v", err)
	}
	consumer, _ := newFulfillmentContractFixture(t, "wu-1")
	if err := consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("unknown work unit must be skipped, not an error: %v", err)
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope and the producer's dataschema urn, and the pinned spec's
// envelope schema requires those attributes.
func TestCloudEventsEnvelopeInvariants_Fulfillment(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	for _, ex := range fulfillmentGoldenExamples(t) {
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
		if evt.Source() != "/warehouse/fulfillment-execution" {
			t.Errorf("%s: source %q, want /warehouse/fulfillment-execution", ex.Name, evt.Source())
		}
		if evt.DataMediaType() != cloudevents.DataContentType {
			t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
		}
		if evt.DataSchema() != "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1" {
			t.Errorf("%s: dataschema %q, want the producer's TaskCompleted v1 urn", ex.Name, evt.DataSchema())
		}
	}
	assertEnvelopeRequired(t, doc, "TaskCompleted")
}

// E4: a Conformist consumer on a shared, fan-out topic — unknown
// ADDITIVE data keys (the producer's own relocate/replenish enrichment
// fields are the live example) must not fail decode or dispatch; an
// unknown event TYPE is silently skipped; and a message that is not a
// CloudEvent at all is a deterministic ErrNotCloudEvent — the exact
// signal Consumer.dispatch dead-letters once and never retries.
func TestUnknownAttributesTolerated_Fulfillment(t *testing.T) {
	golden := fulfillmentGoldenExamples(t)[0].Payload
	workUnitId := golden["data"].(map[string]any)["work_unit_id"].(string)

	tolerant := freshGoldenID(t, golden, "tolerated")
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	evt, err := cloudevents.Decode(goldenJSON(t, tolerant))
	if err != nil {
		t.Fatalf("decode with unknown data attributes: %v", err)
	}
	consumer, workUnits := newFulfillmentContractFixture(t, workUnitId)
	if err := consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	unit, err := workUnits.FindById(context.Background(), workUnitId)
	if err != nil || unit.State() != workunit.Completed {
		t.Errorf("unknown data attributes changed the outcome: unit %v err %v", unit.State(), err)
	}

	otherType := freshGoldenID(t, golden, "othertype")
	otherType["type"] = "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"
	evt, err = cloudevents.Decode(goldenJSON(t, otherType))
	if err != nil {
		t.Fatalf("decode unknown-type example: %v", err)
	}
	if err := consumer.handleFulfillmentEvent(context.Background(), evt); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	if _, err = cloudevents.Decode(goldenJSON(t, map[string]any{
		"task_id": "task-legacy", "station_id": "station-1", "work_unit_id": workUnitId,
	})); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
