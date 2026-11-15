package kafka

// Event consumer contract tests (E1-E4) for the order-management edge:
// this repo's Consumer reads order-management's shared topic
// warehouse.order-management.events (cloudevents.TopicOrderManagementEvents
// wired into kafka.ReaderConfig in newConsumer) and acts only on
// OrderAllocated and OrderPartiallyAllocated — both share one payload
// shape and one meaning ("these lines are ready to enqueue") in
// handleOrderManagementEvent. The contract is the PINNED copy of
// order-management's apis/asyncapi.yaml
// (testdata/order-management.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/order-management@develop).
//
// Golden fixtures come from the producer spec's OWN examples, including
// the site_id-carrying (ADR-0048) variants.

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

const pinnedOrderSpec = "testdata/order-management.asyncapi.yaml"

// orderContractMessages maps the message names the producer documents on
// the subscribed channel to the CloudEvents types this repo routes on.
var orderContractMessages = map[string]string{
	"OrderAllocatedIntegration":          cloudevents.TypeOrderAllocated,
	"OrderPartiallyAllocatedIntegration": cloudevents.TypeOrderPartiallyAllocated,
}

func newOrderContractFixture() (*Consumer, *memory.WorkUnitRepo) {
	workUnits := memory.NewWorkUnitRepo()
	publisher := events.NewLogPublisher(nil)
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, memory.NewWorkPoolRepo(), publisher, memory.FixedClock{})
	return &Consumer{
		applyOrderAllocated: usecases.NewApplyOrderAllocated(enqueue, memory.NewProcessedEventRepo(), testCatalogue()),
		catalogue:           testCatalogue(),
	}, workUnits
}

func orderGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedOrderSpec)
	msg := doc.message(t, message)
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned order-management spec documents %s without examples", message)
	}
	return msg.Examples
}

// E1: the topic this repo's reader subscribes to is documented by the
// producer's pinned spec, AND the channel's message list documents both
// CloudEvents types handleOrderManagementEvent routes on.
func TestPinnedAsyncAPICoversSubscriptions_OrderManagementEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOrderSpec)
	if _, how := doc.channelMessageNames(t, cloudevents.TopicOrderManagementEvents); how == "" {
		t.Fatalf("no message list found for %s", cloudevents.TopicOrderManagementEvents)
	}
	documented := doc.documentedTypes(t, cloudevents.TopicOrderManagementEvents)
	for _, want := range orderContractMessages {
		if !documented[want] {
			t.Errorf("pinned order-management spec does not document %s on %s (documented: %v)",
				want, cloudevents.TopicOrderManagementEvents, documented)
		}
	}
}

// E2: every OrderAllocated / OrderPartiallyAllocated example the
// producer's pinned spec documents decodes through the REAL
// cloudevents.Decode and routes through the REAL
// handleOrderManagementEvent -> ApplyOrderAllocated -> EnqueueWorkUnit
// path: exactly one work unit per line, keyed order-line, carrying the
// wire's SKU and gift flag; a replayed CloudEvents id enqueues nothing
// more.
func TestGoldenEventsDecode_OrderAllocated(t *testing.T) {
	for message := range orderContractMessages {
		for _, ex := range orderGoldenExamples(t, message) {
			t.Run(ex.Name, func(t *testing.T) {
				runOrderGolden(t, message, ex)
			})
		}
	}
}

// runOrderGolden is one allocation golden example through the REAL
// decode + dispatch path, extracted per-example for the complexity gate.
func runOrderGolden(t *testing.T, message string, ex pinnedExample) {
	t.Helper()
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode golden example: %v", err)
	}
	if evt.Type() != orderContractMessages[message] {
		t.Fatalf("golden example type %q is not the routed type %q", evt.Type(), orderContractMessages[message])
	}
	consumer, workUnits := newOrderContractFixture()
	if err := consumer.handleOrderManagementEvent(context.Background(), evt); err != nil {
		t.Fatalf("handleOrderManagementEvent: %v", err)
	}
	lines := assertOrderEnqueued(t, ex, workUnits)
	if err := consumer.handleOrderManagementEvent(context.Background(), evt); err != nil {
		t.Fatalf("replayed handleOrderManagementEvent: %v", err)
	}
	if after := orderUnitCount(t, ex, workUnits); after != lines {
		t.Errorf("replayed CloudEvents id changed the work-unit set: %d -> %d", lines, after)
	}
}

// assertOrderEnqueued asserts one work unit per line exists, keyed by
// the documented order-line convention, carrying the wire's own facts;
// it returns the line count for the replay comparison.
func assertOrderEnqueued(t *testing.T, ex pinnedExample, workUnits *memory.WorkUnitRepo) int {
	t.Helper()
	data := ex.Payload["data"].(map[string]any)
	orderId := data["order_id"].(string)
	rawLines := data["lines"].([]any)
	for _, raw := range rawLines {
		line := raw.(map[string]any)
		unitId := orderId + "-line-" + strconv.Itoa(goldenInt(t, line["line_no"]))
		unit, err := workUnits.FindById(context.Background(), unitId)
		if err != nil {
			t.Fatalf("no work unit enqueued for %s: %v", unitId, err)
		}
		if sku, _ := line["sku"].(string); unit.SKU() != sku {
			t.Errorf("unit %s sku %q, wire says %q", unitId, unit.SKU(), sku)
		}
		if gift, ok := line["gift_wrap"].(bool); ok && unit.GiftWrap() != gift {
			t.Errorf("unit %s gift_wrap %v, wire says %v", unitId, unit.GiftWrap(), gift)
		}
	}
	return len(rawLines)
}

func orderUnitCount(t *testing.T, ex pinnedExample, workUnits *memory.WorkUnitRepo) int {
	t.Helper()
	data := ex.Payload["data"].(map[string]any)
	orderId := data["order_id"].(string)
	lines := data["lines"].([]any)
	count := 0
	for _, raw := range lines {
		line := raw.(map[string]any)
		unitId := orderId + "-line-" + strconv.Itoa(goldenInt(t, line["line_no"]))
		if _, err := workUnits.FindById(context.Background(), unitId); err == nil {
			count++
		}
	}
	return count
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope and the producer's dataschema urn, and the pinned spec's
// envelope schemas require those attributes.
func TestCloudEventsEnvelopeInvariants_Order(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOrderSpec)
	for message, wantSchema := range map[string]string{
		"OrderAllocatedIntegration":          "urn:warehouse:order-management:events:OrderAllocated:v1",
		"OrderPartiallyAllocatedIntegration": "urn:warehouse:order-management:events:OrderPartiallyAllocated:v1",
	} {
		for _, ex := range orderGoldenExamples(t, message) {
			assertGoldenEnvelope(t, ex, "/warehouse/order-management", wantSchema)
		}
		assertEnvelopeRequired(t, doc, message)
	}
}

// E4: unknown ADDITIVE data keys (the producer's own fulfillment_class
// line field and site_id top-level field are the live examples) must not
// fail decode or dispatch; an unknown event TYPE on the shared topic is
// silently skipped; and a non-CloudEvent message is a deterministic
// ErrNotCloudEvent — the signal Consumer.dispatch dead-letters once and
// never retries.
func TestUnknownAttributesTolerated_Order(t *testing.T) {
	golden := orderGoldenExamples(t, "OrderAllocatedIntegration")[0].Payload

	tolerant := freshGoldenID(t, golden, "tolerated")
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	firstLine := data["lines"].([]any)[0].(map[string]any)
	firstLine["another_future_field"] = true
	evt, err := cloudevents.Decode(goldenJSON(t, tolerant))
	if err != nil {
		t.Fatalf("decode with unknown data attributes: %v", err)
	}
	consumer, workUnits := newOrderContractFixture()
	if err := consumer.handleOrderManagementEvent(context.Background(), evt); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if lines := assertOrderEnqueued(t, pinnedExample{Payload: tolerant}, workUnits); lines == 0 {
		t.Error("unknown data attributes changed the outcome: nothing enqueued")
	}

	otherType := freshGoldenID(t, golden, "othertype")
	otherType["type"] = "com.warehouse.wes.order-management.order.OrderRepromised"
	evt, err = cloudevents.Decode(goldenJSON(t, otherType))
	if err != nil {
		t.Fatalf("decode unknown-type example: %v", err)
	}
	if err := consumer.handleOrderManagementEvent(context.Background(), evt); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	if _, err = cloudevents.Decode(goldenJSON(t, map[string]any{
		"order_id": "ord-flat", "promise_date": "2026-09-09T10:01:00Z", "lines": []any{},
	})); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("flat payload decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
