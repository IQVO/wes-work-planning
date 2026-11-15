package kafka

// Event consumer contract tests (E1-E4) for the inventory-storage edge:
// this repo's Consumer reads inventory-storage's shared topic
// warehouse.inventory.events (cloudevents.TopicInventoryEvents wired
// into kafka.ReaderConfig in newConsumer) and acts only on
// StockReserved (delta -quantity) and ReservationRevoked (delta
// +quantity) — handleInventoryEvent. The contract is the PINNED copy of
// inventory-storage's apis/asyncapi.yaml
// (testdata/inventory-storage.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/inventory-storage@develop).
//
// Golden fixtures come from the producer spec's OWN examples.

import (
	"context"
	"errors"
	"testing"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

const pinnedInventorySpec = "testdata/inventory-storage.asyncapi.yaml"

// inventoryContractMessages maps the message names this producer
// documents on the subscribed channel to the CloudEvents type this
// repo's decoder routes on.
var inventoryContractMessages = map[string]string{
	"StockReserved":      cloudevents.TypeStockReserved,
	"ReservationRevoked": cloudevents.TypeReservationRevoked,
}

func newInventoryContractFixture() (*Consumer, *memory.InventoryViewRepo) {
	views := memory.NewInventoryViewRepo()
	consumer := &Consumer{
		observeInventory: usecases.NewObserveInventoryChange(views, memory.NewProcessedEventRepo()),
	}
	return consumer, views
}

func inventoryGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedInventorySpec)
	msg := doc.message(t, message)
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned inventory-storage spec documents %s without examples", message)
	}
	return msg.Examples
}

// E1: the topic this repo's reader subscribes to is documented by the
// producer's pinned spec, AND the channel's message list documents both
// CloudEvents types handleInventoryEvent routes on. The channel carries
// many other types (Transfer*, Container*, Discrepancy*, ...) — every
// one of them is an unknown type this consumer must skip, which E4
// exercises by type.
func TestPinnedAsyncAPICoversSubscriptions_InventoryEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInventorySpec)
	if _, how := doc.channelMessageNames(t, cloudevents.TopicInventoryEvents); how == "" {
		t.Fatalf("no message list found for %s", cloudevents.TopicInventoryEvents)
	}
	documented := doc.documentedTypes(t, cloudevents.TopicInventoryEvents)
	for _, want := range inventoryContractMessages {
		if !documented[want] {
			t.Errorf("pinned inventory-storage spec does not document %s on %s (documented: %v)",
				want, cloudevents.TopicInventoryEvents, documented)
		}
	}
}

// E2: every StockReserved / ReservationRevoked example the producer's
// pinned spec documents decodes through the REAL cloudevents.Decode and
// routes through the REAL handleInventoryEvent -> ObserveInventoryChange
// path: exactly one inventory-view row per SKU, carrying the wire's
// delta direction, and a replayed CloudEvents id applies nothing more.
func TestGoldenEventsDecode_InventoryReservations(t *testing.T) {
	for message, wantDelta := range map[string]int{
		"StockReserved":      -1,
		"ReservationRevoked": 1,
	} {
		for _, ex := range inventoryGoldenExamples(t, message) {
			t.Run(ex.Name, func(t *testing.T) {
				runInventoryGolden(t, message, wantDelta, ex)
			})
		}
	}
}

// runInventoryGolden is one reservation golden example through the REAL
// decode + dispatch path, extracted per-example for the complexity gate.
func runInventoryGolden(t *testing.T, message string, wantDelta int, ex pinnedExample) {
	t.Helper()
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode golden example: %v", err)
	}
	if evt.Type() != inventoryContractMessages[message] {
		t.Fatalf("golden example type %q is not the routed type %q", evt.Type(), inventoryContractMessages[message])
	}
	consumer, views := newInventoryContractFixture()
	if err := consumer.handleInventoryEvent(context.Background(), evt); err != nil {
		t.Fatalf("handleInventoryEvent: %v", err)
	}
	assertInventoryObserved(t, ex, evt, views, wantDelta)
	before := inventoryUsableQuantity(t, views, ex)
	if err := consumer.handleInventoryEvent(context.Background(), evt); err != nil {
		t.Fatalf("replayed handleInventoryEvent: %v", err)
	}
	if after := inventoryUsableQuantity(t, views, ex); after != before {
		t.Errorf("replayed CloudEvents id changed the view: %d -> %d", before, after)
	}
}

func assertInventoryObserved(t *testing.T, ex pinnedExample, evt ce.Event, views *memory.InventoryViewRepo, direction int) {
	t.Helper()
	data := ex.Payload["data"].(map[string]any)
	sku := data["sku"].(string)
	view, err := views.FindBySKU(context.Background(), sku)
	if err != nil {
		t.Fatalf("no inventory view observed for the golden SKU: %v", err)
	}
	quantity := goldenInt(t, data["quantity"])
	if view.UsableQuantity != direction*quantity {
		t.Errorf("observed usable quantity %d, want delta %d x quantity %d", view.UsableQuantity, direction, quantity)
	}
	if !view.ObservedAt.Equal(evt.Time()) {
		t.Errorf("observed at %v, CloudEvents time is %v", view.ObservedAt, evt.Time())
	}
}

func inventoryUsableQuantity(t *testing.T, views *memory.InventoryViewRepo, ex pinnedExample) int {
	t.Helper()
	sku := ex.Payload["data"].(map[string]any)["sku"].(string)
	view, err := views.FindBySKU(context.Background(), sku)
	if err != nil {
		t.Fatalf("no inventory view for %s: %v", sku, err)
	}
	return view.UsableQuantity
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope and the producer's dataschema urn, and the pinned spec's
// envelope schemas require those attributes.
func TestCloudEventsEnvelopeInvariants_Inventory(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInventorySpec)
	for message, wantSchema := range map[string]string{
		"StockReserved":      "urn:warehouse:inventory-storage:events:StockReserved:v1",
		"ReservationRevoked": "urn:warehouse:inventory-storage:events:ReservationRevoked:v1",
	} {
		for _, ex := range inventoryGoldenExamples(t, message) {
			assertGoldenEnvelope(t, ex, "/warehouse/inventory-storage", wantSchema)
		}
		assertEnvelopeRequired(t, doc, message)
	}
}

// E4: unknown ADDITIVE data keys must not fail decode or dispatch; an
// unknown event TYPE on the shared topic is silently skipped; and a
// non-CloudEvent message is a deterministic ErrNotCloudEvent — the
// signal Consumer.dispatch dead-letters once and never retries.
func TestUnknownAttributesTolerated_Inventory(t *testing.T) {
	golden := inventoryGoldenExamples(t, "StockReserved")[0].Payload

	tolerant := freshGoldenID(t, golden, "tolerated")
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	evt, err := cloudevents.Decode(goldenJSON(t, tolerant))
	if err != nil {
		t.Fatalf("decode with unknown data attributes: %v", err)
	}
	consumer, views := newInventoryContractFixture()
	if err := consumer.handleInventoryEvent(context.Background(), evt); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if _, err := views.FindBySKU(context.Background(), data["sku"].(string)); err != nil {
		t.Errorf("unknown data attributes changed the outcome: %v", err)
	}

	otherType := freshGoldenID(t, golden, "othertype")
	otherType["type"] = "com.warehouse.wms.inventory-storage.stock.StockRelocated"
	evt, err = cloudevents.Decode(goldenJSON(t, otherType))
	if err != nil {
		t.Fatalf("decode unknown-type example: %v", err)
	}
	if err := consumer.handleInventoryEvent(context.Background(), evt); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	if _, err = cloudevents.Decode(goldenJSON(t, map[string]any{
		"sku": "SKU-1", "quantity": 5, "demand_ref": "order-42",
	})); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("flat payload decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
