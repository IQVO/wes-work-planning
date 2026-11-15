package kafka

// Event consumer contract tests (E1-E4) for the network-inventory-
// planning edge: this repo's Consumer reads network-inventory-planning's
// topic warehouse.network-inventory-planning.events
// (cloudevents.TopicNetworkDemandEvents wired into kafka.ReaderConfig in
// newConsumer) and acts only on
// com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased
// (handleNetworkDemandEvent -> ApplyWorkDemandReleased, ADR-0033). The
// contract is the PINNED copy of network-inventory-planning's
// apis/asyncapi.yaml (testdata/network-inventory-planning.asyncapi.yaml,
// registered in contracts/pinned.json; the pinned-contracts CI job fails
// when the copy drifts from IQVO/network-inventory-planning@develop).
//
// FIXTURE PROVENANCE: the producer's pinned spec documents
// workDemandReleased WITHOUT examples (a producer example backfill is
// pending), so the golden fixture here is DERIVED FROM THE PINNED
// PAYLOAD SCHEMA (WorkDemandReleasedEvent: every data field required,
// work_kind enum TRANSFER_PICK|TRANSFER_DISPATCH|TRANSFER_ARRIVAL) and
// cross-checked against this repo's existing integration-test publisher
// (consumer_work_demand_integration_test.go, whose payload the producer
// itself mirrored when it wrote the schema). E2 runs that fixture
// through the REAL decode + dispatch path.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

const pinnedNetworkDemandSpec = "testdata/network-inventory-planning.asyncapi.yaml"

func newNetworkDemandContractFixture() (*Consumer, *memory.WorkUnitRepo) {
	workUnits := memory.NewWorkUnitRepo()
	publisher := events.NewLogPublisher(nil)
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, memory.NewWorkPoolRepo(), publisher, memory.FixedClock{})
	return &Consumer{
		applyWorkDemandReleased: usecases.NewApplyWorkDemandReleased(enqueue, memory.NewProcessedEventRepo(), testCatalogue()),
	}, workUnits
}

// networkDemandGoldenFixture derives one full wire document from the
// pinned payload schema: every field the schema REQUIRES is present with
// a schema-legal value; nothing else is invented. The id is minted fresh
// per call (the id is the idempotency key).
func networkDemandGoldenFixture(t *testing.T, suffix string) map[string]any {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedNetworkDemandSpec)
	msg := doc.message(t, "workDemandReleased")
	data := map[string]any{
		"demand_id":    "transfer-42:pick-" + suffix,
		"work_kind":    "TRANSFER_PICK",
		"transfer_ref": "transfer-42",
		"path_id":      "pick-a",
		"site_id":      "SITE-9",
		"cpt":          "2026-11-20T18:00:00Z",
		"sku":          "SKU-NET-1",
		"quantity":     12,
	}
	schemaData := schemaRequiredFor(t, doc, msg.Payload, "data")
	for field := range data {
		if !schemaData[field] {
			t.Errorf("fixture carries %q, which the pinned schema's required data list does not name", field)
		}
	}
	for field := range schemaData {
		if _, ok := data[field]; !ok {
			t.Errorf("pinned schema requires data.%q, fixture must carry it", field)
		}
	}
	return map[string]any{
		"specversion":     "1.0",
		"id":              "nip-golden-" + suffix,
		"source":          "/warehouse/network-inventory-planning",
		"type":            cloudevents.TypeWorkDemandReleased,
		"subject":         data["demand_id"],
		"time":            "2026-11-15T09:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:network-inventory-planning:events:WorkDemandReleased:v1",
		"data":            data,
	}
}

// schemaRequiredFor returns the required property names of one property
// of the payload's envelope schema, resolving payload $refs and walking
// allOf overlays (where this producer keeps the data object).
func schemaRequiredFor(t *testing.T, doc *pinnedAsyncAPI, payload any, prop string) map[string]bool {
	t.Helper()
	required := map[string]bool{}
	found := false
	var walk func(node map[string]any, resolving map[string]bool)
	walk = func(node map[string]any, resolving map[string]bool) {
		if schema, ok := doc.resolveSchemaRef(t, node, resolving); ok {
			walk(schema.(map[string]any), resolving)
			return
		}
		if props, ok := node["properties"].(map[string]any); ok {
			if nested, ok := props[prop].(map[string]any); ok {
				found = true
				for _, r := range requiredList(nested) {
					required[r] = true
				}
			}
		}
		for _, item := range allOfItems(node) {
			if m, ok := item.(map[string]any); ok {
				walk(m, resolving)
			}
		}
	}
	v, ok := payload.(map[string]any)
	if !ok {
		t.Fatalf("payload schema is not a map")
	}
	walk(v, map[string]bool{})
	if !found {
		t.Fatalf("pinned schema declares no object under data.properties.%q", prop)
	}
	return required
}

func TestPinnedAsyncAPICoversSubscriptions_NetworkDemandEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedNetworkDemandSpec)
	// This producer documents the channel with a channel-level message
	// map (no subscribe/publish operations) — the message list is the
	// contract either way.
	names, _ := doc.channelMessageNames(t, cloudevents.TopicNetworkDemandEvents)
	found := false
	for _, name := range names {
		msg := doc.message(t, name) // fails the test on a dangling $ref
		if c := doc.payloadConsts(t, msg.Payload)["type"]; c == cloudevents.TypeWorkDemandReleased {
			found = true
		}
	}
	if !found {
		t.Errorf("pinned network-inventory-planning spec does not document %s on %s (messages: %v)",
			cloudevents.TypeWorkDemandReleased, cloudevents.TopicNetworkDemandEvents, names)
	}
}

// E2: the schema-derived golden fixture decodes through the REAL
// cloudevents.Decode and routes through the REAL
// handleNetworkDemandEvent -> ApplyWorkDemandReleased -> EnqueueWorkUnit
// path: exactly one transfer work unit minted under demand_id, and a
// replayed CloudEvents id (or a different event under the same
// demand_id) enqueues nothing more.
func TestGoldenEventsDecode_NetworkWorkDemandReleased(t *testing.T) {
	fixture := networkDemandGoldenFixture(t, "1")
	evt, err := cloudevents.Decode(goldenJSON(t, fixture))
	if err != nil {
		t.Fatalf("decode schema-derived fixture: %v", err)
	}
	if evt.Type() != cloudevents.TypeWorkDemandReleased {
		t.Fatalf("fixture type %q is not the routed type", evt.Type())
	}
	consumer, workUnits := newNetworkDemandContractFixture()
	if err := consumer.handleNetworkDemandEvent(context.Background(), evt); err != nil {
		t.Fatalf("handleNetworkDemandEvent: %v", err)
	}
	demandId := fixture["data"].(map[string]any)["demand_id"].(string)
	unit, err := workUnits.FindById(context.Background(), demandId)
	if err != nil {
		t.Fatalf("no transfer work unit minted under the demand id: %v", err)
	}
	if got := unit.Reference(); got != demandId {
		t.Errorf("unit reference %q, want the wire's demand_id %q", got, demandId)
	}
	if err := consumer.handleNetworkDemandEvent(context.Background(), evt); err != nil {
		t.Fatalf("replayed handleNetworkDemandEvent: %v", err)
	}
	unit2, err := workUnits.FindById(context.Background(), demandId)
	if err != nil || unit2.State() != unit.State() {
		t.Errorf("replayed CloudEvents id changed the outcome (state %v -> %v, err %v)", unit.State(), unit2.State(), err)
	}
}

// E3: the pinned payload schema pins the envelope facts E3 asserts on
// (type/dataschema consts on the allOf envelope), and the schema-derived
// fixture satisfies the fleet-mandatory CloudEvents 1.0 invariants.
func TestCloudEventsEnvelopeInvariants_NetworkDemand(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedNetworkDemandSpec)
	consts := doc.payloadConsts(t, doc.message(t, "workDemandReleased").Payload)
	if consts["type"] != cloudevents.TypeWorkDemandReleased {
		t.Errorf("pinned schema type const %q, want %q", consts["type"], cloudevents.TypeWorkDemandReleased)
	}
	if consts["dataschema"] != "urn:warehouse:network-inventory-planning:events:WorkDemandReleased:v1" {
		t.Errorf("pinned schema dataschema const %q", consts["dataschema"])
	}
	if consts["source"] != "/warehouse/network-inventory-planning" {
		t.Errorf("pinned schema source const %q", consts["source"])
	}
	assertEnvelopeRequired(t, doc, "workDemandReleased")

	evt, err := cloudevents.Decode(goldenJSON(t, networkDemandGoldenFixture(t, "e3")))
	if err != nil {
		t.Fatalf("decode schema-derived fixture: %v", err)
	}
	if evt.SpecVersion() != cloudevents.SpecVersion || evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("fixture envelope specversion/datacontenttype %q/%q", evt.SpecVersion(), evt.DataMediaType())
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Time().IsZero() {
		t.Errorf("fixture id/subject/time must all be present")
	}
}

// E4: unknown ADDITIVE data keys must not fail decode or dispatch; an
// unknown event TYPE on the topic is silently skipped; and a
// non-CloudEvent message is a deterministic ErrNotCloudEvent — the
// signal Consumer.dispatch dead-letters once and never retries.
func TestUnknownAttributesTolerated_NetworkDemand(t *testing.T) {
	golden := networkDemandGoldenFixture(t, "e4")

	tolerant := clonePayload(golden)
	tolerant["id"] = "nip-tolerated"
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	evt, err := cloudevents.Decode(goldenJSON(t, tolerant))
	if err != nil {
		t.Fatalf("decode with unknown data attributes: %v", err)
	}
	consumer, workUnits := newNetworkDemandContractFixture()
	if err := consumer.handleNetworkDemandEvent(context.Background(), evt); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if _, err := workUnits.FindById(context.Background(), data["demand_id"].(string)); err != nil {
		t.Errorf("unknown data attributes changed the outcome: %v", err)
	}

	otherType := clonePayload(golden)
	otherType["id"] = "nip-othertype"
	otherType["type"] = "com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved"
	evt, err = cloudevents.Decode(goldenJSON(t, otherType))
	if err != nil {
		t.Fatalf("decode unknown-type example: %v", err)
	}
	if err := consumer.handleNetworkDemandEvent(context.Background(), evt); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	if _, err = cloudevents.Decode(goldenJSON(t, map[string]any{
		"demand_id": "transfer-flat:pick", "work_kind": "TRANSFER_PICK", "path_id": "pick-a",
		"transfer_ref": "transfer-flat", "site_id": "SITE-9", "cpt": time.Now().UTC().Format(time.RFC3339),
		"sku": "SKU-1", "quantity": 1,
	})); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("flat payload decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
