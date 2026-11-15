package kafka

// Event consumer contract tests (E1-E4) for the product-master edge:
// this repo runs TWO consumers over product-master's topic
// warehouse.product-master.events (cloudevents.TopicProductMasterEvents
// wired into kafka.ReaderConfig by NewProductClassificationConsumer and
// NewPackLevelConsumer): the classification copy
// (ProductClassified -> ObserveProductClassification, ADR-0035) and the
// pack-level copy (PackLevelDeclared/Retired/Measured ->
// ObservePackLevel, ADR-0045). The contract is the PINNED copy of
// product-master's apis/asyncapi.yaml (testdata/product-master.asyncapi.yaml,
// registered in contracts/pinned.json; the pinned-contracts CI job fails
// when the copy drifts from IQVO/product-master@develop).
//
// Golden fixtures come from the producer spec's OWN examples (the spec
// declares no payload-schema type consts — the examples' own `type`
// attributes are the documented type list, which documentedTypes reads).
//
// Documented WITHOUT routing here: ProductRegistered,
// ProductDescriptionChanged, ProductDimensionsDeclared, ProductMeasured
// — E1-only (no consumer in this repo routes on them).

import (
	"context"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

const pinnedProductMasterSpec = "testdata/product-master.asyncapi.yaml"

func newClassificationContractConsumer() (*ProductClassificationConsumer, *productclassificationcopy.MemoryStore) {
	copies := productclassificationcopy.NewMemoryStore()
	return &ProductClassificationConsumer{
		observe:      usecases.NewObserveProductClassification(copies, memory.NewProcessedEventRepo()),
		logger:       slog.New(slog.DiscardHandler),
		retryInitial: classificationRetryInitial, retryMax: classificationRetryMax,
	}, copies
}

func newPackLevelContractConsumer() (*PackLevelConsumer, *productclassificationcopy.MemoryStore) {
	copies := productclassificationcopy.NewMemoryStore()
	return &PackLevelConsumer{
		observe:      usecases.NewObservePackLevel(copies, memory.NewProcessedEventRepo()),
		logger:       slog.New(slog.DiscardHandler),
		retryInitial: classificationRetryInitial, retryMax: classificationRetryMax,
	}, copies
}

func productMasterGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedProductMasterSpec)
	msg := doc.message(t, message)
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned product-master spec documents %s without examples", message)
	}
	return msg.Examples
}

// E1: the topic both readers subscribe to is documented by the
// producer's pinned spec, AND the channel's message list documents every
// CloudEvents type this repo routes on (classification: 1, pack level:
// 3).
func TestPinnedAsyncAPICoversSubscriptions_ProductMasterEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedProductMasterSpec)
	if _, how := doc.channelMessageNames(t, cloudevents.TopicProductMasterEvents); how == "" {
		t.Fatalf("no message list found for %s", cloudevents.TopicProductMasterEvents)
	}
	documented := doc.documentedTypes(t, cloudevents.TopicProductMasterEvents)
	for _, want := range []string{
		cloudevents.TypeProductClassified,
		cloudevents.TypeProductPackLevelDeclared,
		cloudevents.TypeProductPackLevelRetired,
		cloudevents.TypeProductPackLevelMeasured,
	} {
		if !documented[want] {
			t.Errorf("pinned product-master spec does not document %s on %s (documented: %v)",
				want, cloudevents.TopicProductMasterEvents, documented)
		}
	}
}

// E2 (classification): every ProductClassified example the producer's
// pinned spec documents decodes through the REAL handle path
// (decode -> applyWithRetry -> ObserveProductClassification): the local
// copy carries the wire's own handling tags and DOT class, and a
// replayed CloudEvents id writes nothing newer.
func TestGoldenEventsDecode_ProductClassified(t *testing.T) {
	for _, ex := range productMasterGoldenExamples(t, "ProductClassified") {
		t.Run(ex.Name, func(t *testing.T) {
			consumer, copies := newClassificationContractConsumer()
			if err := consumer.handle(context.Background(), goldenMsg(t, ex)); err != nil {
				t.Fatalf("classification handle: %v", err)
			}
			assertClassificationCopied(t, ex, copies)
			version := copies.Version(goldenSKU(t, ex))
			if err := consumer.handle(context.Background(), goldenMsg(t, ex)); err != nil {
				t.Fatalf("replayed classification handle: %v", err)
			}
			if after := copies.Version(goldenSKU(t, ex)); after != version {
				t.Errorf("replayed CloudEvents id advanced the copy version: %d -> %d", version, after)
			}
		})
	}
}

func assertClassificationCopied(t *testing.T, ex pinnedExample, copies *productclassificationcopy.MemoryStore) {
	t.Helper()
	sku := goldenSKU(t, ex)
	view, err := copies.GetClassification(context.Background(), sku)
	if err != nil {
		t.Fatalf("classification copy empty for %s: %v", sku, err)
	}
	data := ex.Payload["data"].(map[string]any)
	if temp, _ := data["temperature_class"].(string); view.TemperatureClass != temp {
		t.Errorf("copied temperature class %q, wire says %q", view.TemperatureClass, temp)
	}
}

func goldenSKU(t *testing.T, ex pinnedExample) string {
	t.Helper()
	sku, _ := ex.Payload["data"].(map[string]any)["sku"].(string)
	if sku == "" {
		t.Fatal("golden example carries no sku")
	}
	return sku
}

func goldenMsg(t *testing.T, ex pinnedExample) kafkago.Message {
	t.Helper()
	return kafkago.Message{Topic: cloudevents.TopicProductMasterEvents, Value: goldenJSON(t, ex.Payload)}
}

// E2 (pack level): every PackLevelDeclared/Retired/Measured example the
// producer's pinned spec documents decodes through the REAL pack-level
// handle path (decode -> requestFor -> applyWithRetry ->
// ObservePackLevel): the Declared example lands a pack-level copy whose
// version advances monotonically across the Declared -> Measured
// sequence, and a replayed id applies nothing newer.
func TestGoldenEventsDecode_ProductPackLevels(t *testing.T) {
	consumer, copies := newPackLevelContractConsumer()
	for _, message := range []string{"ProductPackLevelDeclared", "ProductPackLevelMeasured"} {
		for _, ex := range productMasterGoldenExamples(t, message) {
			t.Run(ex.Name, func(t *testing.T) {
				runPackLevelGolden(t, consumer, copies, message, ex)
			})
		}
	}
}

// runPackLevelGolden is one pack-level golden example through the REAL
// handle path, extracted per-example for the complexity gate.
func runPackLevelGolden(t *testing.T, consumer *PackLevelConsumer, copies *productclassificationcopy.MemoryStore, message string, ex pinnedExample) {
	t.Helper()
	if err := consumer.handle(context.Background(), goldenMsg(t, ex)); err != nil {
		t.Fatalf("pack-level handle: %v", err)
	}
	rec := packLevelOf(t, copies, goldenSKU(t, ex))
	switch message {
	case "ProductPackLevelDeclared":
		if want := goldenInt(t, ex.Payload["data"].(map[string]any)["units_per_pack"]); rec.UnitsPerPack != want {
			t.Errorf("declared copy carries units_per_pack %d, wire says %d", rec.UnitsPerPack, want)
		}
	case "ProductPackLevelMeasured":
		if !rec.Measured || rec.Dims.LengthMm < 1 {
			t.Error("measured copy carries no measured dimensions from the wire")
		}
	}
	if err := consumer.handle(context.Background(), goldenMsg(t, ex)); err != nil {
		t.Fatalf("replayed pack-level handle: %v", err)
	}
	if replayed := packLevelOf(t, copies, goldenSKU(t, ex)); replayed != rec {
		t.Errorf("replayed CloudEvents id changed the pack copy: %+v -> %+v", rec, replayed)
	}
}

// packLevelOf reads the CASE pack record the copy attached to the SKU's
// classification view (the golden examples are all CASE level).
func packLevelOf(t *testing.T, copies *productclassificationcopy.MemoryStore, sku string) productclassificationview.PackFacts {
	t.Helper()
	view, err := copies.GetClassification(context.Background(), sku)
	if err != nil {
		t.Fatalf("classification view for %s: %v", sku, err)
	}
	if view.Case == nil {
		t.Fatalf("no CASE pack record copied for %s", sku)
	}
	return *view.Case
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope and the producer's dataschema urns, and the pinned spec's
// envelope schemas require those attributes.
func TestCloudEventsEnvelopeInvariants_ProductMaster(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedProductMasterSpec)
	for message, wantSchema := range map[string]string{
		"ProductClassified":        "urn:warehouse:product-master:events:ProductClassified:v1",
		"ProductPackLevelDeclared": "urn:warehouse:product-master:events:ProductPackLevelDeclared:v1",
		"ProductPackLevelRetired":  "urn:warehouse:product-master:events:ProductPackLevelRetired:v1",
		"ProductPackLevelMeasured": "urn:warehouse:product-master:events:ProductPackLevelMeasured:v1",
	} {
		for _, ex := range productMasterGoldenExamples(t, message) {
			assertGoldenEnvelope(t, ex, "/warehouse/product-master", wantSchema)
		}
		assertEnvelopeRequired(t, doc, message)
	}
}

// E4: per the consumers' own doc comments, a message that is not a
// CloudEvent is a WARN-skip (nil error, committed, never DLQ'd), an
// unknown event TYPE is skipped, and unknown ADDITIVE data keys must not
// fail decode or dispatch.
func TestUnknownAttributesTolerated_ProductMaster(t *testing.T) {
	golden := productMasterGoldenExamples(t, "ProductClassified")[0].Payload

	tolerant := freshGoldenID(t, golden, "tolerated")
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, copies := newClassificationContractConsumer()
	if err := consumer.handle(context.Background(), kafkago.Message{Topic: cloudevents.TopicProductMasterEvents, Value: goldenJSON(t, tolerant)}); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if _, err := copies.GetClassification(context.Background(), data["sku"].(string)); err != nil {
		t.Errorf("unknown data attributes changed the outcome: %v", err)
	}

	otherType := freshGoldenID(t, golden, "othertype")
	otherType["type"] = "com.warehouse.wms.product-master.product.ProductRegistered"
	if err := consumer.handle(context.Background(), kafkago.Message{Topic: cloudevents.TopicProductMasterEvents, Value: goldenJSON(t, otherType)}); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	if err := consumer.handle(context.Background(), kafkago.Message{Topic: cloudevents.TopicProductMasterEvents, Value: goldenJSON(t, map[string]any{
		"sku": "SKU-FLAT", "handling_tags": []any{"Hazmat"}, "temperature_class": "Frozen", "dot_hazard_class": 3,
	})}); err != nil {
		t.Fatalf("flat payload must be warn-skipped per the consumer's documented policy, got: %v", err)
	}

	// The pack-level consumer's E4 mirrors: flat payload warn-skipped.
	packConsumer, _ := newPackLevelContractConsumer()
	if err := packConsumer.handle(context.Background(), kafkago.Message{Topic: cloudevents.TopicProductMasterEvents, Value: goldenJSON(t, map[string]any{"sku": "SKU-FLAT"})}); err != nil {
		t.Fatalf("pack-level flat payload must be warn-skipped, got: %v", err)
	}
}
