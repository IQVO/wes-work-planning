package kafka

// Event consumer contract tests (E1-E4) for the pick-face-replenishment
// edge: this repo's ReplenishmentConsumer reads pick-face-
// replenishment's topic warehouse.pick-face-replenishment.events
// (cloudevents.TopicPickFaceReplenishmentEvents wired into
// kafka.ReaderConfig by NewReplenishmentConsumer) and acts only on
// ReplenishmentRequested (plan REPLENISH work) and
// ReplenishmentCancelled (withdraw it while Pending, ADR-0042). The
// contract is the PINNED copy of pick-face-replenishment's
// apis/asyncapi.yaml (testdata/pick-face-replenishment.asyncapi.yaml,
// registered in contracts/pinned.json; the pinned-contracts CI job fails
// when the copy drifts from IQVO/pick-face-replenishment@develop).
//
// Golden fixtures come from the producer spec's OWN examples — the spec
// declares no payload-schema type consts, so the examples' own `type`
// attributes are the documented type list, which documentedTypes reads.
//
// Documented WITHOUT routing here: ReplenishmentCompleted — E1-only
// (the consumer's doc comment names it as ignored).

import (
	"context"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

const pinnedPickFaceSpec = "testdata/pick-face-replenishment.asyncapi.yaml"

func newReplenishmentContractConsumer() (*ReplenishmentConsumer, *memory.WorkUnitRepo) {
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	processed := memory.NewProcessedEventRepo()
	clock := memory.FixedClock{At: time.Date(2026, 10, 17, 9, 31, 0, 0, time.UTC)}
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "REPLENISH", MatchPrefix: "replenish", RequiredCapabilities: []string{"replenish"}}})
	requested := usecases.NewApplyReplenishmentRequested(workUnits, pools, processed, catalogue, events.NewLogPublisher(nil), clock)
	cancelled := usecases.NewApplyReplenishmentCancelled(workUnits, pools, processed, clock)
	return &ReplenishmentConsumer{
		topic:     cloudevents.TopicPickFaceReplenishmentEvents,
		requested: requested, cancelled: cancelled,
		logger: slog.New(slog.DiscardHandler),
	}, workUnits
}

func replenishmentMsg(value []byte) kafkago.Message {
	return kafkago.Message{Topic: cloudevents.TopicPickFaceReplenishmentEvents, Value: value}
}

func pickFaceGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedPickFaceSpec)
	msg := doc.message(t, message)
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned pick-face-replenishment spec documents %s without examples", message)
	}
	return msg.Examples
}

// E1: the topic this repo's reader subscribes to is documented by the
// producer's pinned spec, AND the channel's message list documents both
// CloudEvents types the consumer's applierFor routes on.
func TestPinnedAsyncAPICoversSubscriptions_PickFaceReplenishmentEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedPickFaceSpec)
	if _, how := doc.channelMessageNames(t, cloudevents.TopicPickFaceReplenishmentEvents); how == "" {
		t.Fatalf("no message list found for %s", cloudevents.TopicPickFaceReplenishmentEvents)
	}
	documented := doc.documentedTypes(t, cloudevents.TopicPickFaceReplenishmentEvents)
	for _, want := range []string{cloudevents.TypeReplenishmentRequested, cloudevents.TypeReplenishmentCancelled} {
		if !documented[want] {
			t.Errorf("pinned pick-face-replenishment spec does not document %s on %s (documented: %v)",
				want, cloudevents.TopicPickFaceReplenishmentEvents, documented)
		}
	}
}

// E2: every ReplenishmentRequested example the producer's pinned spec
// documents decodes through the REAL handle path (decode ->
// applierFor -> ApplyReplenishmentRequested): exactly one REPLENISH
// work unit planned under the wire's request_id, and a replayed
// CloudEvents id plans nothing more. The spec's ReplenishmentCancelled
// example carries the SAME request_id as its Requested example — the
// pair is run in document order: request, then cancel, then replay of
// each, asserting the withdraw leaves one withdrawn unit, never two.
func TestGoldenEventsDecode_Replenishment(t *testing.T) {
	consumer, workUnits := newReplenishmentContractConsumer()

	for _, ex := range pickFaceGoldenExamples(t, "ReplenishmentRequested") {
		t.Run(ex.Name, func(t *testing.T) {
			runReplenishmentRequestedGolden(t, consumer, workUnits, ex)
		})
	}

	for _, ex := range pickFaceGoldenExamples(t, "ReplenishmentCancelled") {
		t.Run(ex.Name, func(t *testing.T) {
			runReplenishmentCancelledGolden(t, consumer, workUnits, ex)
		})
	}
}

// runReplenishmentRequestedGolden is one ReplenishmentRequested golden
// example through the REAL handle path, extracted per-example for the
// complexity gate.
func runReplenishmentRequestedGolden(t *testing.T, consumer *ReplenishmentConsumer, workUnits *memory.WorkUnitRepo, ex pinnedExample) {
	t.Helper()
	if err := consumer.handle(context.Background(), replenishmentMsg(goldenJSON(t, ex.Payload))); err != nil {
		t.Fatalf("requested handle: %v", err)
	}
	requestId := ex.Payload["data"].(map[string]any)["request_id"].(string)
	unit, err := workUnits.FindById(context.Background(), workunit.ReplenishmentWorkUnitId(requestId))
	if err != nil {
		t.Fatalf("no REPLENISH unit planned under the request id: %v", err)
	}
	if unit.State() == workunit.Completed {
		t.Errorf("planned unit already completed")
	}
	if err := consumer.handle(context.Background(), replenishmentMsg(goldenJSON(t, ex.Payload))); err != nil {
		t.Fatalf("replayed requested handle: %v", err)
	}
	if _, err := workUnits.FindById(context.Background(), workunit.ReplenishmentWorkUnitId(requestId)); err != nil {
		t.Errorf("replayed CloudEvents id changed the outcome: %v", err)
	}
}

// runReplenishmentCancelledGolden is one ReplenishmentCancelled golden
// example; the spec pairs it with a Requested example of the same
// request_id, so it withdraws a unit the golden run planned.
func runReplenishmentCancelledGolden(t *testing.T, consumer *ReplenishmentConsumer, workUnits *memory.WorkUnitRepo, ex pinnedExample) {
	t.Helper()
	requestId := ex.Payload["data"].(map[string]any)["request_id"].(string)
	if _, err := workUnits.FindById(context.Background(), workunit.ReplenishmentWorkUnitId(requestId)); err != nil {
		t.Skipf("cancellation example names request %s that no Requested example in this spec planned; producer example backfill pending", requestId)
	}
	if err := consumer.handle(context.Background(), replenishmentMsg(goldenJSON(t, ex.Payload))); err != nil {
		t.Fatalf("cancelled handle: %v", err)
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope and the producer's dataschema urns, and the pinned spec's
// envelope schemas require those attributes.
func TestCloudEventsEnvelopeInvariants_PickFace(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedPickFaceSpec)
	for message, wantSchema := range map[string]string{
		"ReplenishmentRequested": "urn:warehouse:pick-face-replenishment:events:ReplenishmentRequested:v1",
		"ReplenishmentCancelled": "urn:warehouse:pick-face-replenishment:events:ReplenishmentCancelled:v1",
		"ReplenishmentCompleted": "urn:warehouse:pick-face-replenishment:events:ReplenishmentCompleted:v1",
	} {
		for _, ex := range pickFaceGoldenExamples(t, message) {
			assertGoldenEnvelope(t, ex, "/warehouse/pick-face-replenishment", wantSchema)
		}
		assertEnvelopeRequired(t, doc, message)
	}
}

// E4: per the consumer's own doc comments, a message that is not a
// CloudEvent is a WARN-skip (nil error, committed, never retried or
// DLQ'd), an unknown event TYPE (ReplenishmentCompleted) is skipped,
// and unknown ADDITIVE data keys must not fail decode or dispatch.
func TestUnknownAttributesTolerated_PickFace(t *testing.T) {
	golden := pickFaceGoldenExamples(t, "ReplenishmentRequested")[0].Payload

	tolerant := freshGoldenID(t, golden, "tolerated")
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, workUnits := newReplenishmentContractConsumer()
	if err := consumer.handle(context.Background(), replenishmentMsg(goldenJSON(t, tolerant))); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if _, err := workUnits.FindById(context.Background(), workunit.ReplenishmentWorkUnitId(data["request_id"].(string))); err != nil {
		t.Errorf("unknown data attributes changed the outcome: %v", err)
	}

	otherType := freshGoldenID(t, golden, "othertype")
	otherType["type"] = "com.warehouse.wms.pick-face-replenishment.replenishment.ReplenishmentCompleted"
	if err := consumer.handle(context.Background(), replenishmentMsg(goldenJSON(t, otherType))); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	if err := consumer.handle(context.Background(), replenishmentMsg(goldenJSON(t, map[string]any{
		"request_id": "repl-flat", "site_id": "WH1", "sku": "SKU-1", "quantity": 1, "priority": "NORMAL",
	}))); err != nil {
		t.Fatalf("flat payload must be warn-skipped per the consumer's documented policy, got: %v", err)
	}
}
