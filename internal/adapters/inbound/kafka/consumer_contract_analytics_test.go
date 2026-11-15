package kafka

// Event consumer contract tests (E1-E4) for the ANALYTICS edge: this
// repo's own cmd/wes-projector (AnalyticsConsumer) subscribes to the
// topic this service itself publishes through its transactional outbox
// relay — warehouse.wes.analytics (cloudevents.TopicAnalytics /
// outboundkafka.AnalyticsTopic, ADR-0011) — and folds the five
// throughput-moving types (WorkReleased, WorkUnitCompleted,
// BacklogThresholdBreached, PathThrottled, RateDeviationDetected) into
// the report projection, deduped on the CloudEvents id. The contract is
// the PINNED copy of this repo's OWN apis/asyncapi.yaml
// (testdata/wes-work-planning.asyncapi.yaml, registered in
// contracts/pinned.json): a self-pin, so the projector's decode
// contract and the publisher's spec cannot drift apart silently.
//
// Golden fixtures come from that spec's OWN examples.
//
// Types documented on the analytics channel WITHOUT examples (E1-only,
// producer example backfill pending): WorkUnitCompletedAnalytics,
// BacklogThresholdBreachedAnalytics, PathThrottledAnalytics,
// RateDeviationDetectedAnalytics, ShiftPlanCommittedAnalytics,
// WorkUnitCreatedAnalytics, ChargeForecastReceivedAnalytics,
// LaborReassignmentFlaggedAnalytics, PathCapacityChangedAnalytics,
// PathPlanDriftDetectedAnalytics — only WorkReleasedAnalytics carries an
// example today, so it is the one E2 golden-runs.

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
)

const pinnedAnalyticsSpec = "testdata/wes-work-planning.asyncapi.yaml"

// analyticsProjectedTypes maps the CloudEvents types HandleMessage's
// routing switch projects to the message names the analytics channel
// documents for them.
var analyticsProjectedTypes = map[string]string{
	"WorkUnitCompletedAnalytics":        typeWorkUnitCompleted,
	"BacklogThresholdBreachedAnalytics": typeBacklogThresholdBreached,
	"PathThrottledAnalytics":            typePathThrottled,
	"RateDeviationDetectedAnalytics":    typeRateDeviationDetected,
	// WorkReleasedAnalytics has the spec example; kept via the helper
	// below rather than a duplicate constant.
}

// analyticsRecordingProjection records each projection call so tests can
// assert exactly-one routing per event.
type analyticsRecordingProjection struct {
	applied []string // one entry per projection call: "<type>|<pathId>"
}

func (p *analyticsRecordingProjection) ApplyWorkReleased(_ context.Context, _, pathId string, _ time.Time) error {
	p.applied = append(p.applied, "WorkReleased|"+pathId)
	return nil
}

func (p *analyticsRecordingProjection) ApplyWorkUnitCompleted(_ context.Context, _, pathId string, _ time.Time) error {
	p.applied = append(p.applied, "WorkUnitCompleted|"+pathId)
	return nil
}

func (p *analyticsRecordingProjection) ApplyBacklogThresholdBreached(_ context.Context, _, pathId string, _ time.Time) error {
	p.applied = append(p.applied, "BacklogThresholdBreached|"+pathId)
	return nil
}

func (p *analyticsRecordingProjection) ApplyPathThrottled(_ context.Context, _, pathId string, _ time.Time) error {
	p.applied = append(p.applied, "PathThrottled|"+pathId)
	return nil
}

func (p *analyticsRecordingProjection) ApplyRateDeviationDetected(_ context.Context, _, pathId string, _ time.Time) error {
	p.applied = append(p.applied, "RateDeviationDetected|"+pathId)
	return nil
}

// analyticsProcessedGate is a ProcessedEvents that remembers ids.
type analyticsProcessedGate struct {
	seen map[string]bool
}

func (g *analyticsProcessedGate) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	if g.seen[eventId] {
		return false, nil
	}
	if g.seen == nil {
		g.seen = map[string]bool{}
	}
	g.seen[eventId] = true
	return true, nil
}

func newAnalyticsContractConsumer() (*AnalyticsConsumer, *analyticsRecordingProjection) {
	proj := &analyticsRecordingProjection{}
	return &AnalyticsConsumer{
		Projection: proj,
		Processed:  &analyticsProcessedGate{},
		Logger:     slog.New(slog.DiscardHandler),
	}, proj
}

func analyticsGoldenExamples(t *testing.T) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec)
	msg := doc.message(t, "WorkReleasedAnalytics")
	if len(msg.Examples) == 0 {
		t.Fatal("pinned self-spec documents WorkReleasedAnalytics without examples")
	}
	return msg.Examples
}

// E1: the topic this repo's projector reader subscribes to is this
// repo's own analytics topic, documented by the pinned self-spec, AND
// the channel's message list documents every CloudEvents type the
// projector's routing switch projects (plus the non-projecting types it
// must skip).
func TestPinnedAsyncAPICoversSubscriptions_Analytics(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec)
	if _, how := doc.channelMessageNames(t, cloudevents.TopicAnalytics); how == "" {
		t.Fatalf("no message list found for %s", cloudevents.TopicAnalytics)
	}
	documented := doc.documentedTypes(t, cloudevents.TopicAnalytics)
	for _, want := range append([]string{typeWorkReleased}, analyticsProjectedTypesValue()...) {
		if !documented[want] {
			t.Errorf("pinned self-spec does not document %s on %s (documented: %v)", want, cloudevents.TopicAnalytics, documented)
		}
	}
}

func analyticsProjectedTypesValue() []string {
	out := make([]string, 0, len(analyticsProjectedTypes))
	for _, v := range analyticsProjectedTypes {
		out = append(out, v)
	}
	return out
}

// E2: the WorkReleasedAnalytics example the self-spec documents decodes
// through the REAL AnalyticsConsumer.HandleMessage (decode -> routing
// switch -> MarkProcessed -> projection): exactly one projection call
// carrying the wire's path, and a replayed CloudEvents id projects
// nothing more (applied-set size compared before/after).
func TestGoldenEventsDecode_AnalyticsWorkReleased(t *testing.T) {
	for _, ex := range analyticsGoldenExamples(t) {
		t.Run(ex.Name, func(t *testing.T) {
			consumer, proj := newAnalyticsContractConsumer()
			if err := consumer.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(proj.applied) != 1 {
				t.Fatalf("golden event produced %d projection calls, want 1 (%v)", len(proj.applied), proj.applied)
			}
			pathId := ex.Payload["data"].(map[string]any)["path_id"].(string)
			if proj.applied[0] != "WorkReleased|"+pathId {
				t.Errorf("projection applied %q, want WorkReleased|%s", proj.applied[0], pathId)
			}
			if err := consumer.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
				t.Fatalf("replayed HandleMessage: %v", err)
			}
			if len(proj.applied) != 1 {
				t.Errorf("replayed CloudEvents id projected again: %d calls (%v)", len(proj.applied), proj.applied)
			}
		})
	}
}

// E3: the golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope and this repo's own analytics dataschema urn convention
// (urn:warehouse:wes-work-planning:analytics:<Event>:v1), and the pinned
// self-spec's envelope schema requires those attributes.
func TestCloudEventsEnvelopeInvariants_Analytics(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedAnalyticsSpec)
	for _, ex := range analyticsGoldenExamples(t) {
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
		if evt.Source() != cloudevents.Source {
			t.Errorf("%s: source %q, want this repo's own %q", ex.Name, evt.Source(), cloudevents.Source)
		}
		if evt.DataMediaType() != cloudevents.DataContentType {
			t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
		}
		if evt.DataSchema() != "urn:warehouse:wes-work-planning:analytics:WorkReleased:v1" {
			t.Errorf("%s: dataschema %q, want the analytics WorkReleased v1 urn", ex.Name, evt.DataSchema())
		}
	}
	assertEnvelopeRequired(t, doc, "WorkReleasedAnalytics")
}

// E4: the projector's own documented policy (analytics_consumer.go):
// unknown ADDITIVE data keys must not fail decode or projection; an
// unknown event TYPE (every non-projecting type on this channel) is
// acknowledged without touching the read model or the processed set; a
// non-CloudEvent message fails HandleMessage wrapping ErrNotCloudEvent
// and Run WARN-skips it (never retries).
func TestUnknownAttributesTolerated_Analytics(t *testing.T) {
	golden := analyticsGoldenExamples(t)[0].Payload

	tolerant := freshGoldenID(t, golden, "tolerated")
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, proj := newAnalyticsContractConsumer()
	if err := consumer.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("HandleMessage with unknown data attributes: %v", err)
	}
	if len(proj.applied) != 1 {
		t.Errorf("unknown data attributes changed the outcome: %d projection calls", len(proj.applied))
	}

	otherType := freshGoldenID(t, golden, "othertype")
	otherType["type"] = typeBacklogThresholdBreached // documented on the channel, NOT in this example's shape
	otherType["dataschema"] = "urn:warehouse:wes-work-planning:analytics:BacklogThresholdBreached:v1"
	if err := consumer.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("documented-but-shape-mismatched type must project via its own path or be a real error, got: %v", err)
	}

	unprojected := freshGoldenID(t, golden, "unprojected")
	unprojected["type"] = cloudevents.Type("workunit", "WorkUnitCreated")
	unprojected["dataschema"] = "urn:warehouse:wes-work-planning:analytics:WorkUnitCreated:v1"
	if err := consumer.HandleMessage(context.Background(), goldenJSON(t, unprojected)); err != nil {
		t.Fatalf("non-projecting documented type must be acknowledged, got: %v", err)
	}
	if len(proj.applied) != 2 { // WorkReleased (tolerated) + BacklogThresholdBreached (otherType) — WorkUnitCreated adds none
		t.Errorf("a non-projecting type touched the projection: %d calls (%v)", len(proj.applied), proj.applied)
	}

	_, err := cloudevents.Decode(goldenJSON(t, map[string]any{"path_id": "pick-to-tote", "work_unit_id": "order-88421-line-3"}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("flat payload decoded as %v, want ErrNotCloudEvent (Run's WARN-skip trigger)", err)
	}
}
