package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// spanCtx returns a ctx carrying a real (sampled) span so otelkafka.Inject
// has something to propagate; the tracer provider is restored afterwards.
func spanCtx(t *testing.T) context.Context {
	t.Helper()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})
	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	t.Cleanup(func() { span.End() })
	return ctx
}

func headerValue(headers []kafkago.Header, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func TestPublisher_Encode_ProducesIntegrationWireForm(t *testing.T) {
	workUnits := newReleasedWorkUnitWithGiftWrap(t, "wu-enc", "sku-plain", true)
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "evt-enc" })
	pathId := mustPathId(t, "pick-a")
	at := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)

	encoded, err := pub.Encode(spanCtx(t), shared.NewWorkReleased("wu-enc", pathId, at))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("got %d encoded, want 1", len(encoded))
	}
	e := encoded[0]

	env, err := cloudevents.Decode(e.Value)
	if err != nil {
		t.Fatalf("decode CloudEvent: %v", err)
	}
	var data map[string]any
	if err := env.DataAs(&data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}

	t.Run("routes to the integration topic keyed by aggregate id", func(t *testing.T) {
		assertEncodedRouting(t, e)
	})

	t.Run("wraps the payload in a CloudEvents 1.0 event", func(t *testing.T) {
		assertEncodedEnvelope(t, env, at)
	})

	t.Run("enriches the WorkReleased data at encode time", func(t *testing.T) {
		assertEncodedEnrichment(t, data)
	})

	t.Run("injects the active span's trace headers", func(t *testing.T) {
		assertEncodedTraceHeaders(t, e)
	})
}

func assertEncodedRouting(t *testing.T, e outboundkafka.Encoded) {
	t.Helper()
	if e.Topic != cloudevents.TopicWorkPlanningEvents {
		t.Fatalf("topic = %q, want %q", e.Topic, cloudevents.TopicWorkPlanningEvents)
	}
	if e.EventType != "com.warehouse.wes.work-planning.workunit.WorkReleased" {
		t.Fatalf("event type = %q, want the full CloudEvents type", e.EventType)
	}
	if string(e.Key) != "wu-enc" {
		t.Fatalf("key = %q, want the aggregate (work unit) id", e.Key)
	}
}

func assertEncodedEnvelope(t *testing.T, env ce.Event, at time.Time) {
	t.Helper()
	if env.ID() != "evt-enc" ||
		env.Type() != "com.warehouse.wes.work-planning.workunit.WorkReleased" ||
		env.Source() != "/warehouse/wes-work-planning" ||
		env.Subject() != "wu-enc" ||
		env.DataSchema() != "urn:warehouse:wes-work-planning:events:WorkReleased:v1" ||
		!env.Time().Equal(at) {
		t.Fatalf("unexpected CloudEvent %s", env)
	}
}

func assertEncodedEnrichment(t *testing.T, data map[string]any) {
	t.Helper()
	// The enrichment that READS the work-unit repo happened at encode
	// time — that is the property the outbox relies on.
	if data["ref"] != "ref-1" || data["gift_wrap"] != true || data["work_unit_id"] != "wu-enc" {
		t.Fatalf("unexpected enriched data %v", data)
	}
}

func assertEncodedTraceHeaders(t *testing.T, e outboundkafka.Encoded) {
	t.Helper()
	if _, ok := headerValue(e.Headers, "traceparent"); !ok {
		t.Fatalf("expected a traceparent header when a span is active, got %v", e.Headers)
	}
	if ct, _ := headerValue(e.Headers, "content-type"); ct != cloudevents.MediaType {
		t.Fatalf("content-type header = %q, want %q", ct, cloudevents.MediaType)
	}
}

func TestPublisher_Encode_NoSpan_OnlyContentTypeHeader(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-1", "")
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "evt-1" })
	pathId := mustPathId(t, "pick-a")

	encoded, err := pub.Encode(context.Background(), shared.NewWorkUnitCreated("wu-1", pathId, time.Now()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(encoded) != 1 || len(encoded[0].Headers) != 1 || encoded[0].Headers[0].Key != "content-type" {
		t.Fatalf("expected one message carrying only the content-type header, got %+v", encoded)
	}
}

func TestPublisher_Publish_LeavesMessageTopicEmptyForFixedTopicWriter(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-1", "")
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-1" })
	pathId := mustPathId(t, "pick-a")

	if err := pub.Publish(context.Background(), shared.NewWorkUnitCreated("wu-1", pathId, time.Now())); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(writer.msgs) != 1 || writer.msgs[0].Topic != "" {
		t.Fatalf("a fixed-topic writer must receive topic-less messages, got %+v", writer.msgs)
	}
}

func TestAnalyticsPublisher_Encode_ProducesAnalyticsWireFormAndSkipsUnknown(t *testing.T) {
	pub := outboundkafka.NewAnalyticsPublisherWithWriter(&fakeWriter{}, func() string { return "evt-a" })
	pathId := mustPathId(t, "pick-a")
	at := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)

	encoded, err := pub.Encode(spanCtx(t),
		unknownEvent{at: at},
		shared.NewWorkReleased("wu-9", pathId, at),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("got %d encoded, want 1 (unknown event skipped)", len(encoded))
	}
	e := encoded[0]
	if e.Topic != outboundkafka.AnalyticsTopic || e.EventType != "com.warehouse.wes.work-planning.workunit.WorkReleased" || string(e.Key) != "wu-9" {
		t.Fatalf("unexpected encoded %+v", e)
	}
	env, err := cloudevents.Decode(e.Value)
	if err != nil {
		t.Fatalf("decode CloudEvent: %v", err)
	}
	if env.ID() != "evt-a" || env.DataSchema() != "urn:warehouse:wes-work-planning:analytics:WorkReleased:v1" || env.Source() != "/warehouse/wes-work-planning" {
		t.Fatalf("unexpected analytics CloudEvent %s", env)
	}
	if _, ok := headerValue(e.Headers, "traceparent"); !ok {
		t.Fatalf("expected a traceparent header, got %v", e.Headers)
	}
}

// unknownEvent is a DomainEvent outside the analytics contract.
type unknownEvent struct{ at time.Time }

func (unknownEvent) EventName() string       { return "SomethingElse" }
func (u unknownEvent) OccurredAt() time.Time { return u.at }

func TestRelaySink_SetsTopicPerMessageAndWritesOnce(t *testing.T) {
	writer := &fakeWriter{}
	sink := outboundkafka.NewRelaySinkWithWriter(writer)

	err := sink.Send(context.Background(),
		outboundkafka.Encoded{Topic: "t1", Key: []byte("k1"), Value: []byte("v1"), Headers: []kafkago.Header{{Key: "h", Value: []byte("x")}}},
		outboundkafka.Encoded{Topic: "t2", Key: []byte("k2"), Value: []byte("v2")},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(writer.msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(writer.msgs))
	}
	if writer.msgs[0].Topic != "t1" || writer.msgs[1].Topic != "t2" {
		t.Fatalf("topics not routed per message: %+v", writer.msgs)
	}
	if string(writer.msgs[0].Key) != "k1" || string(writer.msgs[0].Value) != "v1" || len(writer.msgs[0].Headers) != 1 {
		t.Fatalf("message 0 not carried through: %+v", writer.msgs[0])
	}
	if err := sink.Send(context.Background()); err != nil {
		t.Fatalf("empty send must be a no-op, got %v", err)
	}
	if err := sink.Close(); err != nil || !writer.closed {
		t.Fatalf("close: err=%v closed=%v", err, writer.closed)
	}
}

func TestRelaySink_PropagatesWriterError(t *testing.T) {
	sink := outboundkafka.NewRelaySinkWithWriter(&fakeWriter{err: errors.New("broker down")})
	if err := sink.Send(context.Background(), outboundkafka.Encoded{Topic: "t", Value: []byte("v")}); err == nil {
		t.Fatal("expected the writer error to propagate")
	}
}
