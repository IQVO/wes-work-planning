//go:build integration

// D27 (trace continuity across the outbox): the W3C trace context of the
// transaction that wrote an outbox row crosses the outbox to Kafka. This repo
// already stores it: the encoders inject the active span into the message
// headers (otelkafka.Inject in encodeCloudEventKeyed), OutboxPublisher
// persists them in outbox_events.headers, and the relay sends them back as
// Kafka headers. These tests pin that end to end against a real Postgres (the
// package's shared testcontainer) and a real Kafka broker (testcontainers).
package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// installW3CTracing installs a real SDK tracer provider and the W3C
// trace-context propagator globally (what telemetry.Setup does in
// production), restoring the previous globals when the test ends.
func installW3CTracing(t *testing.T) trace.Tracer {
	t.Helper()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return tp.Tracer("outbox-trace-itest")
}

// startTraceKafka boots a disposable broker and pre-creates topics so the
// relay's first write does not race topic auto-creation.
func startTraceKafka(t *testing.T, ctx context.Context, topics ...string) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("wes-outbox-trace-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	for _, topic := range topics {
		if err := createCapacityTopic(ctx, brokers, topic); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
	return brokers
}

func readOneMessage(t *testing.T, ctx context.Context, brokers []string, topic string) kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6, MaxWait: 2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	if err := reader.SetOffset(0); err != nil {
		t.Fatalf("SetOffset: %v", err)
	}
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("read %s: %v", topic, err)
	}
	return msg
}

// TestOutboxTrace_TraceparentCrossesTheOutboxToKafka: a use case runs inside
// a request span; the outbox row stores that span's traceparent; the relay (a
// later pass, no span of its own) sends it as a Kafka header; and a consumer
// extracting it with the global propagator lands on the request's trace and
// span.
func TestOutboxTrace_TraceparentCrossesTheOutboxToKafka(t *testing.T) {
	tracer := installW3CTracing(t)
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	brokers := startTraceKafka(t, ctx, cloudevents.TopicWorkPlanningEvents)

	integration, _ := encoders(pool)
	outbox := postgres.NewOutboxPublisher(pool, integration)
	clock := memory.FixedClock{At: time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC)}
	enqueue := usecases.NewEnqueueWorkUnit(postgres.NewWorkUnitRepo(pool), postgres.NewWorkPoolRepo(pool), outbox, clock).
		WithUnitOfWork(postgres.NewUnitOfWork(pool))

	reqCtx, reqSpan := tracer.Start(ctx, "POST /paths/{pathId}/work-units", trace.WithSpanKind(trace.SpanKindServer))
	if _, err := enqueue.Execute(reqCtx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-trace-1", PathId: mustPath(t, "pick-trace"), CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Reference: "order-trace",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	reqSpan.End()
	want := reqSpan.SpanContext()
	wantTraceparent := fmt.Sprintf("00-%s-%s-01", want.TraceID(), want.SpanID())

	const created = "com.warehouse.wes.work-planning.workunit.WorkUnitCreated"
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT headers FROM outbox_events WHERE event_type = $1`, created).Scan(&raw); err != nil {
		t.Fatalf("read stored headers: %v", err)
	}
	var stored []map[string]string
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("decode stored headers %s: %v", raw, err)
	}
	var storedTP string
	for _, h := range stored {
		if h["key"] == "traceparent" {
			storedTP = h["value"]
		}
	}
	if storedTP != wantTraceparent {
		t.Fatalf("outbox row traceparent = %q, want the request span's %q (stored headers %s)", storedTP, wantTraceparent, raw)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(20))
	// The relay runs without the request's context: continuity must come
	// from the stored row, not from the caller.
	if n, err := relay.RelayOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readOneMessage(t, ctx, brokers, cloudevents.TopicWorkPlanningEvents)
	got := capacityHeader(msg.Headers, "traceparent")
	if got != wantTraceparent {
		t.Fatalf("Kafka traceparent = %q, want %q (headers %+v)", got, wantTraceparent, msg.Headers)
	}
	if ct := capacityHeader(msg.Headers, "content-type"); ct != cloudevents.MediaType {
		t.Fatalf("content-type header lost on the outbox path: %+v", msg.Headers)
	}

	consumerCtx := otel.GetTextMapPropagator().Extract(context.Background(), traceHeaderCarrier(msg.Headers))
	parent := trace.SpanContextFromContext(consumerCtx)
	if parent.TraceID() != want.TraceID() || parent.SpanID() != want.SpanID() || !parent.IsRemote() {
		t.Fatalf("consumer would parent on %s/%s (remote=%v), want the request span %s/%s",
			parent.TraceID(), parent.SpanID(), parent.IsRemote(), want.TraceID(), want.SpanID())
	}
}

// TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged pins the other half
// of D27: a row written with no trace context (no active span, or a row that
// predates trace capture) relays with exactly its stored key and value, and
// carries no traceparent a consumer could mis-parent onto.
func TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged(t *testing.T) {
	installW3CTracing(t)
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const topic = "warehouse.work-planning.itest-no-trace"
	brokers := startTraceKafka(t, ctx, topic)

	insertRawOutboxRow(t, pool, topic, "com.example.Untraced", "pick-untraced")

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(20))
	if n, err := relay.RelayOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readOneMessage(t, ctx, brokers, topic)
	if string(msg.Key) != "pick-untraced" || string(msg.Value) != `{}` {
		t.Fatalf("row relayed changed: key=%q value=%q", msg.Key, msg.Value)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("a row without stored headers must relay with none, got %+v", msg.Headers)
	}
}

// traceHeaderCarrier adapts kafka-go headers to the OTel text-map carrier,
// read-only, the way a consumer extracts the parent context.
type traceHeaderCarrier []kafkago.Header

func (c traceHeaderCarrier) Get(key string) string { return capacityHeader(c, key) }

func (c traceHeaderCarrier) Set(string, string) {}

func (c traceHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}
