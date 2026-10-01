package otelkafka_test

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/otelkafka"
)

func TestHeaderCarrierGetSetKeys(t *testing.T) {
	headers := []kafkago.Header{{Key: "existing", Value: []byte("value")}}
	carrier := otelkafka.NewHeaderCarrier(&headers)

	if got := carrier.Get("existing"); got != "value" {
		t.Errorf("Get(existing) = %q, want value", got)
	}
	if got := carrier.Get("absent"); got != "" {
		t.Errorf("Get(absent) = %q, want empty", got)
	}

	carrier.Set("traceparent", "first")
	if got := carrier.Get("traceparent"); got != "first" {
		t.Errorf("Get(traceparent) = %q, want first", got)
	}

	// Setting again must replace, never append a second conflicting header.
	carrier.Set("traceparent", "second")
	if got := carrier.Get("traceparent"); got != "second" {
		t.Errorf("Get(traceparent) = %q, want second", got)
	}
	if len(headers) != 2 {
		t.Errorf("got %d headers, want 2 (Set must overwrite in place): %v", len(headers), headers)
	}

	keys := carrier.Keys()
	if len(keys) != 2 || keys[0] != "existing" || keys[1] != "traceparent" {
		t.Errorf("Keys() = %v, want [existing traceparent]", keys)
	}
}

func TestHeaderCarrierWithNilHeadersIsInert(t *testing.T) {
	carrier := otelkafka.HeaderCarrier{}
	carrier.Set("traceparent", "value") // must not panic
	if got := carrier.Get("traceparent"); got != "" {
		t.Errorf("Get = %q, want empty", got)
	}
	if got := carrier.Keys(); got != nil {
		t.Errorf("Keys = %v, want nil", got)
	}
}

// TestTraceContextPropagatesAcrossTheMessageBoundary is the unit-level proof
// of cross-service tracing: a span opened on the producer side is injected
// into the message headers, and the consumer side's span — started from a
// context that never saw the producer's — comes back as its child, in the
// same trace.
func TestTraceContextPropagatesAcrossTheMessageBoundary(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracerProvider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tracerProvider)
	t.Cleanup(func() { _ = tracerProvider.Shutdown(context.Background()) })

	producerCtx, publishSpan := otelkafka.StartPublishSpan(context.Background(), "warehouse.work-planning.events")
	msg := kafkago.Message{Value: []byte(`{"specversion":"1.0","type":"com.warehouse.wes.work-planning.workunit.WorkReleased"}`)}
	otelkafka.Inject(producerCtx, &msg)
	publishSpan.End()

	if carrier := otelkafka.NewHeaderCarrier(&msg.Headers); carrier.Get("traceparent") == "" {
		t.Fatalf("Inject wrote no traceparent header: %v", msg.Headers)
	}

	// The consumer starts from a fresh context, as a real consumer loop does.
	consumerCtx, consumeSpan := otelkafka.StartConsumeSpan(
		otelkafka.Extract(context.Background(), &msg),
		"warehouse.work-planning.events",
	)
	defer consumeSpan.End()

	produced := publishSpan.SpanContext()
	consumed := trace.SpanContextFromContext(consumerCtx)

	if consumed.TraceID() != produced.TraceID() {
		t.Errorf("consumer trace id = %s, want the producer's %s", consumed.TraceID(), produced.TraceID())
	}
	if consumed.SpanID() == produced.SpanID() {
		t.Error("consumer reused the producer's span id instead of starting a child span")
	}
	if !consumed.IsValid() {
		t.Error("consumer span context is not valid")
	}
}

func TestExtractWithoutHeadersYieldsNoParent(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	msg := kafkago.Message{Value: []byte(`{}`)}
	ctx := otelkafka.Extract(context.Background(), &msg)

	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("an uninstrumented message must not produce a valid parent span context")
	}
}
