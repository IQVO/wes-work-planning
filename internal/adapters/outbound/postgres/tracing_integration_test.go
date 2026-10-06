//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// TestQueriesProduceChildSpans proves the otelpgx tracer installed by
// postgres.Connect turns every DB call into a child of the caller's span,
// and that the recorded statement is the normalized SQL — never the literal
// argument values.
func TestQueriesProduceChildSpans(t *testing.T) {
	// Boot + migrate BEFORE installing the tracer provider so the
	// migration's own statements never reach the recording below.
	dsn := migratedDSN(t)

	exporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tracerProvider)
	t.Cleanup(func() { _ = tracerProvider.Shutdown(context.Background()) })

	pool, err := postgres.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Warm the pool first so connect/prepare spans do not land in the
	// recording we assert on.
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	exporter.Reset()

	ctx, parent := tracerProvider.Tracer("test").Start(context.Background(), "find labor plan view")
	pathId, _ := shared.NewPathId("tracing-pick-a")
	if _, err := postgres.NewLaborPlanViewRepo(pool).FindByPathId(ctx, pathId); err == nil {
		t.Log("row unexpectedly present; the span assertions below still hold")
	}
	parent.End()

	query, found := findQuerySpan(exporter.GetSpans())
	if !found {
		t.Fatalf("no db query span recorded: %v", spanNames(exporter.GetSpans()))
	}
	if query.Parent.TraceID() != parent.SpanContext().TraceID() {
		t.Errorf("query span trace = %s, want the caller's %s", query.Parent.TraceID(), parent.SpanContext().TraceID())
	}
	if query.Parent.SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("query span parent = %s, want the caller's span %s", query.Parent.SpanID(), parent.SpanContext().SpanID())
	}
	if query.SpanKind != trace.SpanKindClient {
		t.Errorf("query span kind = %v, want client", query.SpanKind)
	}

	statement, ok := attributeValue(query, "db.query.text", "db.statement")
	if !ok {
		t.Fatalf("query span carries no statement attribute: %v", query.Attributes)
	}
	if !strings.Contains(strings.ToLower(statement), "select") {
		t.Errorf("statement = %q, want the SELECT this repo issues", statement)
	}
	if strings.Contains(statement, "tracing-pick-a") {
		t.Errorf("statement leaked a literal argument value: %q", statement)
	}
}

// findQuerySpan picks otelpgx's query span, ignoring the pool.acquire and
// prepare spans it also emits around the same call.
func findQuerySpan(spans tracetest.SpanStubs) (tracetest.SpanStub, bool) {
	for _, s := range spans {
		if strings.HasPrefix(s.Name, "query ") {
			return s, true
		}
	}
	return tracetest.SpanStub{}, false
}

func attributeValue(span tracetest.SpanStub, keys ...string) (string, bool) {
	for _, key := range keys {
		for _, kv := range span.Attributes {
			if string(kv.Key) == key {
				return kv.Value.String(), true
			}
		}
	}
	return "", false
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name
	}
	return names
}
