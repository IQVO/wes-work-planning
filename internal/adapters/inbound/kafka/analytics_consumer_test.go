package kafka_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
)

// projCall captures one projection-store method invocation.
type projCall struct {
	method  string
	eventId string
	pathId  string
	at      time.Time
}

// fakeProjection records the calls the consumer makes so a test can assert
// the envelope was routed to the right method with the right fields.
type fakeProjection struct {
	calls []projCall
}

func (f *fakeProjection) ApplyWorkReleased(_ context.Context, eventId, pathId string, at time.Time) error {
	f.calls = append(f.calls, projCall{"released", eventId, pathId, at})
	return nil
}
func (f *fakeProjection) ApplyWorkUnitCompleted(_ context.Context, eventId, pathId string, at time.Time) error {
	f.calls = append(f.calls, projCall{"completed", eventId, pathId, at})
	return nil
}
func (f *fakeProjection) ApplyBacklogThresholdBreached(_ context.Context, eventId, pathId string, at time.Time) error {
	f.calls = append(f.calls, projCall{"backlog", eventId, pathId, at})
	return nil
}
func (f *fakeProjection) ApplyPathThrottled(_ context.Context, eventId, pathId string, at time.Time) error {
	f.calls = append(f.calls, projCall{"throttled", eventId, pathId, at})
	return nil
}
func (f *fakeProjection) ApplyRateDeviationDetected(_ context.Context, eventId, pathId string, at time.Time) error {
	f.calls = append(f.calls, projCall{"deviation", eventId, pathId, at})
	return nil
}

// fakeProcessed is an in-memory report.ProcessedEvents.
type fakeProcessed struct {
	seen map[string]bool
}

func newFakeProcessed() *fakeProcessed { return &fakeProcessed{seen: map[string]bool{}} }

func (p *fakeProcessed) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	if p.seen[eventId] {
		return false, nil
	}
	p.seen[eventId] = true
	return true, nil
}

// analyticsEntity is the CloudEvents type entity segment per analytics event.
var analyticsEntity = map[string]string{
	"WorkReleased":             "workunit",
	"WorkUnitCompleted":        "workunit",
	"WorkUnitCreated":          "workunit",
	"BacklogThresholdBreached": "workpool",
	"PathThrottled":            "workpool",
	"RateDeviationDetected":    "workpool",
}

// analyticsEnvelopeBytes builds the exact wire bytes the AnalyticsPublisher
// produces: a CloudEvents 1.0 event with the analytics dataschema.
func analyticsEnvelopeBytes(t *testing.T, eventId, eventName string, at time.Time, data map[string]any) []byte {
	t.Helper()
	b, err := cloudevents.New(cloudevents.Spec{
		ID:        eventId,
		Entity:    analyticsEntity[eventName],
		EventName: eventName,
		Subject:   "pick-zone-a",
		Time:      at,
		Stream:    cloudevents.StreamAnalytics,
		Version:   1,
		Data:      data,
	})
	if err != nil {
		t.Fatalf("build CloudEvent: %v", err)
	}
	return b
}

func TestAnalyticsConsumer_RoutesEachEventType(t *testing.T) {
	at := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	data := map[string]any{"path_id": "pick-zone-a", "work_unit_id": "wu-1"}

	tests := []struct {
		name       string
		eventType  string
		wantMethod string
	}{
		{"released", "WorkReleased", "released"},
		{"completed", "WorkUnitCompleted", "completed"},
		{"backlog", "BacklogThresholdBreached", "backlog"},
		{"throttled", "PathThrottled", "throttled"},
		{"deviation", "RateDeviationDetected", "deviation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proj := &fakeProjection{}
			processed := newFakeProcessed()
			c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

			raw := analyticsEnvelopeBytes(t, "e-"+tt.name, tt.eventType, at, data)
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(proj.calls) != 1 {
				t.Fatalf("calls = %d, want 1", len(proj.calls))
			}
			if proj.calls[0].method != tt.wantMethod {
				t.Errorf("method = %q, want %q", proj.calls[0].method, tt.wantMethod)
			}
			if proj.calls[0].pathId != "pick-zone-a" {
				t.Errorf("pathId = %q, want pick-zone-a", proj.calls[0].pathId)
			}
			if !proj.calls[0].at.Equal(at) {
				t.Errorf("at = %v, want %v", proj.calls[0].at, at)
			}
		})
	}
}

func TestAnalyticsConsumer_Idempotent(t *testing.T) {
	at := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	raw := analyticsEnvelopeBytes(t, "dup", "WorkReleased", at, map[string]any{"path_id": "pick-zone-a"})
	for range 2 {
		if err := c.HandleMessage(context.Background(), raw); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
	if len(proj.calls) != 1 {
		t.Fatalf("expected 1 apply for duplicate delivery, got %d", len(proj.calls))
	}
}

func TestAnalyticsConsumer_IgnoresNonProjectingEventType(t *testing.T) {
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	// WorkUnitCreated is published to the analytics topic but does not move
	// this report, so it must be acknowledged without projecting.
	raw := analyticsEnvelopeBytes(t, "e1", "WorkUnitCreated", time.Now(), map[string]any{"path_id": "pick-zone-a"})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 0 {
		t.Fatalf("expected non-projecting event to make no call, got %d", len(proj.calls))
	}
	// A non-projecting event must NOT be marked processed, so a later
	// contract change could reprocess it.
	if processed.seen["e1"] {
		t.Error("non-projecting event should not be marked processed")
	}
}

// TestAnalyticsConsumer_RejectsLegacyFlatEnvelope: a retired flat-envelope
// message (event_id/event_type/occurred_at/schema_version) is never parsed:
// it is rejected as ErrNotCloudEvent, nothing is projected or marked.
func TestAnalyticsConsumer_RejectsLegacyFlatEnvelope(t *testing.T) {
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	flat := []byte(`{"event_id":"legacy-1","event_type":"WorkReleased","occurred_at":"2026-05-01T08:00:00Z","source":"wes-work-planning","schema_version":1,"data":{"path_id":"pick-zone-a"}}`)
	err := c.HandleMessage(context.Background(), flat)
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("HandleMessage error = %v, want ErrNotCloudEvent", err)
	}
	if len(proj.calls) != 0 || len(processed.seen) != 0 {
		t.Fatalf("legacy message must not be projected or marked: calls=%d seen=%v", len(proj.calls), processed.seen)
	}
}
