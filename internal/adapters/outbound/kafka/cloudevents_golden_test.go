package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// updateGolden rewrites testdata/*.golden.json from the current encoder
// output: `go test ./internal/adapters/outbound/kafka -run Golden -update`.
// Review the diff before committing — these files ARE the wire contract.
var updateGolden = flag.Bool("update", false, "rewrite CloudEvents golden files")

// goldenCase is one published event type: the domain event plus the exact
// `type` and `subject` attributes it must carry on the wire.
type goldenCase struct {
	name    string
	event   shared.DomainEvent
	ceType  string
	subject string
}

func goldenCases(t *testing.T) []goldenCase {
	t.Helper()
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	pathId := mustPathId(t, "pick-a")
	const prefix = "com.warehouse.wes.work-planning."
	return []goldenCase{
		{"ChargeForecastReceived", shared.NewChargeForecastReceived(pathId, at), prefix + "charge.ChargeForecastReceived", "pick-a"},
		{"ShiftPlanCommitted", shared.NewShiftPlanCommitted(pathId, at), prefix + "plan.ShiftPlanCommitted", "pick-a"},
		{"WorkUnitCreated", shared.NewWorkUnitCreated("wu-gold", pathId, at), prefix + "workunit.WorkUnitCreated", "wu-gold"},
		{"WorkReleased", shared.NewWorkReleased("wu-gold", pathId, at), prefix + "workunit.WorkReleased", "wu-gold"},
		{"WorkUnitCompleted", shared.NewWorkUnitCompleted("wu-gold", pathId, at), prefix + "workunit.WorkUnitCompleted", "wu-gold"},
		{"BacklogThresholdBreached", shared.NewBacklogThresholdBreached(pathId, at), prefix + "workpool.BacklogThresholdBreached", "pick-a"},
		{"RateDeviationDetected", shared.NewRateDeviationDetected(pathId, at), prefix + "workpool.RateDeviationDetected", "pick-a"},
		{"PathThrottled", shared.NewPathThrottled(pathId, at), prefix + "workpool.PathThrottled", "pick-a"},
		{"LaborReassignmentFlagged", shared.NewLaborReassignmentFlagged(pathId, at), prefix + "workpool.LaborReassignmentFlagged", "pick-a"},
		{"PathCapacityChanged", shared.NewPathCapacityChanged(pathId, time.Date(2026, 9, 30, 18, 0, 0, 0, time.UTC), 42, true, at), prefix + "workpool.PathCapacityChanged", "pick-a"},
	}
}

// TestCloudEvents_Golden_Integration asserts the exact JSON (every context
// attribute, the full type string and the data payload) of every event type
// published on warehouse.work-planning.events, plus the content-type header.
func TestCloudEvents_Golden_Integration(t *testing.T) {
	workUnits := newReleasedWorkUnitWithGiftWrap(t, "wu-gold", "sku-plain", true)
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "11111111-1111-4111-8111-111111111111" })

	for _, tc := range goldenCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := pub.Encode(context.Background(), tc.event)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(encoded) != 1 {
				t.Fatalf("got %d encoded, want 1", len(encoded))
			}
			assertGolden(t, encoded[0], tc, cloudevents.StreamEvents, "events_"+tc.name)
			if string(encoded[0].Key) != tc.subject {
				t.Errorf("key = %q, want the aggregate id %q (ADR-0024)", encoded[0].Key, tc.subject)
			}
		})
	}
}

// TestCloudEvents_Golden_Analytics is the same for warehouse.wes.analytics:
// same type per occurrence, analytics dataschema, no schema_version.
func TestCloudEvents_Golden_Analytics(t *testing.T) {
	pub := outboundkafka.NewAnalyticsPublisherWithWriter(&fakeWriter{}, func() string { return "22222222-2222-4222-8222-222222222222" })

	for _, tc := range goldenCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := pub.Encode(context.Background(), tc.event)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(encoded) != 1 {
				t.Fatalf("got %d encoded, want 1", len(encoded))
			}
			assertGolden(t, encoded[0], tc, cloudevents.StreamAnalytics, "analytics_"+tc.name)
			if string(encoded[0].Key) != tc.subject {
				t.Errorf("key = %q, want the aggregate id %q", encoded[0].Key, tc.subject)
			}
		})
	}
}

func assertGolden(t *testing.T, e outboundkafka.Encoded, tc goldenCase, stream, file string) {
	t.Helper()

	if ct := headerOf(e.Headers, "content-type"); ct != "application/cloudevents+json; charset=UTF-8" {
		t.Errorf("content-type header = %q", ct)
	}
	if e.EventType != tc.ceType {
		t.Errorf("Encoded.EventType = %q, want %q", e.EventType, tc.ceType)
	}

	var attrs map[string]any
	if err := json.Unmarshal(e.Value, &attrs); err != nil {
		t.Fatalf("value is not JSON: %v", err)
	}
	want := map[string]string{
		"specversion":     "1.0",
		"source":          "/warehouse/wes-work-planning",
		"type":            tc.ceType,
		"subject":         tc.subject,
		"time":            "2026-09-30T12:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:wes-work-planning:" + stream + ":" + tc.name + ":v1",
	}
	for k, v := range want {
		if attrs[k] != v {
			t.Errorf("attribute %s = %v, want %q", k, attrs[k], v)
		}
	}
	if id, _ := attrs["id"].(string); id == "" {
		t.Error("attribute id is empty")
	}
	for _, legacy := range []string{"event_id", "event_type", "occurred_at", "schema_version"} {
		if _, ok := attrs[legacy]; ok {
			t.Errorf("legacy flat-envelope field %q must not appear on the wire", legacy)
		}
	}

	var got bytes.Buffer
	if err := json.Indent(&got, e.Value, "", "  "); err != nil {
		t.Fatalf("indent: %v", err)
	}
	got.WriteByte('\n')
	path := filepath.Join("testdata", "ce_"+file+".golden.json")
	if *updateGolden {
		if err := os.WriteFile(path, got.Bytes(), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}
	wantBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run with -update to create): %v", path, err)
	}
	if !bytes.Equal(got.Bytes(), wantBytes) {
		t.Errorf("wire JSON mismatch for %s\n--- got\n%s\n--- want\n%s", path, got.String(), wantBytes)
	}
}

func headerOf(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
