package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// analyticsEnv is the decode shape of the analytics envelope, for asserting
// on what the AnalyticsPublisher wrote.
type analyticsEnv struct {
	EventId       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Source        string          `json:"source"`
	SchemaVersion int             `json:"schema_version"`
	Data          json.RawMessage `json:"data"`
}

func newAnalyticsID() outboundkafka.IDGenerator {
	n := 0
	return func() string {
		n++
		return "evt-" + string(rune('0'+n))
	}
}

func mustPathId(t *testing.T, s string) shared.PathId {
	t.Helper()
	p, err := shared.NewPathId(s)
	if err != nil {
		t.Fatalf("NewPathId(%q): %v", s, err)
	}
	return p
}

// analyticsCase is one row of TestAnalyticsPublisher_EmitsEnvelopePerEvent's
// table.
type analyticsCase struct {
	name     string
	event    shared.DomainEvent
	wantType string
	wantKey  string
	wantPath string
	wantUnit string // "" when the event carries no work_unit_id
}

// assertAnalyticsMessage decodes one written message and asserts the
// analytics wire form: the envelope's fixed fields plus the data payload's
// path/work-unit identity.
func assertAnalyticsMessage(t *testing.T, key, value []byte, tt analyticsCase, at time.Time) {
	t.Helper()
	if string(key) != tt.wantKey {
		t.Errorf("key = %q, want %q", key, tt.wantKey)
	}

	var env analyticsEnv
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.EventType != tt.wantType {
		t.Errorf("event_type = %q, want %q", env.EventType, tt.wantType)
	}
	if env.Source != "wes-work-planning" {
		t.Errorf("source = %q, want wes-work-planning", env.Source)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("schema_version = %d, want 1", env.SchemaVersion)
	}
	if env.EventId == "" {
		t.Error("event_id is empty")
	}
	if !env.OccurredAt.Equal(at) {
		t.Errorf("occurred_at = %v, want %v", env.OccurredAt, at)
	}

	var data map[string]any
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if data["path_id"] != tt.wantPath {
		t.Errorf("data.path_id = %v, want %q", data["path_id"], tt.wantPath)
	}
	if tt.wantUnit != "" && data["work_unit_id"] != tt.wantUnit {
		t.Errorf("data.work_unit_id = %v, want %q", data["work_unit_id"], tt.wantUnit)
	}
}

func TestAnalyticsPublisher_EmitsEnvelopePerEvent(t *testing.T) {
	at := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	pathId := mustPathId(t, "pick-zone-a")

	tests := []analyticsCase{
		{"work released", shared.NewWorkReleased("wu-1", pathId, at), "WorkReleased", "wu-1", "pick-zone-a", "wu-1"},
		{"work completed", shared.NewWorkUnitCompleted("wu-2", pathId, at), "WorkUnitCompleted", "wu-2", "pick-zone-a", "wu-2"},
		{"work created", shared.NewWorkUnitCreated("wu-3", pathId, at), "WorkUnitCreated", "wu-3", "pick-zone-a", "wu-3"},
		{"backlog breach", shared.NewBacklogThresholdBreached(pathId, at), "BacklogThresholdBreached", "pick-zone-a", "pick-zone-a", ""},
		{"path throttled", shared.NewPathThrottled(pathId, at), "PathThrottled", "pick-zone-a", "pick-zone-a", ""},
		{"rate deviation", shared.NewRateDeviationDetected(pathId, at), "RateDeviationDetected", "pick-zone-a", "pick-zone-a", ""},
		{"charge forecast", shared.NewChargeForecastReceived(pathId, at), "ChargeForecastReceived", "pick-zone-a", "pick-zone-a", ""},
		{"shift plan", shared.NewShiftPlanCommitted(pathId, at), "ShiftPlanCommitted", "pick-zone-a", "pick-zone-a", ""},
		{"labor reassign", shared.NewLaborReassignmentFlagged(pathId, at), "LaborReassignmentFlagged", "pick-zone-a", "pick-zone-a", ""},
		{"path capacity changed", shared.NewPathCapacityChanged(pathId, at.Add(time.Hour), 4, true, at), "PathCapacityChanged", "pick-zone-a", "pick-zone-a", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &fakeWriter{}
			p := outboundkafka.NewAnalyticsPublisherWithWriter(w, newAnalyticsID())

			if err := p.Publish(context.Background(), tt.event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(w.msgs) != 1 {
				t.Fatalf("messages = %d, want 1", len(w.msgs))
			}
			assertAnalyticsMessage(t, w.msgs[0].Key, w.msgs[0].Value, tt, at)
		})
	}
}

func TestAnalyticsPublisher_EmptyAndClose(t *testing.T) {
	w := &fakeWriter{}
	p := outboundkafka.NewAnalyticsPublisherWithWriter(w, newAnalyticsID())

	// No events: nothing written, no error.
	if err := p.Publish(context.Background()); err != nil {
		t.Fatalf("Publish(empty): %v", err)
	}
	if len(w.msgs) != 0 {
		t.Fatalf("messages = %d, want 0", len(w.msgs))
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !w.closed {
		t.Error("writer not closed")
	}
}
