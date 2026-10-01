package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

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

// analyticsCase is one row of TestAnalyticsPublisher_EmitsCloudEventPerEvent's
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
// analytics wire form: the CloudEvent context attributes plus the data payload's
// path/work-unit identity.
func assertAnalyticsMessage(t *testing.T, key, value []byte, tt analyticsCase, at time.Time) {
	t.Helper()
	if string(key) != tt.wantKey {
		t.Errorf("key = %q, want %q", key, tt.wantKey)
	}

	env, err := cloudevents.Decode(value)
	if err != nil {
		t.Fatalf("decode CloudEvent: %v", err)
	}
	if env.Type() != tt.wantType {
		t.Errorf("type = %q, want %q", env.Type(), tt.wantType)
	}
	if env.Source() != "/warehouse/wes-work-planning" {
		t.Errorf("source = %q, want /warehouse/wes-work-planning", env.Source())
	}
	if env.Subject() != tt.wantKey {
		t.Errorf("subject = %q, want %q (the aggregate id)", env.Subject(), tt.wantKey)
	}
	if env.ID() == "" {
		t.Error("id is empty")
	}
	if !env.Time().Equal(at) {
		t.Errorf("time = %v, want %v", env.Time(), at)
	}

	var data map[string]any
	if err := env.DataAs(&data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if data["path_id"] != tt.wantPath {
		t.Errorf("data.path_id = %v, want %q", data["path_id"], tt.wantPath)
	}
	if tt.wantUnit != "" && data["work_unit_id"] != tt.wantUnit {
		t.Errorf("data.work_unit_id = %v, want %q", data["work_unit_id"], tt.wantUnit)
	}
}

func TestAnalyticsPublisher_EmitsCloudEventPerEvent(t *testing.T) {
	at := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	pathId := mustPathId(t, "pick-zone-a")

	tests := []analyticsCase{
		{"work released", shared.NewWorkReleased("wu-1", pathId, at), "com.warehouse.wes.work-planning.workunit.WorkReleased", "wu-1", "pick-zone-a", "wu-1"},
		{"work completed", shared.NewWorkUnitCompleted("wu-2", pathId, at), "com.warehouse.wes.work-planning.workunit.WorkUnitCompleted", "wu-2", "pick-zone-a", "wu-2"},
		{"work created", shared.NewWorkUnitCreated("wu-3", pathId, at), "com.warehouse.wes.work-planning.workunit.WorkUnitCreated", "wu-3", "pick-zone-a", "wu-3"},
		{"backlog breach", shared.NewBacklogThresholdBreached(pathId, at), "com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached", "pick-zone-a", "pick-zone-a", ""},
		{"path throttled", shared.NewPathThrottled(pathId, at), "com.warehouse.wes.work-planning.workpool.PathThrottled", "pick-zone-a", "pick-zone-a", ""},
		{"rate deviation", shared.NewRateDeviationDetected(pathId, at), "com.warehouse.wes.work-planning.workpool.RateDeviationDetected", "pick-zone-a", "pick-zone-a", ""},
		{"charge forecast", shared.NewChargeForecastReceived(pathId, at), "com.warehouse.wes.work-planning.charge.ChargeForecastReceived", "pick-zone-a", "pick-zone-a", ""},
		{"shift plan", shared.NewShiftPlanCommitted(pathId, at), "com.warehouse.wes.work-planning.plan.ShiftPlanCommitted", "pick-zone-a", "pick-zone-a", ""},
		{"labor reassign", shared.NewLaborReassignmentFlagged(pathId, at), "com.warehouse.wes.work-planning.workpool.LaborReassignmentFlagged", "pick-zone-a", "pick-zone-a", ""},
		{"path capacity changed", shared.NewPathCapacityChanged(pathId, at.Add(time.Hour), 4, true, at), "com.warehouse.wes.work-planning.workpool.PathCapacityChanged", "pick-zone-a", "pick-zone-a", ""},
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
