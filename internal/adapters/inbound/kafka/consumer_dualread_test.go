package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/envelope"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0021 Phase 2, Task 2c: wes-work-planning's fulfillmentReader must
// dual-read fulfillment-execution's TaskCompleted event in either the
// legacy flat envelope shape or the new CloudEvents 1.0 structured envelope
// shape, normalizing both to the identical envelope.Envelope the existing
// handleFulfillmentEvent logic already consumes unchanged.

// flatTaskCompletedFixture returns a raw flat-envelope-shaped TaskCompleted
// message, byte-identical in structure to what fulfillment-execution
// publishes today.
func flatTaskCompletedFixture(t *testing.T, eventId, workUnitId string) []byte {
	t.Helper()
	data, err := json.Marshal(taskCompletedData{TaskId: "task-1", StationId: "station-1", WorkUnitId: workUnitId})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	raw, err := json.Marshal(envelope.Envelope{
		EventId:    eventId,
		EventType:  envelope.EventTypeTaskCompleted,
		OccurredAt: time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC),
		Source:     "fulfillment-execution",
		Data:       data,
	})
	if err != nil {
		t.Fatalf("marshal flat envelope: %v", err)
	}
	return raw
}

// cloudEventTaskCompletedFixture returns a raw CloudEvents 1.0-shaped
// TaskCompleted message, mirroring fulfillment-execution's own
// apis/asyncapi.yaml worked example verbatim (the real
// "com.warehouse.wes.fulfillment-execution.task.TaskCompleted" type
// string), so the test fails if the code ever drifts from the real schema.
func cloudEventTaskCompletedFixture(t *testing.T, eventId, workUnitId string) []byte {
	t.Helper()
	data, err := json.Marshal(taskCompletedData{TaskId: "task-1", StationId: "station-1", WorkUnitId: workUnitId})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	raw, err := json.Marshal(cloudEventEnvelope{
		SpecVersion:     "1.0",
		Id:              eventId,
		Type:            "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		Source:          "/warehouse/fulfillment-execution",
		Subject:         "task-1",
		Time:            time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC),
		DataContentType: "application/json",
		Data:            data,
	})
	if err != nil {
		t.Fatalf("marshal cloudevents envelope: %v", err)
	}
	return raw
}

// Test 1 (regression): decoding a flat-shaped TaskCompleted fixture
// produces the same normalized envelope.Envelope as today.
func TestDecodeFulfillmentEnvelope_FlatShape_RegressesToTodaysResult(t *testing.T) {
	raw := flatTaskCompletedFixture(t, "evt-flat-1", "wu-1")

	env, err := decodeFulfillmentEnvelope(raw)
	if err != nil {
		t.Fatalf("decodeFulfillmentEnvelope: %v", err)
	}

	if env.EventId != "evt-flat-1" {
		t.Fatalf("got EventId %q, want %q", env.EventId, "evt-flat-1")
	}
	if env.EventType != envelope.EventTypeTaskCompleted {
		t.Fatalf("got EventType %q, want %q", env.EventType, envelope.EventTypeTaskCompleted)
	}
	if !env.OccurredAt.Equal(time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("got OccurredAt %v, want 2026-08-21T09:00:00Z", env.OccurredAt)
	}
	if env.Source != "fulfillment-execution" {
		t.Fatalf("got Source %q, want %q", env.Source, "fulfillment-execution")
	}

	var data taskCompletedData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if data.WorkUnitId != "wu-1" {
		t.Fatalf("got WorkUnitId %q, want %q", data.WorkUnitId, "wu-1")
	}
}

// Test 2: decoding a CloudEvents-shaped TaskCompleted fixture (the real
// schema/type string from fulfillment-execution's apis/asyncapi.yaml)
// normalizes to the SAME result as the equivalent flat fixture.
func TestDecodeFulfillmentEnvelope_CloudEventsShape_MatchesFlatEquivalent(t *testing.T) {
	flatEnv, err := decodeFulfillmentEnvelope(flatTaskCompletedFixture(t, "evt-same-id", "wu-1"))
	if err != nil {
		t.Fatalf("decodeFulfillmentEnvelope (flat): %v", err)
	}

	ceEnv, err := decodeFulfillmentEnvelope(cloudEventTaskCompletedFixture(t, "evt-same-id", "wu-1"))
	if err != nil {
		t.Fatalf("decodeFulfillmentEnvelope (cloudevents): %v", err)
	}

	if ceEnv.EventId != flatEnv.EventId {
		t.Fatalf("got EventId %q, want %q (flat equivalent)", ceEnv.EventId, flatEnv.EventId)
	}
	if ceEnv.EventType != flatEnv.EventType {
		t.Fatalf("got EventType %q, want %q (flat equivalent)", ceEnv.EventType, flatEnv.EventType)
	}
	if ceEnv.EventType != envelope.EventTypeTaskCompleted {
		t.Fatalf("got EventType %q, want the bare name %q (reverse-DNS prefix must be stripped)", ceEnv.EventType, envelope.EventTypeTaskCompleted)
	}
	if !ceEnv.OccurredAt.Equal(flatEnv.OccurredAt) {
		t.Fatalf("got OccurredAt %v, want %v (flat equivalent)", ceEnv.OccurredAt, flatEnv.OccurredAt)
	}
	if string(ceEnv.Data) != string(flatEnv.Data) {
		t.Fatalf("got Data %s, want byte-identical %s (flat equivalent)", ceEnv.Data, flatEnv.Data)
	}
}

// Test 3: a malformed/unrecognized specversion fails soft, mirroring this
// repo's existing malformed-message posture (handleMessage: log and
// commit, never redeliver forever) rather than crashing the consume loop.
func TestDecodeFulfillmentEnvelope_UnrecognizedSpecVersion_FailsSoft(t *testing.T) {
	raw := []byte(`{"specversion":"2.0","id":"evt-bad","type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted","source":"/warehouse/fulfillment-execution","time":"2026-08-21T09:00:00Z","datacontenttype":"application/json","data":{"task_id":"task-1","station_id":"station-1","work_unit_id":"wu-1"}}`)

	_, err := decodeFulfillmentEnvelope(raw)
	if err == nil {
		t.Fatalf("expected an error for an unrecognized specversion, got nil")
	}
}

// TestDecodeFulfillmentEnvelope_MalformedJSON_FailsSoft covers the other
// malformed case this repo already treats as fail-soft: JSON that doesn't
// even parse as an object.
func TestDecodeFulfillmentEnvelope_MalformedJSON_FailsSoft(t *testing.T) {
	_, err := decodeFulfillmentEnvelope([]byte(`not json`))
	if err == nil {
		t.Fatalf("expected an error for malformed JSON, got nil")
	}
}

// TestHandleFulfillmentMessage_CloudEventsShape_CompletesTheWorkUnit proves
// the full handoff: a raw CloudEvents-shaped message on the real
// fulfillmentReader path drives the exact same RecordCompletion use case a
// flat message already does, through handleFulfillmentMessage /
// handleFulfillmentEvent — no duplicated business logic across the two
// decode paths.
func TestHandleFulfillmentMessage_CloudEventsShape_CompletesTheWorkUnit(t *testing.T) {
	f := newFulfillmentFixture()

	env, err := decodeFulfillmentEnvelope(cloudEventTaskCompletedFixture(t, "evt-ce-1", "wu-1"))
	if err != nil {
		t.Fatalf("decodeFulfillmentEnvelope: %v", err)
	}

	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "wu-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.State() != workunit.Completed {
		t.Fatalf("expected work unit to be Completed, got %v", unit.State())
	}
}

// TestHandleFulfillmentEvent_SameEventId_FlatThenCloudEvents_ProcessesOnce
// is the unit-level analogue of the integration test's idempotency
// assertion: the SAME event_id/id arriving once in each shape must only
// invoke RecordCompletion once, via the existing processed_events gate —
// load-bearing for the later dual-write phase (ADR-0021 §3).
func TestHandleFulfillmentEvent_SameEventId_FlatThenCloudEvents_ProcessesOnce(t *testing.T) {
	f := newFulfillmentFixture()

	flatEnv, err := decodeFulfillmentEnvelope(flatTaskCompletedFixture(t, "evt-dual-shape", "wu-1"))
	if err != nil {
		t.Fatalf("decodeFulfillmentEnvelope (flat): %v", err)
	}
	ceEnv, err := decodeFulfillmentEnvelope(cloudEventTaskCompletedFixture(t, "evt-dual-shape", "wu-1"))
	if err != nil {
		t.Fatalf("decodeFulfillmentEnvelope (cloudevents): %v", err)
	}

	if err := f.consumer.handleFulfillmentEvent(context.Background(), flatEnv); err != nil {
		t.Fatalf("first handleFulfillmentEvent (flat): %v", err)
	}
	// The CloudEvents-shaped sibling carries the same event_id — the
	// processed_events gate must no-op it, not attempt a second
	// RecordCompletion (which would surface workunit.ErrAlreadyCompleted).
	if err := f.consumer.handleFulfillmentEvent(context.Background(), ceEnv); err != nil {
		t.Fatalf("second handleFulfillmentEvent (cloudevents, same event_id): %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "wu-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.State() != workunit.Completed {
		t.Fatalf("expected work unit to be Completed, got %v", unit.State())
	}
}
