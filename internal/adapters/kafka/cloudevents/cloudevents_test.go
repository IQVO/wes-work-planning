package cloudevents_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
)

func TestType(t *testing.T) {
	got := cloudevents.Type("workunit", "WorkReleased")
	if want := "com.warehouse.wes.work-planning.workunit.WorkReleased"; got != want {
		t.Fatalf("Type = %q, want %q", got, want)
	}
}

func TestDataSchema(t *testing.T) {
	if got, want := cloudevents.DataSchema(cloudevents.StreamEvents, "WorkReleased", 1), "urn:warehouse:wes-work-planning:events:WorkReleased:v1"; got != want {
		t.Fatalf("DataSchema = %q, want %q", got, want)
	}
	if got, want := cloudevents.DataSchema(cloudevents.StreamAnalytics, "PathThrottled", 2), "urn:warehouse:wes-work-planning:analytics:PathThrottled:v2"; got != want {
		t.Fatalf("DataSchema = %q, want %q", got, want)
	}
}

func TestNew_ExactJSON(t *testing.T) {
	b, err := cloudevents.New(cloudevents.Spec{
		ID:        "11111111-1111-4111-8111-111111111111",
		Entity:    "workpool",
		EventName: "PathCapacityChanged",
		Subject:   "pick-a",
		Time:      time.Date(2026, 9, 30, 9, 0, 0, 0, time.FixedZone("-03", -3*3600)),
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      map[string]any{"path_id": "pick-a", "remaining_units": 3},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := `{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/wes-work-planning","type":"com.warehouse.wes.work-planning.workpool.PathCapacityChanged","subject":"pick-a","datacontenttype":"application/json","dataschema":"urn:warehouse:wes-work-planning:events:PathCapacityChanged:v1","time":"2026-09-30T12:00:00Z","data":{"path_id":"pick-a","remaining_units":3}}`
	if string(b) != want {
		t.Fatalf("New JSON\n got: %s\nwant: %s", b, want)
	}
}

func TestNew_RejectsEmptySubject(t *testing.T) {
	if _, err := cloudevents.New(cloudevents.Spec{ID: "x", Entity: "workpool", EventName: "PathThrottled", Time: time.Now(), Stream: cloudevents.StreamEvents, Data: map[string]any{}}); err == nil {
		t.Fatal("expected an error for an empty subject")
	}
}

func TestNew_DefaultsVersionToOne(t *testing.T) {
	b, err := cloudevents.New(cloudevents.Spec{ID: "x", Entity: "workpool", EventName: "PathThrottled", Subject: "p", Time: time.Now(), Stream: cloudevents.StreamAnalytics, Data: map[string]any{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m["dataschema"] != "urn:warehouse:wes-work-planning:analytics:PathThrottled:v1" {
		t.Fatalf("dataschema = %v", m["dataschema"])
	}
}

func TestDecode_RoundTrip(t *testing.T) {
	b, err := cloudevents.New(cloudevents.Spec{ID: "id-1", Entity: "workunit", EventName: "WorkReleased", Subject: "wu-1", Time: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC), Stream: cloudevents.StreamEvents, Data: map[string]any{"work_unit_id": "wu-1"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := cloudevents.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.ID() != "id-1" || e.Subject() != "wu-1" || e.Type() != "com.warehouse.wes.work-planning.workunit.WorkReleased" {
		t.Fatalf("unexpected decoded event %s", e)
	}
	var data struct {
		WorkUnitId string `json:"work_unit_id"`
	}
	if err := e.DataAs(&data); err != nil || data.WorkUnitId != "wu-1" {
		t.Fatalf("DataAs: %v %+v", err, data)
	}
}

func TestDecode_RejectsLegacyAndInvalid(t *testing.T) {
	cases := map[string]string{
		"legacy flat envelope": `{"event_id":"e1","event_type":"WorkReleased","occurred_at":"2026-09-30T12:00:00Z","source":"wes-work-planning","data":{}}`,
		"not json":             `not-json`,
		"wrong specversion":    `{"specversion":"0.3","id":"1","source":"/x","type":"t"}`,
		"missing id":           `{"specversion":"1.0","source":"/x","type":"t"}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := cloudevents.Decode([]byte(raw))
			if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
				t.Fatalf("Decode error = %v, want ErrNotCloudEvent", err)
			}
		})
	}
}

func TestContentTypeHeader(t *testing.T) {
	h := cloudevents.ContentTypeHeader()
	if h.Key != "content-type" || string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("unexpected header %+v", h)
	}
}
