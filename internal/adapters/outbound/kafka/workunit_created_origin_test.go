package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ADR-0041: on the INTEGRATION stream WorkUnitCreated.data gains the optional
// `reference` and `line_no` (strictly additive, v1 stays v1). The analytics
// stream is a separate data product and stays unchanged.

func encodedData(t *testing.T, value []byte) map[string]any {
	t.Helper()
	var ce struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(value, &ce); err != nil {
		t.Fatalf("unmarshal CloudEvent: %v", err)
	}
	return ce.Data
}

func TestWorkUnitCreated_IntegrationDataCarriesOrigin(t *testing.T) {
	at := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	pathId := mustPathId(t, "pick-a")
	ev := shared.NewWorkUnitCreated("order-9-line-2", pathId, at).WithOrigin("order-9", 2)

	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, nil, nil, func() string { return "33333333-3333-4333-8333-333333333333" })
	encoded, err := pub.Encode(context.Background(), ev)
	if err != nil || len(encoded) != 1 {
		t.Fatalf("Encode = %d events, %v", len(encoded), err)
	}
	data := encodedData(t, encoded[0].Value)
	if data["path_id"] != "pick-a" || data["work_unit_id"] != "order-9-line-2" {
		t.Fatalf("identity fields changed: %v", data)
	}
	if data["reference"] != "order-9" {
		t.Fatalf("reference = %v, want order-9", data["reference"])
	}
	if data["line_no"] != float64(2) {
		t.Fatalf("line_no = %v, want 2", data["line_no"])
	}
}

func TestWorkUnitCreated_IntegrationDataOmitsEmptyOrigin(t *testing.T) {
	at := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	pathId := mustPathId(t, "pick-a")
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, nil, nil, func() string { return "33333333-3333-4333-8333-333333333333" })

	for name, ev := range map[string]shared.WorkUnitCreated{
		"no origin":        shared.NewWorkUnitCreated("wu-1", pathId, at),
		"reference only":   shared.NewWorkUnitCreated("wu-1", pathId, at).WithOrigin("plan-1", 0),
		"line without ref": shared.NewWorkUnitCreated("wu-1", pathId, at).WithOrigin("", 4),
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := pub.Encode(context.Background(), ev)
			if err != nil || len(encoded) != 1 {
				t.Fatalf("Encode = %d events, %v", len(encoded), err)
			}
			data := encodedData(t, encoded[0].Value)
			if _, ok := data["line_no"]; ok && ev.LineNo == 0 {
				t.Fatalf("line_no must be omitted when unknown, got %v", data)
			}
			if _, ok := data["reference"]; ok && ev.Reference == "" {
				t.Fatalf("reference must be omitted when empty, got %v", data)
			}
		})
	}
}

func TestWorkUnitCreated_AnalyticsDataIsUnchangedByOrigin(t *testing.T) {
	at := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	pathId := mustPathId(t, "pick-a")
	ev := shared.NewWorkUnitCreated("order-9-line-2", pathId, at).WithOrigin("order-9", 2)

	pub := outboundkafka.NewAnalyticsPublisherWithWriter(&fakeWriter{}, func() string { return "44444444-4444-4444-8444-444444444444" })
	encoded, err := pub.Encode(context.Background(), ev)
	if err != nil || len(encoded) != 1 {
		t.Fatalf("Encode = %d events, %v", len(encoded), err)
	}
	data := encodedData(t, encoded[0].Value)
	if len(data) != 2 || data["path_id"] != "pick-a" || data["work_unit_id"] != "order-9-line-2" {
		t.Fatalf("analytics data = %v, want exactly path_id and work_unit_id", data)
	}
}
