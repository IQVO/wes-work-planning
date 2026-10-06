package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// TestCloudEvents_Golden_WorkReleased_LegacyShapeIsByteCompatible is the
// ADR-0033 backward-compatibility pin: the WorkReleased payload of a
// NON-transfer work unit must be byte-identical to the golden file this
// contract has always had. The transfer fields are strictly additive — a
// consumer that has never heard of them must see exactly the bytes it saw
// before transfer support existed. The golden fixture (wu-gold, sku-plain,
// gift_wrap=true) is deliberately a NON-transfer unit, so comparing against
// the encoder's live output for the same fixture proves no new key appeared.
func TestCloudEvents_Golden_WorkReleased_LegacyShapeIsByteCompatible(t *testing.T) {
	// The same fixture the existing golden was generated from: released,
	// sku-plain, gift_wrap=true, NO transfer metadata.
	workUnits := newReleasedWorkUnitWithGiftWrap(t, "wu-gold", "sku-plain", true)
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "11111111-1111-4111-8111-111111111111" })

	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	encoded, err := pub.Encode(context.Background(), shared.NewWorkReleased("wu-gold", pathId, time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("got %d encoded, want 1", len(encoded))
	}

	var got bytes.Buffer
	if err := json.Indent(&got, encoded[0].Value, "", "  "); err != nil {
		t.Fatalf("indent: %v", err)
	}
	got.WriteByte('\n')

	wantBytes, err := os.ReadFile(filepath.Join("testdata", "ce_events_WorkReleased.golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if !bytes.Equal(got.Bytes(), wantBytes) {
		t.Fatalf("legacy WorkReleased shape changed for a non-transfer unit\n--- got\n%s\n--- want\n%s", got.String(), wantBytes)
	}

	// And structurally: none of the additive transfer keys may appear.
	var attrs map[string]any
	if err := json.Unmarshal(encoded[0].Value, &attrs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	data, _ := attrs["data"].(map[string]any)
	for _, key := range []string{"transfer_ref", "work_kind", "site_id", "quantity"} {
		if _, ok := data[key]; ok {
			t.Errorf("transfer key %q must not appear on a non-transfer unit", key)
		}
	}
	// The envelope identity is unchanged too: same v1 type + dataschema.
	if attrs["type"] != "com.warehouse.wes.work-planning.workunit.WorkReleased" {
		t.Errorf("type = %v", attrs["type"])
	}
	if attrs["dataschema"] != "urn:warehouse:wes-work-planning:events:WorkReleased:v1" {
		t.Errorf("dataschema = %v", attrs["dataschema"])
	}
}

// TestCloudEvents_Golden_WorkReleased_TransferUnit pins the WorkReleased
// wire shape for a transfer-referenced unit: same v1 type and dataschema
// (additive only, no version bump), with the four OPTIONAL transfer fields
// present in data.
func TestCloudEvents_Golden_WorkReleased_TransferUnit(t *testing.T) {
	workUnits := newReleasedTransferUnit(t, "wu-gold-xfer")
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "11111111-1111-4111-8111-111111111111" })

	pathId, _ := shared.NewPathId("pick-transfer-a")
	encoded, err := pub.Encode(context.Background(), shared.NewWorkReleased("wu-gold-xfer", pathId, time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("got %d encoded, want 1", len(encoded))
	}
	if string(encoded[0].Key) != "wu-gold-xfer" {
		t.Errorf("key = %q, want the aggregate id (ADR-0024)", encoded[0].Key)
	}

	var attrs map[string]any
	if err := json.Unmarshal(encoded[0].Value, &attrs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// SAME v1 identity — additive only, never a new type/dataschema.
	if attrs["type"] != "com.warehouse.wes.work-planning.workunit.WorkReleased" {
		t.Errorf("type = %v", attrs["type"])
	}
	if attrs["dataschema"] != cloudevents.DataSchema(cloudevents.StreamEvents, "WorkReleased", 1) {
		t.Errorf("dataschema = %v", attrs["dataschema"])
	}
	data, _ := attrs["data"].(map[string]any)
	if data["transfer_ref"] != "TRF-2026-042" {
		t.Errorf("transfer_ref = %v", data["transfer_ref"])
	}
	if data["work_kind"] != "TRANSFER_PICK" {
		t.Errorf("work_kind = %v", data["work_kind"])
	}
	if data["site_id"] != "site-north-1" {
		t.Errorf("site_id = %v", data["site_id"])
	}
	if data["quantity"] != float64(17) {
		t.Errorf("quantity = %v", data["quantity"])
	}
	// Required base fields still present on the transfer payload.
	for _, key := range []string{"path_id", "work_unit_id", "cpt", "ref"} {
		if _, ok := data[key]; !ok {
			t.Errorf("required key %q missing from transfer payload", key)
		}
	}
}
