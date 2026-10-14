package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0040: a released RELOCATE unit's WorkReleased carries ONE nested
// `relocation` object in exactly the shape fulfillment-execution's
// WorkReleased consumer decodes (its ADR 0047, WorkReleasedRelocation):
// plan_id, move_seq (required, 0 included), kind, sku, from_bin?, to_bin?.

// feWorkReleasedRelocation mirrors fulfillment-execution's decoder struct
// field for field (internal/adapters/inbound/kafka WorkReleasedRelocation):
// move_seq is a pointer there so an absent value is rejected.
type feWorkReleasedRelocation struct {
	PlanId  string `json:"plan_id"`
	MoveSeq *int   `json:"move_seq"`
	Kind    string `json:"kind"`
	SKU     string `json:"sku"`
	FromBin string `json:"from_bin,omitempty"`
	ToBin   string `json:"to_bin,omitempty"`
}

type feWorkReleasedData struct {
	PathId      string                    `json:"path_id"`
	WorkUnitId  string                    `json:"work_unit_id"`
	Ref         string                    `json:"ref"`
	TransferRef string                    `json:"transfer_ref,omitempty"`
	WorkKind    string                    `json:"work_kind,omitempty"`
	Relocation  *feWorkReleasedRelocation `json:"relocation,omitempty"`
}

func releasedRelocationUnit(t *testing.T, id, planId string, seq int, kind, from, to, sku string) *memory.WorkUnitRepo {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	pathId, err := shared.NewPathId("RELOCATE")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	unit, err := workunit.NewWorkUnit(id, pathId, shared.NewCPT(time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)), planId)
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	move, err := workunit.NewRelocationMove(planId, seq, kind, from, to)
	if err != nil {
		t.Fatalf("NewRelocationMove: %v", err)
	}
	unit.SetWorkKind(workunit.WorkKindRelocate)
	unit.SetSiteId("SITE-1")
	unit.SetSKU(sku)
	unit.SetRelocation(move)
	if err := unit.Release(time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return workUnits
}

func encodeRelocationRelease(t *testing.T, workUnits *memory.WorkUnitRepo, id string) []byte {
	t.Helper()
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "11111111-1111-4111-8111-111111111111" })
	pathId, _ := shared.NewPathId("RELOCATE")
	encoded, err := pub.Encode(context.Background(), shared.NewWorkReleased(id, pathId, time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 || string(encoded[0].Key) != id {
		t.Fatalf("encoded = %d messages, key %q", len(encoded), encoded[0].Key)
	}
	return encoded[0].Value
}

func TestCloudEvents_Golden_WorkReleased_RelocateUnit(t *testing.T) {
	workUnits := releasedRelocationUnit(t, "plan-3b0e8d2a-move-1", "plan-3b0e8d2a", 1, "Relocate", "WH1-FWD-A02-03-01-A", "WH1-FWD-A01-01-02-A", "SKU-2")
	value := encodeRelocationRelease(t, workUnits, "plan-3b0e8d2a-move-1")

	var got bytes.Buffer
	if err := json.Indent(&got, value, "", "  "); err != nil {
		t.Fatalf("indent: %v", err)
	}
	got.WriteByte('\n')
	want, err := os.ReadFile(filepath.Join("testdata", "ce_events_WorkReleasedRelocation.golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("RELOCATE WorkReleased wire shape changed\n--- got\n%s\n--- want\n%s", got.String(), want)
	}
}

func decodeAsFulfillmentExecution(t *testing.T, value []byte) feWorkReleasedData {
	t.Helper()
	var env struct {
		Data feWorkReleasedData `json:"data"`
	}
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatalf("decode as fulfillment-execution: %v", err)
	}
	return env.Data
}

func TestPublisher_WorkReleased_RelocationDecodesAsFulfillmentExecutionMove(t *testing.T) {
	cases := []struct {
		name, kind, from, to string
		seq                  int
	}{
		{"vacate: move 0, no to_bin", "Vacate", "S3", "", 0},
		{"assign: no from_bin", "Assign", "", "S1", 2},
		{"relocate: both bins", "Relocate", "S2", "S3", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := "plan-x-move"
			data := decodeAsFulfillmentExecution(t, encodeRelocationRelease(t, releasedRelocationUnit(t, id, "plan-x", tc.seq, tc.kind, tc.from, tc.to, "SKU-7"), id))
			r := data.Relocation
			if r == nil || r.MoveSeq == nil {
				t.Fatalf("relocation = %+v: the object and move_seq must be present", r)
			}
			if r.PlanId != "plan-x" || *r.MoveSeq != tc.seq || r.Kind != tc.kind || r.SKU != "SKU-7" || r.FromBin != tc.from || r.ToBin != tc.to {
				t.Fatalf("relocation = %+v (move_seq %d)", r, *r.MoveSeq)
			}
			if data.PathId != "RELOCATE" || data.Ref != "plan-x" {
				t.Fatalf("path_id=%q ref=%q", data.PathId, data.Ref)
			}
			// The transfer discriminators stay off RELOCATE work, so
			// fulfillment-execution's transfer branch never fires.
			if data.TransferRef != "" || data.WorkKind != "" {
				t.Fatalf("transfer fields leaked onto a RELOCATE release: %+v", data)
			}
		})
	}
}

func TestPublisher_WorkReleased_NonRelocateUnitHasNoRelocationObject(t *testing.T) {
	for _, workUnits := range []*memory.WorkUnitRepo{
		newReleasedWorkUnitWithGiftWrap(t, "wu-plain-reloc", "sku-plain", false),
		newReleasedTransferUnit(t, "wu-plain-reloc"),
	} {
		data := publishAndDecode(t, workUnits, "wu-plain-reloc")
		if _, ok := data["relocation"]; ok {
			t.Fatalf("relocation must not appear on order or transfer work: %v", data)
		}
	}
}
