package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0042: a released REPLENISH unit's WorkReleased carries ONE nested
// `replenishment` object in exactly the shape fulfillment-execution's
// REPLENISH task decodes (its ADR 0050, WorkReleasedReplenishment): the six
// members request_id, sku, from_bin, to_bin, quantity, priority.

// feReplenishment, feWorkReleased and feReplenishmentMove below are a
// field-for-field copy of fulfillment-execution's decoder
// (merged on origin/develop, internal/adapters/inbound/kafka/consumer.go
// WorkReleasedData / WorkReleasedReplenishment / replenishmentMoveOf, and
// internal/domain/task/replenishment.go NewReplenishmentMove): quantity is a pointer there so an absent value is
// rejected, not read as 0.
type feReplenishment struct {
	RequestId string `json:"request_id"`
	SKU       string `json:"sku"`
	FromBin   string `json:"from_bin"`
	ToBin     string `json:"to_bin"`
	Quantity  *int   `json:"quantity"`
	Priority  string `json:"priority"`
}

type feWorkReleased struct {
	PathId        string                    `json:"path_id"`
	WorkUnitId    string                    `json:"work_unit_id"`
	CPT           time.Time                 `json:"cpt"`
	Ref           string                    `json:"ref"`
	LineNo        int                       `json:"line_no,omitempty"`
	Fragile       bool                      `json:"fragile"`
	GiftWrap      bool                      `json:"gift_wrap"`
	TransferRef   string                    `json:"transfer_ref,omitempty"`
	WorkKind      string                    `json:"work_kind,omitempty"`
	SiteId        string                    `json:"site_id,omitempty"`
	SKU           string                    `json:"sku,omitempty"`
	Quantity      int                       `json:"quantity,omitempty"`
	Relocation    *feWorkReleasedRelocation `json:"relocation,omitempty"`
	Replenishment *feReplenishment          `json:"replenishment,omitempty"`
}

type feReplenishmentMove struct {
	RequestId, SKU, FromBin, ToBin, Priority string
	Quantity                                 int
}

// feNewReplenishmentMove is task.NewReplenishmentMove's validation.
func feNewReplenishmentMove(r *feReplenishment) (feReplenishmentMove, error) {
	if r.Quantity == nil {
		return feReplenishmentMove{}, errors.New("quantity is required")
	}
	m := feReplenishmentMove{
		RequestId: strings.TrimSpace(r.RequestId), SKU: strings.TrimSpace(r.SKU),
		FromBin: strings.TrimSpace(r.FromBin), ToBin: strings.TrimSpace(r.ToBin),
		Quantity: *r.Quantity, Priority: strings.TrimSpace(r.Priority),
	}
	if m.Priority != "NORMAL" && m.Priority != "URGENT" {
		return feReplenishmentMove{}, fmt.Errorf("unknown replenishment priority %q", m.Priority)
	}
	switch {
	case m.RequestId == "" || len(m.RequestId) > 200:
		return feReplenishmentMove{}, errors.New("invalid request_id")
	case m.SKU == "" || m.FromBin == "" || m.ToBin == "":
		return feReplenishmentMove{}, errors.New("sku and both bins are required")
	case m.FromBin == m.ToBin:
		return feReplenishmentMove{}, errors.New("from_bin equals to_bin")
	case m.Quantity < 1:
		return feReplenishmentMove{}, errors.New("quantity below 1")
	}
	return m, nil
}

// feCheckPairing is consume_work_released's checkReplenishment /
// checkRelocation: work on the REPLENISH path must carry the replenishment
// object and no relocation object; the object is refused on any other path.
func feCheckPairing(d feWorkReleased) error {
	onReplenish := strings.EqualFold(d.PathId, "replenish")
	if onReplenish && d.Replenishment == nil {
		return errors.New("ErrReplenishmentMoveMissing")
	}
	if !onReplenish && d.Replenishment != nil {
		return errors.New("ErrReplenishmentMoveOnOtherPath")
	}
	if onReplenish && d.Relocation != nil {
		return errors.New("ErrRelocationMoveOnOtherPath")
	}
	return nil
}

var replReleasedAt = time.Date(2026, 10, 17, 9, 45, 0, 0, time.UTC)

func releasedReplenishUnit(t *testing.T, requestId, sku, from, to string, quantity int, priority string, cpt time.Time) *memory.WorkUnitRepo {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	pathId, err := shared.NewPathId("REPLENISH")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	unit, err := workunit.NewWorkUnit(workunit.ReplenishmentWorkUnitId(requestId), pathId, shared.NewCPT(cpt), requestId)
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	move, err := workunit.NewReplenishmentMove(requestId, sku, from, to, quantity, priority)
	if err != nil {
		t.Fatalf("NewReplenishmentMove: %v", err)
	}
	unit.SetWorkKind(workunit.WorkKindReplenish)
	unit.SetSiteId("WH1")
	unit.SetReplenishment(move)
	if err := unit.Release(replReleasedAt); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return workUnits
}

func encodeReplenishRelease(t *testing.T, workUnits *memory.WorkUnitRepo, unitId string) []byte {
	t.Helper()
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "11111111-1111-4111-8111-111111111111" })
	pathId, _ := shared.NewPathId("REPLENISH")
	encoded, err := pub.Encode(context.Background(), shared.NewWorkReleased(unitId, pathId, replReleasedAt))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 || string(encoded[0].Key) != unitId {
		t.Fatalf("encoded = %d message(s), key %q, want one keyed by the unit id", len(encoded), encoded[0].Key)
	}
	return encoded[0].Value
}

func assertReplenishGolden(t *testing.T, value []byte, name string) {
	t.Helper()
	var got bytes.Buffer
	if err := json.Indent(&got, value, "", "  "); err != nil {
		t.Fatalf("indent: %v", err)
	}
	got.WriteByte('\n')
	want, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("%s wire shape changed\n--- got\n%s\n--- want\n%s", name, got.String(), want)
	}
}

func TestCloudEvents_Golden_WorkReleased_ReplenishNormal(t *testing.T) {
	const req = "repl-3b0e8d2a-7c41-4f6a-9d15-2a8b6c4e1f07"
	workUnits := releasedReplenishUnit(t, req, "SKU-1", "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-01-A", 24, "NORMAL", time.Date(2026, 10, 17, 17, 30, 0, 0, time.UTC))
	assertReplenishGolden(t, encodeReplenishRelease(t, workUnits, "replenish-"+req), "ce_events_WorkReleasedReplenishmentNormal.golden.json")
}

func TestCloudEvents_Golden_WorkReleased_ReplenishUrgent(t *testing.T) {
	const req = "repl-9c2f6b1e-5d3a-4e8b-a7f0-1b2c3d4e5f60"
	workUnits := releasedReplenishUnit(t, req, "SKU-2", "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-02-A", 6, "URGENT", time.Date(2026, 10, 17, 10, 30, 0, 0, time.UTC))
	assertReplenishGolden(t, encodeReplenishRelease(t, workUnits, "replenish-"+req), "ce_events_WorkReleasedReplenishmentUrgent.golden.json")
}

func decodeAsFEWorkReleased(t *testing.T, value []byte) feWorkReleased {
	t.Helper()
	var env struct {
		Data feWorkReleased `json:"data"`
	}
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatalf("decode as fulfillment-execution: %v", err)
	}
	return env.Data
}

// The payload decodes through fulfillment-execution's decoder UNCHANGED:
// every member lands in its field, the move validates, the REPLENISH-path
// pairing rule passes, and none of the transfer or relocation blocks leak.
func TestPublisher_WorkReleased_ReplenishmentDecodesAsFulfillmentExecutionMove(t *testing.T) {
	for _, priority := range []string{"NORMAL", "URGENT"} {
		t.Run(priority, func(t *testing.T) {
			workUnits := releasedReplenishUnit(t, "req-9", "SKU-1", "WH1-RSV-AMB-B02-03-02-C", "WH1-FWD-AMB-A01-01-01-A", 24, priority, replReleasedAt.Add(8*time.Hour))
			d := decodeAsFEWorkReleased(t, encodeReplenishRelease(t, workUnits, "replenish-req-9"))
			if err := feCheckPairing(d); err != nil {
				t.Fatalf("pairing rule: %v", err)
			}
			move, err := feNewReplenishmentMove(d.Replenishment)
			if err != nil {
				t.Fatalf("fulfillment-execution would reject the move: %v", err)
			}
			want := feReplenishmentMove{RequestId: "req-9", SKU: "SKU-1", FromBin: "WH1-RSV-AMB-B02-03-02-C", ToBin: "WH1-FWD-AMB-A01-01-01-A", Priority: priority, Quantity: 24}
			if move != want {
				t.Fatalf("move = %+v, want %+v", move, want)
			}
			if d.PathId != "REPLENISH" || d.WorkUnitId != "replenish-req-9" || d.Ref != "req-9" || !d.CPT.Equal(replReleasedAt.Add(8*time.Hour)) {
				t.Fatalf("envelope = %+v", d)
			}
			if d.Relocation != nil || d.TransferRef != "" || d.WorkKind != "" || d.SiteId != "" || d.SKU != "" || d.Quantity != 0 {
				t.Fatalf("a non-replenishment block leaked onto a REPLENISH release: %+v", d)
			}
		})
	}
}

// The object has exactly the six members fulfillment-execution reads: no
// more (nothing it would ignore silently), no fewer.
func TestPublisher_WorkReleased_ReplenishmentHasExactlyTheSixDecodedMembers(t *testing.T) {
	workUnits := releasedReplenishUnit(t, "req-1", "SKU-1", "A", "B", 1, "NORMAL", replReleasedAt.Add(time.Hour))
	var env struct {
		Data struct {
			Replenishment map[string]any `json:"replenishment"`
		} `json:"data"`
	}
	if err := json.Unmarshal(encodeReplenishRelease(t, workUnits, "replenish-req-1"), &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := env.Data.Replenishment
	for _, member := range []string{"request_id", "sku", "from_bin", "to_bin", "quantity", "priority"} {
		if _, ok := got[member]; !ok {
			t.Fatalf("member %q missing from %v", member, got)
		}
	}
	if len(got) != 6 {
		t.Fatalf("replenishment = %v, want exactly six members", got)
	}
}

// fulfillment-execution's rules reject what this adapter must never produce:
// the guard that the copied decoder is not vacuous.
func TestFulfillmentExecutionDecoderCopy_RejectsBrokenReplenishments(t *testing.T) {
	one := 1
	zero := 0
	cases := map[string]feReplenishment{
		"no quantity":      {RequestId: "r", SKU: "S", FromBin: "A", ToBin: "B", Priority: "NORMAL"},
		"zero quantity":    {RequestId: "r", SKU: "S", FromBin: "A", ToBin: "B", Quantity: &zero, Priority: "NORMAL"},
		"same bins":        {RequestId: "r", SKU: "S", FromBin: "A", ToBin: "A", Quantity: &one, Priority: "NORMAL"},
		"unknown priority": {RequestId: "r", SKU: "S", FromBin: "A", ToBin: "B", Quantity: &one, Priority: "HIGH"},
		"no request id":    {SKU: "S", FromBin: "A", ToBin: "B", Quantity: &one, Priority: "NORMAL"},
	}
	for name, r := range cases {
		if _, err := feNewReplenishmentMove(&r); err == nil {
			t.Fatalf("%s: the copied decoder accepted a broken move", name)
		}
	}
	if err := feCheckPairing(feWorkReleased{PathId: "REPLENISH", Relocation: &feWorkReleasedRelocation{}, Replenishment: &feReplenishment{}}); err == nil {
		t.Fatal("a relocation object on the REPLENISH path must be rejected")
	}
}

// Only a REPLENISH unit carries the object; order, transfer and RELOCATE
// releases are byte-for-byte what they were.
func TestPublisher_WorkReleased_OtherUnitsHaveNoReplenishmentObject(t *testing.T) {
	for _, workUnits := range []*memory.WorkUnitRepo{
		newReleasedWorkUnitWithGiftWrap(t, "wu-plain-repl", "sku-plain", false),
		newReleasedTransferUnit(t, "wu-plain-repl"),
		releasedRelocationUnit(t, "wu-plain-repl", "plan-x", 0, "Relocate", "S2", "S3", "SKU-7"),
	} {
		data := publishAndDecode(t, workUnits, "wu-plain-repl")
		if _, ok := data["replenishment"]; ok {
			t.Fatalf("replenishment must not appear on order, transfer or RELOCATE work: %v", data)
		}
	}
}
