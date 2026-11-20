package kafka_test

import (
	"context"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0048: WorkReleased carries the site on ALL work kinds. These tests
// pin the payload-level behaviour the golden pickData tests cannot: the
// top-level site_id of an ORDER unit (site without transfer context), the
// pick object's site_id, and the unchanged transfer shape.

// newReleasedOrderUnit stores a released ORDER-driven unit the way
// ApplyOrderAllocated (with the event's additive site_id) plus
// ReleaseNextWork would have: SKU, line_no, a site, no transfer metadata,
// no pick plan.
func newReleasedOrderUnit(t *testing.T, id, siteId string) *memory.WorkUnitRepo {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	cpt := shared.NewCPT(time.Date(2026, 11, 5, 23, 0, 0, 0, time.UTC))
	opts := []workunit.Option{workunit.WithSKU("sku-order"), workunit.WithLineNo(3)}
	if siteId != "" {
		opts = append(opts, workunit.WithSiteId(siteId))
	}
	unit, err := workunit.NewWorkUnit(id, pathId, cpt, "order-"+id, opts...)
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	if err := unit.Release(time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return workUnits
}

// newReleasedPickUnit stores a released EACH_PICK unit (ADR-0045) with a
// caller-stated plan and a site, the shape a site-scoped REST pick has.
func newReleasedPickUnit(t *testing.T, id, siteId, fromBin string) *memory.WorkUnitRepo {
	t.Helper()
	plan, err := workunit.NewPickPlan("sku-pick", 6, "EACH", fromBin)
	if err != nil {
		t.Fatalf("NewPickPlan: %v", err)
	}
	workUnits := memory.NewWorkUnitRepo()
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	cpt := shared.NewCPT(time.Date(2026, 11, 5, 23, 0, 0, 0, time.UTC))
	opts := []workunit.Option{workunit.WithPick(plan)}
	if siteId != "" {
		opts = append(opts, workunit.WithSiteId(siteId))
	}
	unit, err := workunit.NewWorkUnit(id, pathId, cpt, "order-"+id, opts...)
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	if err := unit.Release(time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return workUnits
}

func publishAndDecodeUnit(t *testing.T, workUnits *memory.WorkUnitRepo, workUnitId string) map[string]any {
	t.Helper()
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-pick-site" })
	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased(workUnitId, pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return decodeWorkReleasedData(t, writer.msgs[0])
}

// TestPublisher_WorkReleased_OrderUnitCarriesTopLevelSite: the ADR-0048
// headline — an ORDER unit's payload states site_id top-level, with none
// of the transfer context fields around it (no work_kind, no
// transfer_ref, no quantity).
func TestPublisher_WorkReleased_OrderUnitCarriesTopLevelSite(t *testing.T) {
	data := publishAndDecodeUnit(t, newReleasedOrderUnit(t, "wu-order-site", "WH1"), "wu-order-site")

	if data["site_id"] != "WH1" {
		t.Errorf("site_id = %v, want WH1", data["site_id"])
	}
	for _, field := range []string{"work_kind", "transfer_ref", "quantity"} {
		if _, ok := data[field]; ok {
			t.Errorf("did not expect transfer field %s on an order unit, got %v", field, data)
		}
	}
	if data["ref"] != "order-wu-order-site" || data["line_no"] != float64(3) {
		t.Errorf("base fields regressed: %v", data)
	}
}

// TestPublisher_WorkReleased_OrderUnitWithoutSiteOmitsIt: a unit whose
// order event predates the site field (or carried none) keeps the exact
// pre-ADR-0048 payload — site_id absent, not empty.
func TestPublisher_WorkReleased_OrderUnitWithoutSiteOmitsIt(t *testing.T) {
	data := publishAndDecodeUnit(t, newReleasedOrderUnit(t, "wu-order-nosite", ""), "wu-order-nosite")

	if _, ok := data["site_id"]; ok {
		t.Errorf("site_id must be omitted when the unit has no site, got %v", data)
	}
}

// TestPublisher_WorkReleased_PickCarriesSiteAndFromBin: the pick object
// states the unit's site and the planned bin together, and the top-level
// site_id appears for pick work too (it is not transfer-only).
func TestPublisher_WorkReleased_PickCarriesSiteAndFromBin(t *testing.T) {
	data := publishAndDecodeUnit(t, newReleasedPickUnit(t, "wu-pick-site", "WH1", "WH1-FWD-A01-01-02-A"), "wu-pick-site")

	pick, ok := data["pick"].(map[string]any)
	if !ok {
		t.Fatalf("pick object missing: %v", data)
	}
	if pick["site_id"] != "WH1" {
		t.Errorf("pick.site_id = %v, want WH1", pick["site_id"])
	}
	if pick["from_bin"] != "WH1-FWD-A01-01-02-A" {
		t.Errorf("pick.from_bin = %v, want WH1-FWD-A01-01-02-A", pick["from_bin"])
	}
	if pick["sku"] != "sku-pick" || pick["quantity"] != float64(6) || pick["unit"] != "EACH" {
		t.Errorf("pick base members regressed: %v", pick)
	}
	if data["site_id"] != "WH1" {
		t.Errorf("top-level site_id = %v, want WH1 (not transfer-only, ADR-0048)", data["site_id"])
	}
}

// TestPublisher_WorkReleased_PickWithoutSiteOmitsPickSite: the pick object
// omits site_id when the unit has none; from_bin may still be present.
func TestPublisher_WorkReleased_PickWithoutSiteOmitsPickSite(t *testing.T) {
	data := publishAndDecodeUnit(t, newReleasedPickUnit(t, "wu-pick-nosite", "", "BIN-A1"), "wu-pick-nosite")

	pick, ok := data["pick"].(map[string]any)
	if !ok {
		t.Fatalf("pick object missing: %v", data)
	}
	if _, present := pick["site_id"]; present {
		t.Errorf("pick.site_id must be omitted when unknown, got %v", pick)
	}
	if pick["from_bin"] != "BIN-A1" {
		t.Errorf("pick.from_bin = %v, want BIN-A1", pick["from_bin"])
	}
	if _, present := data["site_id"]; present {
		t.Errorf("top-level site_id must be omitted when the unit has no site, got %v", data)
	}
}

// TestPublisher_WorkReleased_TransferSiteBehaviourUnchanged: dropping the
// transfer-only gate changed WHERE site_id is stamped, not WHAT a transfer
// unit publishes — the ADR-0033 transfer shape is pinned separately by
// TestCloudEvents_Golden_WorkReleased_TransferUnit; this re-states the
// site half against the shared publisher entry point.
func TestPublisher_WorkReleased_TransferSiteBehaviourUnchanged(t *testing.T) {
	data := publishAndDecodeUnit(t, newReleasedTransferUnit(t, "wu-xfer-site"), "wu-xfer-site")

	if data["site_id"] != "site-north-1" {
		t.Errorf("site_id = %v, want site-north-1", data["site_id"])
	}
	if data["work_kind"] != "TRANSFER_PICK" || data["transfer_ref"] != "TRF-2026-042" || data["quantity"] != float64(17) {
		t.Errorf("transfer context regressed: %v", data)
	}
}
