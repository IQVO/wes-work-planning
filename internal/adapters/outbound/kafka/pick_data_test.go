package kafka

import (
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// pickUnitFixture builds the unit pickData reads (ADR-0045): an EACH_PICK
// work unit whose caller-stated plan carries SKU and quantity but neither
// a site nor a from_bin — the shape every REST pack-aware pick has when
// the caller supplied no bin.
func pickUnitFixture(t *testing.T) *workunit.WorkUnit {
	t.Helper()
	plan, err := workunit.NewPickPlan("SKU-1", 6, "EACH", "")
	if err != nil {
		t.Fatalf("NewPickPlan: %v", err)
	}
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	unit, err := workunit.NewWorkUnit("wu-pick-1", pathId, shared.NewCPT(time.Date(2026, 11, 5, 23, 0, 0, 0, time.UTC)), "order-1")
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	unit.SetPick(plan)
	return unit
}

// TestPickData_CarriesSiteWhenKnown is the PLAN.md wave-2 golden test
// (ADR-0048): a pick unit anchored to a site states it inside the pick
// object, omit-when-unknown, exactly like from_bin.
func TestPickData_CarriesSiteWhenKnown(t *testing.T) {
	unit := pickUnitFixture(t)
	unit.SetSiteId("WH1")
	got := pickData(unit)
	if got["site_id"] != "WH1" {
		t.Fatalf("site_id = %v, want WH1", got["site_id"])
	}
}

// TestPickData_OmitsSiteWhenUnknown is the PLAN.md wave-2 golden test
// (ADR-0048): a pick unit with no site omits the key entirely — absent,
// never an empty string — so a consumer that predates the field sees the
// bytes it always saw.
func TestPickData_OmitsSiteWhenUnknown(t *testing.T) {
	got := pickData(pickUnitFixture(t))
	if _, present := got["site_id"]; present {
		t.Fatal("site_id must be omitted when unknown")
	}
}

// TestPickData_CarriesFromBinWhenPlanned pins the from_bin half of the
// pick object: present when the plan has a bin, whether caller-stated or
// derived at release (ADR-0048).
func TestPickData_CarriesFromBinWhenPlanned(t *testing.T) {
	unit := pickUnitFixture(t)
	plan, err := workunit.NewPickPlan("SKU-1", 6, "EACH", "WH1-FWD-A01-01-02-A")
	if err != nil {
		t.Fatalf("NewPickPlan: %v", err)
	}
	unit.SetPick(plan)
	got := pickData(unit)
	if got["from_bin"] != "WH1-FWD-A01-01-02-A" {
		t.Fatalf("from_bin = %v, want WH1-FWD-A01-01-02-A", got["from_bin"])
	}
}

// TestPickData_OmitsFromBinWhenUnplanned: a plan with no bin — and no
// forward-pick face to derive one from — omits from_bin; the task then
// completes exactly as it does today (ADR-0048's omit-when-unknown).
func TestPickData_OmitsFromBinWhenUnplanned(t *testing.T) {
	got := pickData(pickUnitFixture(t))
	if _, present := got["from_bin"]; present {
		t.Fatal("from_bin must be omitted when no source bin was planned or derived")
	}
}

// TestPickData_NilForNonPickUnit: only CASE_PICK / EACH_PICK units carry a
// pick object; an order-driven unit with no plan (the common case before
// ADR-0048, and still after it) has none.
func TestPickData_NilForNonPickUnit(t *testing.T) {
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	unit, err := workunit.NewWorkUnit("wu-order-1", pathId, shared.NewCPT(time.Date(2026, 11, 5, 23, 0, 0, 0, time.UTC)), "order-1")
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	unit.SetSiteId("WH1")
	if got := pickData(unit); got != nil {
		t.Fatalf("pickData = %v, want nil for a unit with no pick plan", got)
	}
}
