package usecases_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/pickload"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/traveldistanceview"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0045: pack-aware work. These tests drive the real use cases over the
// in-memory adapters (unit-test doubles); the Postgres and Kafka paths are
// proved by the integration tests.

const (
	caseGTIN = "24006381333935"
	eachGTIN = "04006381333931"
)

var packClock = time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)

// packCopies is a memory pack copy fed through the real ObservePackLevel.
type packCopies struct {
	store   *productclassificationcopy.MemoryStore
	observe *usecases.ObservePackLevel
	seq     int
}

func newPackCopies() *packCopies {
	store := productclassificationcopy.NewMemoryStore()
	return &packCopies{store: store, observe: usecases.NewObservePackLevel(store, memory.NewProcessedEventRepo())}
}

func (p *packCopies) apply(t *testing.T, ev usecases.PackLevelEvent, rec productclassificationview.PackLevelRecord) usecases.PackLevelOutcome {
	t.Helper()
	p.seq++
	out, err := p.observe.Execute(context.Background(), usecases.ObservePackLevelRequest{
		EventId: "pack-evt-" + string(rune('a'+p.seq)), Event: ev, Record: rec, ObservedAt: packClock,
	})
	if err != nil {
		t.Fatalf("observe pack level: %v", err)
	}
	return out
}

func caseRecord(version int64) productclassificationview.PackLevelRecord {
	return productclassificationview.PackLevelRecord{
		SKU: "SKU-1", Level: productclassificationview.PackLevelCase, GTIN: caseGTIN, ChildGTIN: eachGTIN, QtyOfChild: 12, UnitsPerPack: 12,
		Declared:    productclassificationview.Dims{LengthMm: 400, WidthMm: 300, HeightMm: 200, WeightG: 6500},
		IsOrderable: true, IsDespatch: true, EffectiveFrom: packClock.Add(-time.Hour), Version: version,
	}
}

func eachRecord(version int64) productclassificationview.PackLevelRecord {
	return productclassificationview.PackLevelRecord{
		SKU: "SKU-1", Level: productclassificationview.PackLevelEach, GTIN: eachGTIN, UnitsPerPack: 1,
		Declared:    productclassificationview.Dims{LengthMm: 100, WidthMm: 80, HeightMm: 60, WeightG: 450},
		IsOrderable: true, EffectiveFrom: packClock.Add(-time.Hour), Version: version,
	}
}

func packCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick"},
		{Id: "PACK", MatchPrefix: "pack"},
		{Id: "BREAK_CASE", MatchPrefix: "break_case"},
	})
}

func (p *packCopies) enqueue(f fixture) *usecases.EnqueueWorkUnit {
	return usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock).WithPackLookup(p.store).WithCatalogue(packCatalogue())
}

func packEnqueueReq(path string, pick *workunit.PickPlan, brk *workunit.CaseBreak) usecases.EnqueueWorkUnitRequest {
	pathId, _ := shared.NewPathId(path)
	return usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-" + path, PathId: pathId, CPT: shared.NewCPT(packClock.Add(4 * time.Hour)), Reference: "order-1",
		SKU: "SKU-1", Pick: pick, CaseBreak: brk,
	}
}

func mustPick(t *testing.T, qty int, unit string) *workunit.PickPlan {
	t.Helper()
	plan, err := workunit.NewPickPlan("SKU-1", qty, unit, "FWD-01")
	if err != nil {
		t.Fatal(err)
	}
	return &plan
}

func mustBreak(t *testing.T) *workunit.CaseBreak {
	t.Helper()
	brk, err := workunit.NewCaseBreak("LPN-0001", "SKU-1", "FWD-01", "task-1")
	if err != nil {
		t.Fatal(err)
	}
	return &brk
}

func TestObservePackLevel_DeclareMeasureRetire(t *testing.T) {
	p := newPackCopies()
	packDeclaredFacts(t, p)
	packMeasuredWins(t, p)
	packRedeclareKeepsMeasurement(t, p)
	packRetiredLevel(t, p)
}

func packDeclaredFacts(t *testing.T, p *packCopies) {
	t.Helper()
	ctx := context.Background()
	if got := p.apply(t, usecases.PackLevelDeclared, caseRecord(3)); got != usecases.PackLevelApplied {
		t.Fatalf("declare = %v", got)
	}
	p.apply(t, usecases.PackLevelDeclared, eachRecord(2))
	view, _ := p.store.GetClassification(ctx, "SKU-1")
	if view.Known {
		t.Fatal("pack facts must never make a SKU classification-Known")
	}
	if view.Case == nil || view.Case.UnitsPerPack != 12 || view.Case.CubeMm3() != 24_000_000 || view.Case.WeightG() != 6500 || view.Case.Measured {
		t.Fatalf("case facts = %+v", view.Case)
	}
	if view.Each == nil || view.Each.WeightG() != 450 {
		t.Fatalf("each facts = %+v", view.Each)
	}

}

// A measurement wins over the declared size; a stale one changes nothing.
func packMeasuredWins(t *testing.T, p *packCopies) {
	t.Helper()
	ctx := context.Background()
	measured := productclassificationview.PackLevelRecord{GTIN: caseGTIN, Version: 5, Measured: productclassificationview.Dims{LengthMm: 410, WidthMm: 310, HeightMm: 210, WeightG: 6900}}
	if got := p.apply(t, usecases.PackLevelMeasured, measured); got != usecases.PackLevelApplied {
		t.Fatalf("measure = %v", got)
	}
	stale := measured
	stale.Version, stale.Measured.WeightG = 4, 1
	if got := p.apply(t, usecases.PackLevelMeasured, stale); got != usecases.PackLevelStale {
		t.Fatalf("stale measure = %v", got)
	}
	view, _ := p.store.GetClassification(ctx, "SKU-1")
	if !view.Case.Measured || view.Case.WeightG() != 6900 || view.Case.CubeMm3() != 410*310*210 {
		t.Fatalf("measured case facts = %+v", view.Case)
	}

}

// A re-declaration (is_orderable change) at a higher version keeps the measurement.
func packRedeclareKeepsMeasurement(t *testing.T, p *packCopies) {
	t.Helper()
	redeclared := caseRecord(6)
	redeclared.IsOrderable = false
	p.apply(t, usecases.PackLevelDeclared, redeclared)
	view, _ := p.store.GetClassification(context.Background(), "SKU-1")
	if !view.Case.Measured || view.Case.WeightG() != 6900 {
		t.Fatalf("a re-declaration lost the measurement: %+v", view.Case)
	}

}

// Retired: no active CASE level, the EACH level is untouched.
func packRetiredLevel(t *testing.T, p *packCopies) {
	t.Helper()
	retired := productclassificationview.PackLevelRecord{GTIN: caseGTIN, EffectiveTo: packClock, Version: 7}
	if got := p.apply(t, usecases.PackLevelRetired, retired); got != usecases.PackLevelApplied {
		t.Fatalf("retire = %v", got)
	}
	view, _ := p.store.GetClassification(context.Background(), "SKU-1")
	if view.Case != nil || view.Each == nil {
		t.Fatalf("after retire: case %+v each %+v", view.Case, view.Each)
	}
}

func TestObservePackLevel_StaleRedeliveredAndUnknownGTIN(t *testing.T) {
	p := newPackCopies()
	ctx := context.Background()
	req := usecases.ObservePackLevelRequest{EventId: "evt-1", Event: usecases.PackLevelDeclared, Record: caseRecord(3), ObservedAt: packClock}
	if got, err := p.observe.Execute(ctx, req); err != nil || got != usecases.PackLevelApplied {
		t.Fatalf("first = %v, %v", got, err)
	}
	if got, err := p.observe.Execute(ctx, req); err != nil || got != usecases.PackLevelRedelivered {
		t.Fatalf("same id = %v, %v", got, err)
	}
	older := req
	older.EventId, older.Record = "evt-2", caseRecord(2)
	older.Record.UnitsPerPack = 99
	if got, err := p.observe.Execute(ctx, older); err != nil || got != usecases.PackLevelStale {
		t.Fatalf("older version = %v, %v", got, err)
	}
	if view, _ := p.store.GetClassification(ctx, "SKU-1"); view.Case.UnitsPerPack != 12 {
		t.Fatalf("an older version overwrote the copy: %+v", view.Case)
	}
	// Retired / Measured for a GTIN the copy never saw: stale, not an error.
	for ev, rec := range map[usecases.PackLevelEvent]productclassificationview.PackLevelRecord{
		usecases.PackLevelRetired:  {GTIN: "00000000000000", EffectiveTo: packClock, Version: 9},
		usecases.PackLevelMeasured: {GTIN: "00000000000000", Measured: productclassificationview.Dims{LengthMm: 1, WidthMm: 1, HeightMm: 1, WeightG: 1}, Version: 9},
	} {
		if got := p.apply(t, ev, rec); got != usecases.PackLevelStale {
			t.Fatalf("event %d for an unknown gtin = %v, want stale", ev, got)
		}
	}
	if _, err := p.observe.Execute(ctx, usecases.ObservePackLevelRequest{EventId: "evt-x", Event: usecases.PackLevelEvent(99), ObservedAt: packClock}); !errors.Is(err, usecases.ErrUnknownPackLevelEvent) {
		t.Fatalf("unknown event kind: err = %v", err)
	}
}

// a whole number of cases becomes a CASE_PICK in eaches.
func TestEnqueueWorkUnit_WholeCasesBecomeCasePick(t *testing.T) {
	ctx := context.Background()
	p := newPackCopies()
	p.apply(t, usecases.PackLevelDeclared, caseRecord(3))
	f := newFixture()
	unit, err := p.enqueue(f).Execute(ctx, packEnqueueReq("pick-a", mustPick(t, 36, "CASE"), nil))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	plan, ok := unit.Pick()
	if !ok || plan.Quantity != 36 || plan.Unit != workunit.PickUnitCase || plan.FromBin != "FWD-01" || unit.WorkKind() != workunit.WorkKindCasePick || unit.Quantity() != 36 {
		t.Fatalf("unit = %+v kind %q qty %d", plan, unit.WorkKind(), unit.Quantity())
	}
	stored, err := f.workUnits.FindById(ctx, unit.Id())
	if err != nil || stored.WorkKind() != workunit.WorkKindCasePick {
		t.Fatalf("stored = %+v, %v", stored, err)
	}

}

// eaches that are not whole cases are refused.
func TestEnqueueWorkUnit_NotWholeCasesRefused(t *testing.T) {
	ctx := context.Background()
	p := newPackCopies()
	p.apply(t, usecases.PackLevelDeclared, caseRecord(3))
	if _, err := p.enqueue(newFixture()).Execute(ctx, packEnqueueReq("pick-a", mustPick(t, 13, "CASE"), nil)); !errors.Is(err, pickload.ErrNotCaseMultiple) {
		t.Fatalf("err = %v", err)
	}

}

// no CASE level, a retired CASE level, or no lookup at all is refused.
func TestEnqueueWorkUnit_NoCaseLevelRefused(t *testing.T) {
	ctx := context.Background()
	p := newPackCopies()
	if _, err := p.enqueue(newFixture()).Execute(ctx, packEnqueueReq("pick-a", mustPick(t, 12, "CASE"), nil)); !errors.Is(err, pickload.ErrNoActiveCaseLevel) {
		t.Fatalf("empty copy: err = %v", err)
	}
	p.apply(t, usecases.PackLevelDeclared, caseRecord(3))
	p.apply(t, usecases.PackLevelRetired, productclassificationview.PackLevelRecord{GTIN: caseGTIN, EffectiveTo: packClock, Version: 4})
	if _, err := p.enqueue(newFixture()).Execute(ctx, packEnqueueReq("pick-a", mustPick(t, 12, "CASE"), nil)); !errors.Is(err, pickload.ErrNoActiveCaseLevel) {
		t.Fatalf("retired level: err = %v", err)
	}
	f := newFixture()
	bare := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	if _, err := bare.Execute(ctx, packEnqueueReq("pick-a", mustPick(t, 12, "CASE"), nil)); !errors.Is(err, pickload.ErrNoActiveCaseLevel) {
		t.Fatalf("no lookup wired: err = %v", err)
	}

}

// an EACH pick needs no pack data and is an EACH_PICK.
func TestEnqueueWorkUnit_EachPickNeedsNoPackData(t *testing.T) {
	ctx := context.Background()
	unit, err := newPackCopies().enqueue(newFixture()).Execute(ctx, packEnqueueReq("pick-a", mustPick(t, 5, "EACH"), nil))
	if err != nil || unit.WorkKind() != workunit.WorkKindEachPick || unit.Quantity() != 5 {
		t.Fatalf("unit = %+v, %v", unit, err)
	}

}

func TestEnqueueWorkUnit_PathPairing(t *testing.T) {
	ctx := context.Background()
	p := newPackCopies()
	p.apply(t, usecases.PackLevelDeclared, caseRecord(3))
	cases := []struct {
		name    string
		req     usecases.EnqueueWorkUnitRequest
		wantErr error
	}{
		{"pick on a PICK-family path", packEnqueueReq("pick-zone-a", mustPick(t, 12, "CASE"), nil), nil},
		{"pick on another process path", packEnqueueReq("pack-a", mustPick(t, 12, "CASE"), nil), workunit.ErrInvalidPickPlan},
		{"pick on the BREAK_CASE path", packEnqueueReq("BREAK_CASE", mustPick(t, 12, "EACH"), nil), workunit.ErrInvalidPickPlan},
		{"case break on the BREAK_CASE path", packEnqueueReq("BREAK_CASE", nil, mustBreak(t)), nil},
		{"case break on a PICK path", packEnqueueReq("pick-a", nil, mustBreak(t)), workunit.ErrInvalidCaseBreak},
		{"BREAK_CASE work without its case break could never run", packEnqueueReq("BREAK_CASE", nil, nil), workunit.ErrInvalidCaseBreak},
		{"a pick and a case break together", packEnqueueReq("pick-a", mustPick(t, 12, "EACH"), mustBreak(t)), workunit.ErrInvalidPickPlan},
		{"a pick-aware request on an unknown path is the catalogue's error", packEnqueueReq("nowhere", mustPick(t, 12, "EACH"), nil), pathcatalog.ErrUnknownPath},
		{"a plain unit on a PACK path is untouched", packEnqueueReq("pack-a", nil, nil), nil},
		{"a plain unit on an unknown path keeps its pre-ADR-0045 behaviour", packEnqueueReq("nowhere", nil, nil), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			unit, err := p.enqueue(newFixture()).Execute(ctx, c.req)
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("err = %v, want %v", err, c.wantErr)
			}
			if c.wantErr == nil && c.req.Pick == nil && c.req.CaseBreak == nil && (unit.WorkKind() != "" || unit.Quantity() != 0) {
				t.Fatalf("a plain unit gained pack-aware state: kind %q qty %d", unit.WorkKind(), unit.Quantity())
			}
		})
	}
}

func TestEnqueueWorkUnit_CaseBreakIsStoredWithItsSku(t *testing.T) {
	f := newFixture()
	p := newPackCopies()
	unit, err := p.enqueue(f).Execute(context.Background(), packEnqueueReq("BREAK_CASE", nil, mustBreak(t)))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := f.workUnits.FindById(context.Background(), unit.Id())
	if err != nil {
		t.Fatal(err)
	}
	brk, ok := stored.CaseBreak()
	if !ok || brk.LPN != "LPN-0001" || brk.LooseBin != "FWD-01" || brk.PickTaskId != "task-1" || stored.SKU() != "SKU-1" || stored.WorkKind() != workunit.WorkKindBreakCase {
		t.Fatalf("stored = %+v kind %q sku %q", brk, stored.WorkKind(), stored.SKU())
	}
}

func TestReceiveChargeForecast_PackAwareEstimate(t *testing.T) {
	ctx := context.Background()
	p := newPackCopies()
	p.apply(t, usecases.PackLevelDeclared, caseRecord(3))
	p.apply(t, usecases.PackLevelDeclared, eachRecord(2))
	cpt := shared.NewCPT(packClock.Add(2 * time.Hour))
	pathId, _ := shared.NewPathId("pick-a")
	qty := func(n int) shared.Quantity { q, _ := shared.NewQuantity(n); return q }

	f := newFixture()
	uc := usecases.NewReceiveChargeForecast(f.charges, f.publisher, f.clock).WithPackLookup(p.store)
	forecast, err := uc.Execute(ctx, usecases.ReceiveChargeForecastRequest{PathId: pathId, Buckets: []usecases.CPTBucketInput{
		{CPT: cpt, Quantity: qty(120), SKU: "SKU-1", PickUnit: workunit.PickUnitCase},
		{CPT: shared.NewCPT(packClock.Add(3 * time.Hour)), Quantity: qty(7), SKU: "SKU-1"},
		{CPT: shared.NewCPT(packClock.Add(4 * time.Hour)), Quantity: qty(9)},
	}})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	b := forecast.Buckets()
	if b[0].Load == nil || b[0].Load.Handles != 10 || b[0].Load.CubeMm3 != 10*24_000_000 || b[0].Load.WeightG != 65_000 || forecast.TotalQuantity().Value() != 136 {
		t.Fatalf("case bucket = %+v (total %d: quantity stays eaches)", b[0].Load, forecast.TotalQuantity().Value())
	}
	if b[1].Load == nil || b[1].Load.Unit != workunit.PickUnitEach || b[1].Load.Handles != 7 || b[1].Load.WeightG != 7*450 {
		t.Fatalf("each bucket = %+v", b[1].Load)
	}
	if b[2].Load != nil || b[2].SKU != "" {
		t.Fatalf("a plain bucket gained an estimate: %+v", b[2])
	}

	assertForecastRefusals(t, uc, pathId, cpt, qty)
	// A refused forecast stores nothing new: the stored one is still the first.
	stored, err := f.charges.FindByPathId(ctx, pathId)
	if err != nil || stored.TotalQuantity().Value() != 136 {
		t.Fatalf("stored = %v, %v", stored, err)
	}
}

func assertForecastRefusals(t *testing.T, uc *usecases.ReceiveChargeForecast, pathId shared.PathId, cpt shared.CPT, qty func(int) shared.Quantity) {
	t.Helper()
	ctx := context.Background()
	for name, in := range map[string]usecases.CPTBucketInput{
		"case quantity that is not whole cases": {CPT: cpt, Quantity: qty(13), SKU: "SKU-1", PickUnit: workunit.PickUnitCase},
		"case for a SKU with no pack data":      {CPT: cpt, Quantity: qty(12), SKU: "SKU-NEW", PickUnit: workunit.PickUnitCase},
		"pickUnit without a sku":                {CPT: cpt, Quantity: qty(12), PickUnit: workunit.PickUnitCase},
	} {
		if _, err := uc.Execute(ctx, usecases.ReceiveChargeForecastRequest{PathId: pathId, Buckets: []usecases.CPTBucketInput{in}}); err == nil {
			t.Fatalf("%s: expected a refusal", name)
		}
	}
}

func TestCommitShiftPlan_PackAwareTravelEstimate(t *testing.T) {
	ctx := context.Background()
	p := newPackCopies()
	p.apply(t, usecases.PackLevelDeclared, caseRecord(3))
	pathId, _ := shared.NewPathId("pick-a")
	rate, _ := shared.NewRate(50)
	heads, _ := shared.NewStationCount(2)
	installed, _ := shared.NewStationCount(3)
	lookup := &fakeTravelDistanceLookup{view: traveldistanceview.TravelDistanceView{MetresM: 40, Known: true}}
	f := newFixture()
	uc := usecases.NewCommitShiftPlan(f.plans, f.publisher, f.clock).WithTravelDistanceLookup(lookup).WithPackLookup(p.store)
	req := usecases.CommitShiftPlanRequest{PathId: pathId, PlannedHeads: heads, InstalledStations: installed, Rate: rate, Hours: 8,
		FromLocationCode: "A", ToLocationCode: "B", SKU: "SKU-1", PickUnit: workunit.PickUnitCase}

	sp, err := uc.Execute(ctx, req)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	plan, _ := sp.PathPlan(pathId)
	load, ok := plan.HandleLoad()
	if !ok || load.Handles != 1 || load.Eaches != 12 || load.WeightG != 6500 {
		t.Fatalf("handle = %+v ok=%v", load, ok)
	}
	if kgM, ok := plan.CarryKgM(); !ok || math.Abs(kgM-6.5*40) > 1e-9 {
		t.Fatalf("carry = %v ok=%v, want %v (one CASE handle over the travel hint)", kgM, ok, 6.5*40)
	}

	// No travel distance known: the handle is still recorded, the carry is not invented.
	noDistance := usecases.NewCommitShiftPlan(f.plans, f.publisher, f.clock).WithPackLookup(p.store)
	sp, err = noDistance.Execute(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	plan, _ = sp.PathPlan(pathId)
	if _, ok := plan.HandleLoad(); !ok {
		t.Fatal("the handle must be recorded without a travel distance")
	}
	if _, ok := plan.CarryKgM(); ok {
		t.Fatal("a carry figure was invented without a travel distance")
	}

	bad := req
	bad.SKU = "SKU-NEW"
	if _, err := uc.Execute(ctx, bad); !errors.Is(err, pickload.ErrNoActiveCaseLevel) {
		t.Fatalf("CASE handle for a SKU without a CASE level: err = %v", err)
	}
	bad = req
	bad.SKU = ""
	if _, err := uc.Execute(ctx, bad); !errors.Is(err, workunit.ErrInvalidPickPlan) {
		t.Fatalf("pickUnit without sku: err = %v", err)
	}
	plain := req
	plain.SKU, plain.PickUnit = "", ""
	sp, err = uc.Execute(ctx, plain)
	if err != nil {
		t.Fatal(err)
	}
	plan, _ = sp.PathPlan(pathId)
	if _, ok := plan.HandleLoad(); ok {
		t.Fatal("a plain plan gained a handle")
	}
}
