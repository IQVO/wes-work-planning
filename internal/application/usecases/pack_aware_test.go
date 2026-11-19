package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/pickload"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
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
