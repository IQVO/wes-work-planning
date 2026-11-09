package productclassificationview_test

import (
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

func TestDims_CubeNeedsAllThreeSides(t *testing.T) {
	if got := (productclassificationview.Dims{LengthMm: 400, WidthMm: 300, HeightMm: 200}).CubeMm3(); got != 24_000_000 {
		t.Fatalf("cube = %d", got)
	}
	for _, d := range []productclassificationview.Dims{{}, {LengthMm: 400, WidthMm: 300}, {WeightG: 10}} {
		if d.Known() || d.CubeMm3() != 0 {
			t.Fatalf("%+v: an incomplete size must not produce a cube", d)
		}
	}
}

func TestPackLevelRecord_FactsPreferTheMeasurement(t *testing.T) {
	declared := productclassificationview.Dims{LengthMm: 400, WidthMm: 300, HeightMm: 200, WeightG: 6500}
	measured := productclassificationview.Dims{LengthMm: 410, WidthMm: 310, HeightMm: 210, WeightG: 6900}
	rec := productclassificationview.PackLevelRecord{
		SKU: "SKU-1", Level: productclassificationview.PackLevelCase, GTIN: "24006381333935", UnitsPerPack: 12,
		Declared: declared, EffectiveFrom: time.Date(2026, 10, 6, 20, 0, 0, 0, time.UTC), Version: 3,
	}
	if f := rec.Facts(); f.Dims != declared || f.Measured || f.UnitsPerPack != 12 || f.GTIN != rec.GTIN || f.WeightG() != 6500 {
		t.Fatalf("declared facts = %+v", f)
	}
	rec.Measured = measured
	if f := rec.Facts(); f.Dims != measured || !f.Measured || f.CubeMm3() != 410*310*210 {
		t.Fatalf("measured facts = %+v", f)
	}
}

func TestPackLevelRecord_ActiveUntilEndDated(t *testing.T) {
	rec := productclassificationview.PackLevelRecord{}
	if !rec.Active() {
		t.Fatal("a level with no effective_to is active")
	}
	rec.EffectiveTo = time.Date(2026, 10, 6, 21, 1, 0, 0, time.UTC)
	if rec.Active() {
		t.Fatal("an end-dated level is not active")
	}
}

func TestPackLevel_Valid(t *testing.T) {
	for _, l := range []productclassificationview.PackLevel{"EACH", "INNER", "CASE", "PALLET"} {
		if !l.Valid() {
			t.Fatalf("%s should be valid", l)
		}
	}
	for _, l := range []productclassificationview.PackLevel{"", "case", "BOX"} {
		if l.Valid() {
			t.Fatalf("%q should be invalid", l)
		}
	}
}
