package pickload_test

import (
	"errors"
	"math"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/domain/pickload"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

func view(each, kase *productclassificationview.PackFacts) productclassificationview.ProductClassificationView {
	return productclassificationview.ProductClassificationView{SKU: "SKU-1", Each: each, Case: kase}
}

var (
	caseOf12 = &productclassificationview.PackFacts{
		Level: productclassificationview.PackLevelCase, UnitsPerPack: 12,
		Dims: productclassificationview.Dims{LengthMm: 400, WidthMm: 300, HeightMm: 200, WeightG: 6500},
	}
	eachSized = &productclassificationview.PackFacts{
		Level: productclassificationview.PackLevelEach, UnitsPerPack: 1,
		Dims: productclassificationview.Dims{LengthMm: 100, WidthMm: 80, HeightMm: 60, WeightG: 450},
	}
)

func TestEstimate(t *testing.T) {
	cases := []struct {
		name    string
		unit    workunit.PickUnit
		eaches  int
		view    productclassificationview.ProductClassificationView
		want    pickload.Load
		wantErr error
	}{
		{"3 cases of 12 use the CASE cube and weight, not the each size", workunit.PickUnitCase, 36, view(eachSized, caseOf12),
			pickload.Load{Unit: workunit.PickUnitCase, Eaches: 36, Handles: 3, CubeMm3: 3 * 400 * 300 * 200, WeightG: 3 * 6500, CubeKnown: true, WeightKnown: true}, nil},
		{"each pick counts eaches with the each size", workunit.PickUnitEach, 5, view(eachSized, caseOf12),
			pickload.Load{Unit: workunit.PickUnitEach, Eaches: 5, Handles: 5, CubeMm3: 5 * 100 * 80 * 60, WeightG: 5 * 450, CubeKnown: true, WeightKnown: true}, nil},
		{"each pick without an each size: handles only, size unknown (not zero)", workunit.PickUnitEach, 5, view(nil, caseOf12),
			pickload.Load{Unit: workunit.PickUnitEach, Eaches: 5, Handles: 5}, nil},
		{"case without a declared size: handles only", workunit.PickUnitCase, 24,
			view(nil, &productclassificationview.PackFacts{Level: productclassificationview.PackLevelCase, UnitsPerPack: 12}),
			pickload.Load{Unit: workunit.PickUnitCase, Eaches: 24, Handles: 2}, nil},
		{"case weight known but no cube", workunit.PickUnitCase, 12,
			view(nil, &productclassificationview.PackFacts{Level: productclassificationview.PackLevelCase, UnitsPerPack: 12, Dims: productclassificationview.Dims{WeightG: 900}}),
			pickload.Load{Unit: workunit.PickUnitCase, Eaches: 12, Handles: 1, WeightG: 900, WeightKnown: true}, nil},
		{"case pick with no active CASE level", workunit.PickUnitCase, 12, view(eachSized, nil), pickload.Load{}, pickload.ErrNoActiveCaseLevel},
		{"case pick that is not a whole number of cases", workunit.PickUnitCase, 13, view(nil, caseOf12), pickload.Load{}, pickload.ErrNotCaseMultiple},
		{"zero eaches", workunit.PickUnitEach, 0, view(nil, nil), pickload.Load{}, pickload.ErrInvalidQuantity},
		{"unknown unit", workunit.PickUnit("PALLET"), 1, view(nil, nil), pickload.Load{}, workunit.ErrUnknownPickUnit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := pickload.Estimate(c.unit, c.eaches, c.view)
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("err = %v, want %v", err, c.wantErr)
			}
			if got != c.want {
				t.Fatalf("load = %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestPerHandle(t *testing.T) {
	got, err := pickload.PerHandle(workunit.PickUnitCase, view(eachSized, caseOf12))
	if err != nil || got.Handles != 1 || got.Eaches != 12 || got.WeightG != 6500 || got.CubeMm3 != 400*300*200 {
		t.Fatalf("case handle = %+v, %v", got, err)
	}
	got, err = pickload.PerHandle(workunit.PickUnitEach, view(eachSized, caseOf12))
	if err != nil || got.Handles != 1 || got.Eaches != 1 || got.WeightG != 450 {
		t.Fatalf("each handle = %+v, %v", got, err)
	}
	if _, err := pickload.PerHandle(workunit.PickUnitCase, view(eachSized, nil)); !errors.Is(err, pickload.ErrNoActiveCaseLevel) {
		t.Fatalf("no case level: err = %v", err)
	}
}

func TestCarryKgM(t *testing.T) {
	load, err := pickload.Estimate(workunit.PickUnitCase, 24, view(nil, caseOf12))
	if err != nil {
		t.Fatal(err)
	}
	// Two cases of 6.5 kg: the carry is per HANDLE (6.5 kg), over 40 m.
	got, ok := load.CarryKgM(40)
	if !ok || math.Abs(got-6.5*40) > 1e-9 {
		t.Fatalf("carry = %v ok=%v, want %v", got, ok, 6.5*40)
	}
	if _, ok := (pickload.Load{Handles: 2}).CarryKgM(40); ok {
		t.Fatal("an unknown weight must not produce a carry figure")
	}
	if _, ok := (pickload.Load{WeightKnown: true, WeightG: 10}).CarryKgM(40); ok {
		t.Fatal("no handles must not produce a carry figure")
	}
}
