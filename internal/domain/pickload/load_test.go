package pickload_test

import (
	"errors"
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
