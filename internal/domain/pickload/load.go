// Package pickload estimates the handling load of a pick in the units the
// operator actually handles (ADR-0045). A CASE_PICK moves whole sealed cases,
// so its charge is counted in cases and its cube and weight are the CASE
// level's; an EACH pick counts eaches with the EACH level's size when known.
// It is a pure domain service over the pack facts of
// productclassificationview: no adapter, no clock, no I/O.
package pickload

import (
	"errors"
	"fmt"

	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

var (
	// ErrInvalidQuantity: the eaches to estimate are below 1.
	ErrInvalidQuantity = errors.New("pick load: quantity must be at least 1 (eaches)")
	// ErrNoActiveCaseLevel: a CASE estimate was asked for a SKU whose local
	// pack copy holds no active CASE level (not declared yet, retired, or
	// the copy has not caught up).
	ErrNoActiveCaseLevel = errors.New("pick load: the SKU has no active CASE pack level in the local copy")
	// ErrNotCaseMultiple: the eaches are not a whole number of cases.
	ErrNotCaseMultiple = errors.New("pick load: quantity is not a whole number of cases")
)

// Load is the handling load of one pick or charge bucket.
type Load struct {
	Unit workunit.PickUnit
	// Eaches is the quantity in eaches, the ledger unit (contracts G1).
	Eaches int
	// Handles is what an operator handles: cases for a CASE load, eaches
	// for an EACH load.
	Handles int
	// CubeMm3 / WeightG are the totals over all Handles. They are 0 with the
	// matching Known flag false when the pack level has no size on record:
	// "unknown" is never presented as zero.
	CubeMm3     int64
	WeightG     int64
	CubeKnown   bool
	WeightKnown bool
}

// Estimate computes the load of eaches of the SKU described by view, picked
// as unit.
//
//   - CASE needs an active CASE level and a whole number of cases; handles =
//     eaches / unitsPerPack and cube / weight are the CASE level's per case.
//   - EACH handles = eaches; cube / weight come from the EACH level when its
//     size is on record, else they are unknown.
func Estimate(unit workunit.PickUnit, eaches int, view productclassificationview.ProductClassificationView) (Load, error) {
	if eaches < 1 {
		return Load{}, fmt.Errorf("%w, got %d", ErrInvalidQuantity, eaches)
	}
	switch unit {
	case workunit.PickUnitCase:
		c := view.Case
		if c == nil || c.UnitsPerPack < 1 {
			return Load{}, fmt.Errorf("%w: %s", ErrNoActiveCaseLevel, view.SKU)
		}
		if eaches%c.UnitsPerPack != 0 {
			return Load{}, fmt.Errorf("%w: %d eaches with %d per case", ErrNotCaseMultiple, eaches, c.UnitsPerPack)
		}
		return totals(unit, eaches, eaches/c.UnitsPerPack, *c), nil
	case workunit.PickUnitEach:
		var each productclassificationview.PackFacts
		if view.Each != nil {
			each = *view.Each
		}
		return totals(unit, eaches, eaches, each), nil
	default:
		return Load{}, fmt.Errorf("%w: %q", workunit.ErrUnknownPickUnit, string(unit))
	}
}

// PerHandle is the load of ONE handle of unit: one case (units-per-pack
// eaches) for CASE, one each for EACH. It is the per-trip figure the travel
// estimate uses.
func PerHandle(unit workunit.PickUnit, view productclassificationview.ProductClassificationView) (Load, error) {
	if unit == workunit.PickUnitCase {
		if view.Case == nil || view.Case.UnitsPerPack < 1 {
			return Load{}, fmt.Errorf("%w: %s", ErrNoActiveCaseLevel, view.SKU)
		}
		return Estimate(unit, view.Case.UnitsPerPack, view)
	}
	return Estimate(unit, 1, view)
}

func totals(unit workunit.PickUnit, eaches, handles int, per productclassificationview.PackFacts) Load {
	l := Load{Unit: unit, Eaches: eaches, Handles: handles}
	if cube := per.CubeMm3(); cube > 0 {
		l.CubeMm3, l.CubeKnown = cube*int64(handles), true
	}
	if weight := per.WeightG(); weight > 0 {
		l.WeightG, l.WeightKnown = int64(weight)*int64(handles), true
	}
	return l
}

// CarryKgM is the carry load of moving one handle over metresM: the handle's
// weight in kilograms times the distance. ok is false when the handle weight
// is unknown. It is a planning figure for comparing a case path to an each
// path over the same travel hint, not a labour standard.
func (l Load) CarryKgM(metresM float64) (kgM float64, ok bool) {
	if !l.WeightKnown || l.Handles < 1 {
		return 0, false
	}
	perHandleKg := float64(l.WeightG) / float64(l.Handles) / 1000
	return perHandleKg * metresM, true
}
