package workunit

import (
	"fmt"
	"regexp"
	"strings"
)

// PickUnit is the unit an operator scans for a pick: one EACH or one sealed
// CASE. The wire form is exactly the `unit` of fulfillment-execution's
// WorkReleased `pick` object (its ADR 0054).
type PickUnit string

const (
	PickUnitEach PickUnit = "EACH"
	PickUnitCase PickUnit = "CASE"
)

// String returns the exact wire form.
func (u PickUnit) String() string { return string(u) }

// ParsePickUnit validates the wire form (exact, case-sensitive); an empty
// value is NOT defaulted here (callers decide).
func ParsePickUnit(value string) (PickUnit, error) {
	u := PickUnit(value)
	if u == PickUnitEach || u == PickUnitCase {
		return u, nil
	}
	return "", fmt.Errorf("%w: %q (want EACH or CASE)", ErrUnknownPickUnit, value)
}

// WorkKind returns the pack-aware kind a pick of unit u is: CASE_PICK or
// EACH_PICK (ADR-0045).
func (u PickUnit) WorkKind() WorkKind {
	if u == PickUnitCase {
		return WorkKindCasePick
	}
	return WorkKindEachPick
}

// PickPlan is the pick a CASE_PICK or EACH_PICK unit executes (ADR-0045):
// Quantity EACHES of SKU, scanned as Unit. Quantity is ALWAYS in eaches (the
// ledger unit, contracts G1): for a CASE pick it is cases x units-per-pack,
// and the case count is derived from the pack view where needed, never
// stored. FromBin is the optional source pick face.
type PickPlan struct {
	SKU      string
	Quantity int
	Unit     PickUnit
	FromBin  string
}

// NewPickPlan validates and builds a plan. Every string is trimmed. The SKU
// is required, the quantity is at least 1 and the unit is EACH or CASE.
func NewPickPlan(sku string, quantity int, unit string, fromBin string) (PickPlan, error) {
	u, err := ParsePickUnit(strings.TrimSpace(unit))
	if err != nil {
		return PickPlan{}, err
	}
	p := PickPlan{SKU: strings.TrimSpace(sku), Quantity: quantity, Unit: u, FromBin: strings.TrimSpace(fromBin)}
	switch {
	case p.SKU == "":
		return PickPlan{}, fmt.Errorf("%w: sku is required", ErrInvalidPickPlan)
	case p.Quantity < 1:
		return PickPlan{}, fmt.Errorf("%w: quantity must be at least 1 (eaches), got %d", ErrInvalidPickPlan, quantity)
	}
	return p, nil
}

// BreakCasePathId is the process path of BREAK_CASE work. fulfillment-execution
// derives a task's type from the released path_id (its catalogue id), so a
// case break MUST be released on this path and a pick MUST NOT be.
const BreakCasePathId = "BREAK_CASE"

// maxLPNLen / lpnPattern mirror fulfillment-execution's task.NewCaseBreak
// (and inventory-storage's container id): 1-64 characters of [A-Za-z0-9._-].
const maxLPNLen = 64

var lpnPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// CaseBreak is what a BREAK_CASE unit executes (ADR-0045): break the sealed
// case LPN of SKU into LooseBin, the pick face of the EACH pick PickTaskId
// that found only the sealed case. The wire form is fulfillment-execution's
// WorkReleased `case_break` object (its ADR 0054): lpn, sku, loose_bin,
// pick_task_id.
type CaseBreak struct {
	LPN        string
	SKU        string
	LooseBin   string
	PickTaskId string
}

// NewCaseBreak validates and builds a break; every field is required and
// every string is trimmed.
func NewCaseBreak(lpn, sku, looseBin, pickTaskId string) (CaseBreak, error) {
	b := CaseBreak{
		LPN: strings.TrimSpace(lpn), SKU: strings.TrimSpace(sku),
		LooseBin: strings.TrimSpace(looseBin), PickTaskId: strings.TrimSpace(pickTaskId),
	}
	switch {
	case b.LPN == "" || len(b.LPN) > maxLPNLen || !lpnPattern.MatchString(b.LPN):
		return CaseBreak{}, fmt.Errorf("%w: lpn %q is not 1-%d characters of [A-Za-z0-9._-]", ErrInvalidCaseBreak, lpn, maxLPNLen)
	case b.SKU == "":
		return CaseBreak{}, fmt.Errorf("%w: sku is required", ErrInvalidCaseBreak)
	case b.LooseBin == "":
		return CaseBreak{}, fmt.Errorf("%w: loose_bin is required", ErrInvalidCaseBreak)
	case b.PickTaskId == "":
		return CaseBreak{}, fmt.Errorf("%w: pick_task_id is required", ErrInvalidCaseBreak)
	}
	return b, nil
}
