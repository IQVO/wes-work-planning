package workunit

import (
	"fmt"
	"strings"
)

// RelocationKind is the kind of one slot-plan move, spelled exactly like
// slotting-optimization's SlotPlanApproved `moves[].kind` (ADR-0040).
type RelocationKind string

const (
	// RelocationKindRelocate moves a SKU from one forward slot to another:
	// both slots are known.
	RelocationKindRelocate RelocationKind = "Relocate"
	// RelocationKindAssign brings a SKU into a forward slot: the source is
	// chosen and scanned by the operator at execution.
	RelocationKindAssign RelocationKind = "Assign"
	// RelocationKindVacate empties a forward slot: the destination (a
	// reserve bin) is chosen and scanned by the operator at execution.
	RelocationKindVacate RelocationKind = "Vacate"
)

// String returns the exact wire form.
func (k RelocationKind) String() string { return string(k) }

// ParseRelocationKind validates the wire form of a RelocationKind (exact,
// case-sensitive). Anything else is ErrUnknownRelocationKind.
func ParseRelocationKind(value string) (RelocationKind, error) {
	k := RelocationKind(value)
	if k == RelocationKindRelocate || k == RelocationKindAssign || k == RelocationKindVacate {
		return k, nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownRelocationKind, value)
}

// RelocationMove is the slot-plan move a RELOCATE work unit executes
// (ADR-0040): the plan it belongs to, its 0-based position in the plan's
// `moves`, its kind and its slots. FromSlot is empty for an Assign whose
// source is chosen at execution; ToSlot is empty for a Vacate. The SKU is
// the work unit's own SKU.
type RelocationMove struct {
	PlanId   string
	MoveSeq  int
	Kind     RelocationKind
	FromSlot string
	ToSlot   string
}

// NewRelocationMove validates and builds a move. Every string is trimmed.
// A Relocate needs both slots, an Assign needs to_slot and a Vacate needs
// from_slot; two known slots must differ.
func NewRelocationMove(planId string, moveSeq int, kind, fromSlot, toSlot string) (RelocationMove, error) {
	k, err := ParseRelocationKind(strings.TrimSpace(kind))
	if err != nil {
		return RelocationMove{}, err
	}
	m := RelocationMove{
		PlanId:   strings.TrimSpace(planId),
		MoveSeq:  moveSeq,
		Kind:     k,
		FromSlot: strings.TrimSpace(fromSlot),
		ToSlot:   strings.TrimSpace(toSlot),
	}
	if err := m.validate(); err != nil {
		return RelocationMove{}, err
	}
	return m, nil
}

func (m RelocationMove) validate() error {
	if m.PlanId == "" {
		return fmt.Errorf("%w: plan_id is required", ErrInvalidRelocationMove)
	}
	if m.MoveSeq < 0 {
		return fmt.Errorf("%w: move_seq must be >= 0, got %d", ErrInvalidRelocationMove, m.MoveSeq)
	}
	if m.Kind != RelocationKindAssign && m.FromSlot == "" {
		return fmt.Errorf("%w: a %s move needs from_slot", ErrInvalidRelocationMove, m.Kind)
	}
	if m.Kind != RelocationKindVacate && m.ToSlot == "" {
		return fmt.Errorf("%w: a %s move needs to_slot", ErrInvalidRelocationMove, m.Kind)
	}
	if m.FromSlot != "" && m.FromSlot == m.ToSlot {
		return fmt.Errorf("%w: from_slot and to_slot are both %q", ErrInvalidRelocationMove, m.FromSlot)
	}
	return nil
}
