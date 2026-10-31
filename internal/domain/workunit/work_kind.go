package workunit

// WorkKind is the enum of non-order work kinds this context plans: the three
// legs of a network-inventory-planning inter-site transfer
// (WorkDemandReleased, ADR-0033) and one slot-plan move (SlotPlanApproved,
// ADR-0040). Empty means "order work" — a plain order-driven work unit,
// exactly as it existed before either demand source.
type WorkKind string

const (
	WorkKindTransferPick     WorkKind = "TRANSFER_PICK"
	WorkKindTransferDispatch WorkKind = "TRANSFER_DISPATCH"
	WorkKindTransferArrival  WorkKind = "TRANSFER_ARRIVAL"
	// WorkKindRelocate is one move of an approved slot plan (ADR-0040),
	// released on the RELOCATE process path.
	WorkKindRelocate WorkKind = "RELOCATE"
	// WorkKindReplenish is one pick-face replenishment request of
	// pick-face-replenishment (ADR-0042), released on the REPLENISH process
	// path.
	WorkKindReplenish WorkKind = "REPLENISH"
	// WorkKindCasePick is a pick of whole sealed cases (ADR-0045): the
	// operator scans the CASE GTIN; the quantity is in eaches. Released on
	// the pick path with `pick.unit = CASE`.
	WorkKindCasePick WorkKind = "CASE_PICK"
	// WorkKindEachPick is a pick of single eaches (ADR-0045), released on
	// the pick path with `pick.unit = EACH`.
	WorkKindEachPick WorkKind = "EACH_PICK"
	// WorkKindBreakCase opens one sealed case into loose eaches (ADR-0045),
	// released on the BREAK_CASE path with the `case_break` object.
	WorkKindBreakCase WorkKind = "BREAK_CASE"
)

// String returns the exact wire form (the identity for a string enum, kept
// for symmetry with State.String).
func (k WorkKind) String() string { return string(k) }

// IsTransfer reports whether k is one of the three transfer legs. Only a
// transfer leg carries the ADR-0033 transfer fields on WorkReleased.
func (k WorkKind) IsTransfer() bool {
	return k == WorkKindTransferPick || k == WorkKindTransferDispatch || k == WorkKindTransferArrival
}

// IsPickKind reports whether k is the pack-aware pick kinds (CASE_PICK or
// EACH_PICK), the ones that carry a `pick` object on WorkReleased.
func (k WorkKind) IsPickKind() bool { return k == WorkKindCasePick || k == WorkKindEachPick }

// ParseWorkKind validates and converts the persisted form of a WorkKind.
// Anything that is not exactly one of the declared kinds — including "" —
// is rejected with ErrUnknownWorkKind, so a corrupt value can never become
// an unvalidated work_kind inside an aggregate (the inverse of String,
// mirroring ParseState's contract). Inbound transfer demands additionally
// require IsTransfer (a WorkDemandReleased can never carry RELOCATE).
func ParseWorkKind(value string) (WorkKind, error) {
	k := WorkKind(value)
	if k.IsTransfer() || k.IsPickKind() || k == WorkKindRelocate || k == WorkKindReplenish || k == WorkKindBreakCase {
		return k, nil
	}
	return "", ErrUnknownWorkKind
}
