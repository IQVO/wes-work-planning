package workunit

// WorkKind is the enum of transfer work kinds this context can be asked to
// plan by network-inventory-planning's WorkDemandReleased: one leg of an
// inter-site transfer (pick at the origin, dispatch/loading, arrival at the
// destination). Empty means "not transfer work" — a plain order-driven
// work unit, exactly as it existed before transfer support.
type WorkKind string

const (
	WorkKindTransferPick     WorkKind = "TRANSFER_PICK"
	WorkKindTransferDispatch WorkKind = "TRANSFER_DISPATCH"
	WorkKindTransferArrival  WorkKind = "TRANSFER_ARRIVAL"
)

// String returns the exact wire form (the identity for a string enum, kept
// for symmetry with State.String).
func (k WorkKind) String() string { return string(k) }

// ParseWorkKind validates and converts the wire form of a WorkKind. Anything
// that is not exactly one of the three declared transfer legs — including ""
// — is rejected with ErrUnknownWorkKind, so a corrupt payload can never
// become an unvalidated work_kind inside an aggregate (the inverse of
// String, mirroring ParseState's contract).
func ParseWorkKind(value string) (WorkKind, error) {
	switch value {
	case WorkKindTransferPick.String():
		return WorkKindTransferPick, nil
	case WorkKindTransferDispatch.String():
		return WorkKindTransferDispatch, nil
	case WorkKindTransferArrival.String():
		return WorkKindTransferArrival, nil
	default:
		return "", ErrUnknownWorkKind
	}
}
