package workunit

import (
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// State is the lifecycle stage of a WorkUnit.
type State int

const (
	Pending State = iota
	Released
	Completed
)

func (s State) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Released:
		return "Released"
	case Completed:
		return "Completed"
	default:
		return "Unknown"
	}
}

// ParseState validates and converts the persisted string form of a State
// (the inverse of State.String). Anything that is not exactly Pending,
// Released or Completed — including "" and "Unknown" — is rejected with
// ErrUnknownState, so a corrupt stored value can never become an invalid
// state inside an aggregate.
func ParseState(value string) (State, error) {
	switch value {
	case Pending.String():
		return Pending, nil
	case Released.String():
		return Released, nil
	case Completed.String():
		return Completed, nil
	default:
		return Pending, ErrUnknownState
	}
}

// WorkUnit is a releasable unit of work (e.g. a pick task). It is assigned
// at most once at a time and cannot complete twice.
type WorkUnit struct {
	id          string
	pathId      shared.PathId
	cpt         shared.CPT
	reference   string
	sku         string
	giftWrap    bool
	transferRef string
	workKind    WorkKind
	siteId      string
	quantity    int
	lineNo      int
	state       State
	releasedAt  *time.Time
	completedAt *time.Time
}

// NewWorkUnit constructs a Pending WorkUnit for a path, carrying its CPT and
// an external reference (e.g. the pick task's source order line).
func NewWorkUnit(id string, pathId shared.PathId, cpt shared.CPT, reference string) (*WorkUnit, error) {
	if id == "" {
		return nil, ErrEmptyId
	}
	if reference == "" {
		return nil, ErrEmptyReference
	}
	return &WorkUnit{id: id, pathId: pathId, cpt: cpt, reference: reference, state: Pending}, nil
}

func (w *WorkUnit) Id() string            { return w.id }
func (w *WorkUnit) PathId() shared.PathId { return w.pathId }
func (w *WorkUnit) CPT() shared.CPT       { return w.cpt }
func (w *WorkUnit) Reference() string     { return w.reference }
func (w *WorkUnit) State() State          { return w.state }

// SKU is the optional inventory SKU this work unit's order line
// corresponds to. Empty when the caller did not supply one (e.g. a
// non-pick process path, or an order line that predates this field) —
// callers must treat "" as "no SKU known", not as an error.
//
// It exists solely so the outbound WorkReleased publisher can look up the
// SKU's ProductClassification once, at release time, and stamp derived
// hazmat/fragile hints onto the published event (see ADR-0009). Nothing in
// this aggregate's own invariants depends on it.
func (w *WorkUnit) SKU() string { return w.sku }

// SetSKU records the optional SKU after construction. A separate setter,
// rather than a NewWorkUnit parameter, keeps every existing caller and test
// fixture compiling unchanged — SKU is additive, not a new invariant.
func (w *WorkUnit) SetSKU(sku string) { w.sku = sku }

// GiftWrap is an optional, caller-stated characteristic of this work unit:
// the requester asked the warehouse to produce a gift package for it,
// declared at enqueue time (see ADR-0010). Unlike SKU/ProductClassification
// (ADR-0009), this is never looked up from another service — it is exactly
// what the caller supplied on EnqueueWorkUnitRequest, read once here so the
// outbound WorkReleased publisher can stamp it onto the published event at
// release time. False by default when the caller did not request it.
func (w *WorkUnit) GiftWrap() bool { return w.giftWrap }

// SetGiftWrap records the optional gift-wrap request after construction. A
// separate setter, rather than a NewWorkUnit parameter, keeps every
// existing caller and test fixture compiling unchanged — GiftWrap is
// additive, not a new invariant.
func (w *WorkUnit) SetGiftWrap(giftWrap bool) { w.giftWrap = giftWrap }

// TransferRef is the optional network transfer reference (NIP's
// transfer_ref) this work unit executes a leg of. Empty on every
// order-driven unit — transfer metadata is additive, never an invariant of
// the aggregate itself (see ADR-0033). It exists so the outbound
// WorkReleased publisher can stamp the transfer context onto the published
// event's data payload at release time; nothing in this aggregate's own
// lifecycle depends on it.
func (w *WorkUnit) TransferRef() string { return w.transferRef }

// SetTransferRef records the optional transfer reference after
// construction, mirroring SetSKU's additive-setter discipline.
func (w *WorkUnit) SetTransferRef(ref string) { w.transferRef = ref }

// WorkKind returns the transfer leg this unit executes (TRANSFER_PICK,
// TRANSFER_DISPATCH or TRANSFER_ARRIVAL), or "" for a plain order-driven
// unit — "" must be read as "not transfer work", not an error.
func (w *WorkUnit) WorkKind() WorkKind { return w.workKind }

// SetWorkKind records the validated transfer leg after construction.
// The wire form must already have gone through ParseWorkKind; the setter
// itself does not re-validate, mirroring SetSKU/SetGiftWrap.
func (w *WorkUnit) SetWorkKind(kind WorkKind) { w.workKind = kind }

// SiteId is the optional network site (NIP's site_id — the transfer's
// origin for a pick/dispatch leg, destination for an arrival leg) this
// unit is anchored to. Empty when not transfer work.
func (w *WorkUnit) SiteId() string { return w.siteId }

// SetSiteId records the optional site id after construction.
func (w *WorkUnit) SetSiteId(siteId string) { w.siteId = siteId }

// Quantity is the optional number of units (per SKU) this transfer leg
// moves. Zero when the caller did not supply one — read as "no quantity
// known", not an error.
func (w *WorkUnit) Quantity() int { return w.quantity }

// SetQuantity records the optional transfer quantity after construction.
func (w *WorkUnit) SetQuantity(quantity int) { w.quantity = quantity }

// LineNo is the optional order line number (1-based) this work unit was
// created for — the value OrderAllocated carries per line and that is also
// embedded in the deterministic id "<order>-line-<n>". 0 means "unknown"
// (a transfer unit, a REST-enqueued unit that gave none, or a row that
// predates ADR-0036) and must be read as "no line", not as an error. It
// exists so the outbound WorkReleased publisher can state the line
// explicitly instead of making consumers parse it out of the id; nothing in
// this aggregate's own invariants depends on it.
func (w *WorkUnit) LineNo() int { return w.lineNo }

// SetLineNo records the optional order line number after construction,
// mirroring SetSKU's additive-setter discipline. Callers validate the
// value with ValidateLineNo; the setter itself does not re-validate.
func (w *WorkUnit) SetLineNo(lineNo int) { w.lineNo = lineNo }

// MaxLineNo is the largest valid line number: every line_no column in the
// fleet is a 32-bit INTEGER, so it is math.MaxInt32.
const MaxLineNo = 1<<31 - 1

// ValidateLineNo reports whether lineNo may be stored on a WorkUnit: 0
// (unknown) or 1..MaxLineNo. Anything else is ErrInvalidLineNo.
func ValidateLineNo(lineNo int) error {
	if lineNo < 0 || lineNo > MaxLineNo {
		return ErrInvalidLineNo
	}
	return nil
}

// Release admits the unit into active work. A unit may be assigned/released
// at most once — releasing an already-released or completed unit fails.
func (w *WorkUnit) Release(at time.Time) error {
	if w.state != Pending {
		return ErrAlreadyReleased
	}
	w.state = Released
	w.releasedAt = &at
	return nil
}

// Complete finishes a released unit. A unit cannot complete twice, and must
// be released before it can complete.
func (w *WorkUnit) Complete(at time.Time) error {
	if w.state == Completed {
		return ErrAlreadyCompleted
	}
	if w.state != Released {
		return ErrNotReleased
	}
	w.state = Completed
	w.completedAt = &at
	return nil
}

func (w *WorkUnit) ReleasedAt() *time.Time  { return w.releasedAt }
func (w *WorkUnit) CompletedAt() *time.Time { return w.completedAt }
