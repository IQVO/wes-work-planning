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
