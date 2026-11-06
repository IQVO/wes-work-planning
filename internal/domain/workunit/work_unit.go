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
	// Cancelled is terminal: a Pending unit withdrawn before release (a
	// superseded slot plan's move, ADR-0040). It is never released.
	Cancelled
)

func (s State) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Released:
		return "Released"
	case Completed:
		return "Completed"
	case Cancelled:
		return "Cancelled"
	default:
		return "Unknown"
	}
}

// ParseState validates and converts the persisted string form of a State
// (the inverse of State.String). Anything that is not exactly Pending,
// Released, Completed or Cancelled — including "" and "Unknown" — is rejected with
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
	case Cancelled.String():
		return Cancelled, nil
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
	relocation  *RelocationMove
	replenish   *ReplenishmentMove
	pick        *PickPlan
	caseBreak   *CaseBreak
	blockedBy   string
	state       State
	releasedAt  *time.Time
	completedAt *time.Time
	cancelledAt *time.Time
}

// Option customises a WorkUnit at construction (the functional-options
// pattern). Options are applied in order, after the base invariants
// (non-empty id and reference) hold, and the first failure aborts
// construction — an invalid WorkUnit cannot exist, and no exported setter
// may mutate one afterwards. When two options touch the same field the
// later one wins; the pack-aware options (WithPick, WithCaseBreak) and
// WithReplenishment deliberately overwrite sku/quantity/kind because the
// move/plan is their single source.
type Option func(*WorkUnit) error

// NewWorkUnit constructs a Pending WorkUnit for a path, carrying its CPT and
// an external reference (e.g. the pick task's source order line). Every
// other attribute arrives as an Option, so the aggregate is complete and
// valid at construction.
func NewWorkUnit(id string, pathId shared.PathId, cpt shared.CPT, reference string, opts ...Option) (*WorkUnit, error) {
	if id == "" {
		return nil, ErrEmptyId
	}
	if reference == "" {
		return nil, ErrEmptyReference
	}
	w := &WorkUnit{id: id, pathId: pathId, cpt: cpt, reference: reference, state: Pending}
	for _, opt := range opts {
		if err := opt(w); err != nil {
			return nil, err
		}
	}
	return w, nil
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

// WithSKU records the optional SKU at construction (ADR-0009).
func WithSKU(sku string) Option {
	return func(w *WorkUnit) error {
		w.sku = sku
		return nil
	}
}

// GiftWrap is an optional, caller-stated characteristic of this work unit:
// the requester asked the warehouse to produce a gift package for it,
// declared at enqueue time (see ADR-0010). Unlike SKU/ProductClassification
// (ADR-0009), this is never looked up from another service — it is exactly
// what the caller supplied on EnqueueWorkUnitRequest, read once here so the
// outbound WorkReleased publisher can stamp it onto the published event at
// release time. False by default when the caller did not request it.
func (w *WorkUnit) GiftWrap() bool { return w.giftWrap }

// WithGiftWrap records the optional gift-wrap request at construction
// (ADR-0010).
func WithGiftWrap(giftWrap bool) Option {
	return func(w *WorkUnit) error {
		w.giftWrap = giftWrap
		return nil
	}
}

// TransferRef is the optional network transfer reference (NIP's
// transfer_ref) this work unit executes a leg of. Empty on every
// order-driven unit — transfer metadata is additive, never an invariant of
// the aggregate itself (see ADR-0033). It exists so the outbound
// WorkReleased publisher can stamp the transfer context onto the published
// event's data payload at release time; nothing in this aggregate's own
// lifecycle depends on it.
func (w *WorkUnit) TransferRef() string { return w.transferRef }

// WorkKind returns the transfer leg this unit executes (TRANSFER_PICK,
// TRANSFER_DISPATCH or TRANSFER_ARRIVAL), or "" for a plain order-driven
// unit — "" must be read as "not transfer work", not an error.
func (w *WorkUnit) WorkKind() WorkKind { return w.workKind }

// WithTransferLeg records the transfer context of a
// network-inventory-planning work demand (ADR-0033) at construction: the
// transfer reference, the validated leg (ParseWorkKind must already have
// accepted it), the anchoring site and the quantity moved. kind must be a
// transfer leg — anything else is ErrUnknownWorkKind, so a transfer context
// can never be stamped on a non-transfer kind.
func WithTransferLeg(transferRef string, kind WorkKind, siteId string, quantity int) Option {
	return func(w *WorkUnit) error {
		if !kind.IsTransfer() {
			return ErrUnknownWorkKind
		}
		w.transferRef = transferRef
		w.workKind = kind
		w.siteId = siteId
		w.quantity = quantity
		return nil
	}
}

// WithWorkKind records a validated non-transfer work kind (RELOCATE,
// REPLENISH) at construction. The pack-aware kinds arrive through
// WithPick/WithCaseBreak instead — the plan/break is their single source.
// A kind ParseWorkKind would reject is ErrUnknownWorkKind.
func WithWorkKind(kind WorkKind) Option {
	return func(w *WorkUnit) error {
		if kind == "" {
			return nil
		}
		if _, err := ParseWorkKind(kind.String()); err != nil {
			return err
		}
		w.workKind = kind
		return nil
	}
}

// SiteId is the optional network site (NIP's site_id — the transfer's
// origin for a pick/dispatch leg, destination for an arrival leg) this
// unit is anchored to. Empty when not transfer work.
func (w *WorkUnit) SiteId() string { return w.siteId }

// WithSiteId records the optional site id at construction.
func WithSiteId(siteId string) Option {
	return func(w *WorkUnit) error {
		w.siteId = siteId
		return nil
	}
}

// Quantity is the optional number of units (per SKU) this transfer leg
// moves. Zero when the caller did not supply one — read as "no quantity
// known", not an error.
func (w *WorkUnit) Quantity() int { return w.quantity }

// LineNo is the optional order line number (1-based) this work unit was
// created for — the value OrderAllocated carries per line and that is also
// embedded in the deterministic id "<order>-line-<n>". 0 means "unknown"
// (a transfer unit, a REST-enqueued unit that gave none, or a row that
// predates ADR-0036) and must be read as "no line", not as an error. It
// exists so the outbound WorkReleased publisher can state the line
// explicitly instead of making consumers parse it out of the id; nothing in
// this aggregate's own invariants depends on it.
func (w *WorkUnit) LineNo() int { return w.lineNo }

// WithLineNo records the optional order line number at construction
// (ADR-0036). The value is validated by ValidateLineNo: an out-of-range
// line fails construction with ErrInvalidLineNo instead of existing on an
// invalid aggregate.
func WithLineNo(lineNo int) Option {
	return func(w *WorkUnit) error {
		if err := ValidateLineNo(lineNo); err != nil {
			return err
		}
		w.lineNo = lineNo
		return nil
	}
}

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

// Relocation returns the slot-plan move a RELOCATE unit executes
// (ADR-0040); ok is false for every other unit.
func (w *WorkUnit) Relocation() (move RelocationMove, ok bool) {
	if w.relocation == nil {
		return RelocationMove{}, false
	}
	return *w.relocation, true
}

// WithRelocation records the validated slot-plan move (built by
// NewRelocationMove) at construction (ADR-0040).
func WithRelocation(move RelocationMove) Option {
	return func(w *WorkUnit) error {
		w.relocation = &move
		return nil
	}
}

// Replenishment returns the pick-face replenishment a REPLENISH unit
// executes (ADR-0042); ok is false for every other unit.
func (w *WorkUnit) Replenishment() (move ReplenishmentMove, ok bool) {
	if w.replenish == nil {
		return ReplenishmentMove{}, false
	}
	return *w.replenish, true
}

// WithReplenishment records the validated replenishment (built by
// NewReplenishmentMove) at construction (ADR-0042). The move is the single
// source of the unit's SKU and quantity, so both are set from it and can
// never drift.
func WithReplenishment(move ReplenishmentMove) Option {
	return func(w *WorkUnit) error {
		w.replenish = &move
		w.sku = move.SKU
		w.quantity = move.Quantity
		return nil
	}
}

// Pick returns the pick plan of a CASE_PICK / EACH_PICK unit (ADR-0045); ok
// is false for every other unit.
func (w *WorkUnit) Pick() (plan PickPlan, ok bool) {
	if w.pick == nil {
		return PickPlan{}, false
	}
	return *w.pick, true
}

// WithPick records the validated pick plan (built by NewPickPlan) at
// construction (ADR-0045). The plan is the single source of the unit's SKU,
// quantity (eaches) and pack-aware kind, so all three are set from it and
// can never drift (the WithReplenishment discipline).
func WithPick(plan PickPlan) Option {
	return func(w *WorkUnit) error {
		w.pick = &plan
		w.sku = plan.SKU
		w.quantity = plan.Quantity
		w.workKind = plan.Unit.WorkKind()
		return nil
	}
}

// CaseBreak returns the sealed-case break of a BREAK_CASE unit (ADR-0045);
// ok is false for every other unit.
func (w *WorkUnit) CaseBreak() (brk CaseBreak, ok bool) {
	if w.caseBreak == nil {
		return CaseBreak{}, false
	}
	return *w.caseBreak, true
}

// WithCaseBreak records the validated case break (built by NewCaseBreak) at
// construction (ADR-0045); it sets the unit's SKU and BREAK_CASE kind from
// it.
func WithCaseBreak(brk CaseBreak) Option {
	return func(w *WorkUnit) error {
		w.caseBreak = &brk
		w.sku = brk.SKU
		w.workKind = WorkKindBreakCase
		return nil
	}
}

// BlockedBy is the id of the work unit this unit waits on: a slot-plan move
// into a slot another move of the same plan vacates is admitted to its pool
// only once that move completes (ADR-0040). "" means not blocked.
func (w *WorkUnit) BlockedBy() string { return w.blockedBy }

// IsBlocked reports whether the unit still waits on another unit.
func (w *WorkUnit) IsBlocked() bool { return w.blockedBy != "" }

// WithBlockedBy records at construction the work unit this one waits on.
func WithBlockedBy(workUnitId string) Option {
	return func(w *WorkUnit) error {
		w.blockedBy = workUnitId
		return nil
	}
}

// Unblock clears the wait: the unit it waited on has completed.
func (w *WorkUnit) Unblock() { w.blockedBy = "" }

// Cancel withdraws a Pending unit for good (ADR-0040: a superseded slot
// plan's not-yet-released move). Released and Completed units run to
// completion and refuse with ErrAlreadyReleased; a second Cancel refuses
// with ErrCancelled.
func (w *WorkUnit) Cancel(at time.Time) error {
	if w.state == Cancelled {
		return ErrCancelled
	}
	if w.state != Pending {
		return ErrAlreadyReleased
	}
	w.state = Cancelled
	w.cancelledAt = &at
	return nil
}

// CancelledAt is when the unit was cancelled; nil unless Cancelled.
func (w *WorkUnit) CancelledAt() *time.Time { return w.cancelledAt }

// Release admits the unit into active work. A unit may be assigned/released
// at most once — releasing an already-released or completed unit fails, and
// a cancelled unit is never released.
func (w *WorkUnit) Release(at time.Time) error {
	if w.state == Cancelled {
		return ErrCancelled
	}
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
