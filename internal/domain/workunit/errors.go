// Package workunit holds the WorkUnit aggregate: a releasable unit of work
// (e.g. a pick task) that is assigned at most once at a time and cannot
// complete twice.
package workunit

import "errors"

var (
	ErrAlreadyReleased  = errors.New("work unit is already released")
	ErrNotReleased      = errors.New("work unit must be released before it can complete")
	ErrAlreadyCompleted = errors.New("work unit is already completed")
	ErrUnknownState     = errors.New("unknown work unit state")
	// ErrMissingTransitionTime marks a persisted Released/Completed work
	// unit whose released_at/completed_at is NULL: the stored row
	// contradicts its own state and cannot be rehydrated.
	ErrMissingTransitionTime = errors.New("persisted work unit state has no matching transition timestamp")
	ErrEmptyId               = errors.New("work unit id must not be empty")
	ErrEmptyReference        = errors.New("work unit reference must not be empty")
	ErrUnknownWorkKind       = errors.New("unknown transfer work kind")
	// ErrUnknownRelocationKind marks a slot-plan move kind other than
	// Relocate, Assign or Vacate (ADR-0040).
	ErrUnknownRelocationKind = errors.New("unknown relocation kind")
	// ErrInvalidRelocationMove marks a slot-plan move that is incomplete or
	// inconsistent: no plan id, a negative move_seq, a slot its kind
	// requires is missing, or both slots are the same (ADR-0040).
	ErrInvalidRelocationMove = errors.New("invalid relocation move")
	// ErrUnknownReplenishmentPriority marks a replenishment priority other
	// than NORMAL or URGENT (ADR-0042).
	ErrUnknownReplenishmentPriority = errors.New("unknown replenishment priority")
	// ErrInvalidReplenishmentMove marks a pick-face replenishment that is
	// incomplete or inconsistent: no request id (or one longer than 200
	// characters), no SKU or bin, equal bins, or a quantity below 1
	// (ADR-0042).
	ErrInvalidReplenishmentMove = errors.New("invalid replenishment move")
	// ErrCancelled marks a transition attempted on a Cancelled work unit:
	// Cancelled is terminal (ADR-0040).
	ErrCancelled = errors.New("work unit is cancelled")
	// ErrInvalidLineNo marks a line number outside 1..MaxLineNo
	// (2147483647, math.MaxInt32: every line_no column is a 32-bit
	// INTEGER). 0 is not an error: it is the "unknown" value of the
	// optional line_no hint (ADR-0036).
	ErrInvalidLineNo = errors.New("work unit line number must be between 1 and 2147483647 when given")
	// ErrUnknownPickUnit marks a pick unit other than EACH or CASE (ADR-0045).
	ErrUnknownPickUnit = errors.New("unknown pick unit")
	// ErrInvalidPickPlan marks a pick that is incomplete or inconsistent: no
	// SKU, a quantity below 1 (eaches), or a pick on the BREAK_CASE path
	// (ADR-0045).
	ErrInvalidPickPlan = errors.New("invalid pick plan")
	// ErrInvalidCaseBreak marks a case break that is incomplete or
	// inconsistent: a malformed lpn, no SKU, loose bin or pick task id, or a
	// path other than BREAK_CASE (ADR-0045).
	ErrInvalidCaseBreak = errors.New("invalid case break")
)
