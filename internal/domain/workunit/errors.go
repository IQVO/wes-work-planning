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
	// ErrInvalidLineNo marks a line number outside 1..MaxLineNo
	// (2147483647, math.MaxInt32: every line_no column is a 32-bit
	// INTEGER). 0 is not an error: it is the "unknown" value of the
	// optional line_no hint (ADR-0036).
	ErrInvalidLineNo = errors.New("work unit line number must be between 1 and 2147483647 when given")
)
