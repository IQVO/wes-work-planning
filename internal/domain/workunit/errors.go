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
	ErrEmptyId          = errors.New("work unit id must not be empty")
	ErrEmptyReference   = errors.New("work unit reference must not be empty")
)
