// Package release holds the WorkPool aggregate — the queue for one process
// path — and the ReleasePolicy domain service that decides release order.
package release

import "errors"

var (
	ErrDuplicateEntry  = errors.New("work unit already enqueued in this pool")
	ErrUnknownEntry    = errors.New("work unit not found in this pool")
	ErrAlreadyReleased = errors.New("work unit already released from this pool")
	ErrNotReleased     = errors.New("work unit has not been released from this pool yet")
	ErrWIPLimitReached = errors.New("release-fed pool WIP limit reached")
	ErrEmptyPool       = errors.New("no pending work in this pool")

	ErrUnknownFeedMode   = errors.New("unknown work pool feed mode")
	ErrUnknownEntryState = errors.New("unknown work pool entry state")
)
