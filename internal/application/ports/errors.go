package ports

import "errors"

// ErrNotFound is returned by repository FindBy* methods when no record
// matches.
var ErrNotFound = errors.New("not found")

// ErrConcurrentModification is returned by a repository Save when the
// aggregate changed since it was loaded (optimistic concurrency). The
// caller re-loads and retries; nothing was written.
var ErrConcurrentModification = errors.New("concurrent modification")
