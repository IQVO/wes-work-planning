package memory

import (
	"context"
	"sync"
	"time"
)

// ProcessedEventRepo is a thread-safe in-memory ports.ProcessedEventRepo,
// backed by a set of already-seen event IDs.
type ProcessedEventRepo struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func NewProcessedEventRepo() *ProcessedEventRepo {
	return &ProcessedEventRepo{seen: make(map[string]struct{})}
}

func (r *ProcessedEventRepo) TryMarkProcessed(ctx context.Context, eventId string, processedAt time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.seen[eventId]; ok {
		return true, nil
	}
	r.seen[eventId] = struct{}{}
	return false, nil
}

// ReleaseProcessed forgets eventId, implementing ports.ProcessedEventReleaser:
// the in-memory stand-in for a rolled-back processed-event insert, used
// when the effect the mark guarded failed (ADR-0028).
func (r *ProcessedEventRepo) ReleaseProcessed(ctx context.Context, eventId string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.seen, eventId)
	return nil
}
