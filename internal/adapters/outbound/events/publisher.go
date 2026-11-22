// Package events provides an EventPublisher that logs and buffers domain
// events. Publisher is an interface so a Kafka-backed implementation can
// later satisfy the same seam.
package events

import (
	"context"
	"log/slog"
	"sync"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// Publisher is the seam a future broker-backed publisher (e.g. Kafka) would
// implement. LogPublisher below is the log/in-memory implementation used
// today.
type Publisher interface {
	Publish(ctx context.Context, events ...shared.DomainEvent) error
}

// LogPublisher logs each published event and buffers it in memory, so tests
// can assert on what was published.
type LogPublisher struct {
	mu     sync.Mutex
	logger *slog.Logger
	events []shared.DomainEvent
}

// NewLogPublisher returns a publisher that logs each event through logger
// and buffers it. A nil logger disables the logging half, which the tests
// rely on.
func NewLogPublisher(logger *slog.Logger) *LogPublisher {
	return &LogPublisher{logger: logger}
}

func (p *LogPublisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range events {
		if p.logger != nil {
			p.logger.InfoContext(ctx, "domain event published",
				"event", sanitizeLog(e.EventName()),
				"occurred_at", e.OccurredAt(),
			)
		}
		p.events = append(p.events, e)
	}
	return nil
}

// Events returns a copy of every event published so far.
func (p *LogPublisher) Events() []shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]shared.DomainEvent, len(p.events))
	copy(out, p.events)
	return out
}
