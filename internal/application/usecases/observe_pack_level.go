package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// ObservePackLevel is the additive projector use case behind product-master's
// ProductPackLevelDeclared / ProductPackLevelRetired / ProductPackLevelMeasured
// events: it applies each to this context's local pack-level copy, keyed by
// GTIN (ADR-0045). The copy is what the SKU's Each / Case pack facts are read
// from at enqueue, commit and estimate time.
//
// The CloudEvents id is claimed in processed_events and the version-guarded
// write runs in the SAME atomic scope (onceAtomically, ADR-0028).
type ObservePackLevel struct {
	copies    ports.PackLevelCopyRepo
	processed ports.ProcessedEventRepo
	uow       ports.UnitOfWork
}

// NewObservePackLevel constructs the use case over the copy and the
// processed-event set.
func NewObservePackLevel(copies ports.PackLevelCopyRepo, processed ports.ProcessedEventRepo) *ObservePackLevel {
	return &ObservePackLevel{copies: copies, processed: processed}
}

// WithUnitOfWork brackets the processed-event mark and the write in one
// transaction (ADR-0028).
func (uc *ObservePackLevel) WithUnitOfWork(u ports.UnitOfWork) *ObservePackLevel {
	uc.uow = u
	return uc
}

// PackLevelEvent says which product-master event a request stands for.
type PackLevelEvent int

const (
	PackLevelDeclared PackLevelEvent = iota
	PackLevelRetired
	PackLevelMeasured
)

// ErrUnknownPackLevelEvent marks a request whose Event is none of the three.
var ErrUnknownPackLevelEvent = errors.New("usecases: unknown pack level event")

// ObservePackLevelRequest is one pack-level occurrence. Declared carries the
// full Record; Retired uses Record.GTIN, Record.EffectiveTo and
// Record.Version; Measured uses Record.GTIN, Record.Measured and
// Record.Version.
type ObservePackLevelRequest struct {
	EventId    string
	Event      PackLevelEvent
	Record     productclassificationview.PackLevelRecord
	ObservedAt time.Time
}

// PackLevelOutcome says what Execute did with one event.
type PackLevelOutcome int

const (
	// PackLevelApplied: the copy now reflects this event.
	PackLevelApplied PackLevelOutcome = iota
	// PackLevelStale: the copy held this or a newer version (or, for a
	// Retired / Measured, no row for the GTIN); nothing changed.
	PackLevelStale
	// PackLevelRedelivered: this CloudEvents id was already processed.
	PackLevelRedelivered
)

// String names the outcome for logs.
func (o PackLevelOutcome) String() string {
	switch o {
	case PackLevelApplied:
		return "applied"
	case PackLevelStale:
		return "stale"
	case PackLevelRedelivered:
		return "redelivered"
	default:
		return "unknown"
	}
}

// Execute claims req.EventId and applies the event. A non-nil error means
// nothing was committed and the event must be retried.
func (uc *ObservePackLevel) Execute(ctx context.Context, req ObservePackLevelRequest) (PackLevelOutcome, error) {
	applied := false
	alreadyProcessed, err := onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.ObservedAt, func(ctx context.Context) error {
		var err error
		switch req.Event {
		case PackLevelDeclared:
			applied, err = uc.copies.ApplyDeclared(ctx, req.Record)
		case PackLevelRetired:
			applied, err = uc.copies.ApplyRetired(ctx, req.Record.GTIN, req.Record.EffectiveTo, req.Record.Version)
		case PackLevelMeasured:
			applied, err = uc.copies.ApplyMeasured(ctx, req.Record.GTIN, req.Record.Measured, req.Record.Version)
		default:
			err = ErrUnknownPackLevelEvent
		}
		return err
	})
	switch {
	case err != nil:
		return PackLevelStale, err
	case alreadyProcessed:
		return PackLevelRedelivered, nil
	case applied:
		return PackLevelApplied, nil
	default:
		return PackLevelStale, nil
	}
}
