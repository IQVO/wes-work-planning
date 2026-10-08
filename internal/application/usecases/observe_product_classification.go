package usecases

import (
	"context"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// ObserveProductClassification is an additive projector use case: it applies
// product-master's ProductClassified integration event to this context's
// local classification copy, keyed by SKU (ADR-0035). The copy is what
// ports.ProductClassificationLookup reads at release time.
//
// The CloudEvents id is claimed in processed_events and the version-guarded
// upsert runs in the SAME atomic scope (onceAtomically, ADR-0028), so a
// failed upsert leaves no mark and the consumer's retry re-applies it.
type ObserveProductClassification struct {
	copies    ports.ProductClassificationCopyRepo
	processed ports.ProcessedEventRepo
	uow       ports.UnitOfWork
}

// NewObserveProductClassification constructs the use case over the copy and
// the processed-event set.
func NewObserveProductClassification(copies ports.ProductClassificationCopyRepo, processed ports.ProcessedEventRepo) *ObserveProductClassification {
	return &ObserveProductClassification{copies: copies, processed: processed}
}

// WithUnitOfWork brackets the processed-event mark and the upsert in one
// transaction (ADR-0028). A nil UnitOfWork (in-memory wiring) runs them back
// to back and releases the mark if the upsert fails.
func (uc *ObserveProductClassification) WithUnitOfWork(u ports.UnitOfWork) *ObserveProductClassification {
	uc.uow = u
	return uc
}

// ObserveProductClassificationRequest is one ProductClassified occurrence:
// a full-state replacement of the SKU's classification at Version.
type ObserveProductClassificationRequest struct {
	EventId          string
	SKU              string
	HandlingTags     []string
	TemperatureClass string // "" when unset
	DOTHazardClass   int    // 0 when unset
	Version          int64
	ObservedAt       time.Time
}

// ProductClassificationOutcome says what Execute did with one event.
type ProductClassificationOutcome int

const (
	// ProductClassificationApplied: the copy now holds this version.
	ProductClassificationApplied ProductClassificationOutcome = iota
	// ProductClassificationStale: the copy already held this or a newer
	// version; nothing changed (the event id is still recorded).
	ProductClassificationStale
	// ProductClassificationRedelivered: this CloudEvents id was already
	// processed; nothing ran.
	ProductClassificationRedelivered
)

// String names the outcome for logs.
func (o ProductClassificationOutcome) String() string {
	switch o {
	case ProductClassificationApplied:
		return "applied"
	case ProductClassificationStale:
		return "stale"
	case ProductClassificationRedelivered:
		return "redelivered"
	default:
		return "unknown"
	}
}

// Execute claims req.EventId and applies the classification if its version
// is newer than the stored one. A non-nil error means nothing was committed
// (the claim rolled back with the effect) and the event must be retried.
func (uc *ObserveProductClassification) Execute(ctx context.Context, req ObserveProductClassificationRequest) (ProductClassificationOutcome, error) {
	view := productclassificationview.ProductClassificationView{
		SKU:              req.SKU,
		HandlingTags:     req.HandlingTags,
		TemperatureClass: req.TemperatureClass,
		Known:            true,
	}
	applied := false
	alreadyProcessed, err := onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.ObservedAt, func(ctx context.Context) error {
		var err error
		applied, err = uc.copies.ApplyIfNewer(ctx, view, req.DOTHazardClass, req.Version, req.ObservedAt)
		return err
	})
	switch {
	case err != nil:
		return ProductClassificationStale, err
	case alreadyProcessed:
		return ProductClassificationRedelivered, nil
	case applied:
		return ProductClassificationApplied, nil
	default:
		return ProductClassificationStale, nil
	}
}
