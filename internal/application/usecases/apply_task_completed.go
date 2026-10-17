package usecases

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// TaskCompletedOutcome reports what ApplyTaskCompleted did with one
// fulfillment-execution TaskCompleted event.
type TaskCompletedOutcome int

const (
	// TaskCompletedApplied: the work unit was completed (and its WIP slot
	// freed) in the same atomic scope as the processed-event mark.
	TaskCompletedApplied TaskCompletedOutcome = iota
	// TaskCompletedAlreadyProcessed: a redelivery of an event already
	// handled; nothing was changed.
	TaskCompletedAlreadyProcessed
	// TaskCompletedUnknownWorkUnit: the event names a work unit this
	// context never planned (e.g. a PACK task fulfillment-execution created
	// itself during rebin consolidation, whose work_unit_id is the order
	// id). A legitimate fact on a shared topic, not a failure: the event is
	// marked processed and nothing else changes.
	TaskCompletedUnknownWorkUnit
	// TaskCompletedAlreadyCompleted: a second, DISTINCT task completed a
	// work unit that was already completed. A fact that no longer
	// applies, not a failure: the event is marked processed (so a
	// redelivery does not re-log the skip) and nothing else changes.
	TaskCompletedAlreadyCompleted
)

// ApplyTaskCompleted is the inbound-event use case behind
// warehouse.fulfillment.events' TaskCompleted. It wraps the existing
// RecordCompletion so the processed-event mark and the completion commit
// atomically (ADR-0028): a transient failure rolls the mark back and the
// event is retried, instead of being swallowed as "already processed" on
// the next attempt — the exact way the WIP ratchet seen in the soak run
// (see RecordCompletion's doc comment) became reachable again.
type ApplyTaskCompleted struct {
	recordCompletion *RecordCompletion
	processed        ports.ProcessedEventRepo
	uow              ports.UnitOfWork
}

func NewApplyTaskCompleted(recordCompletion *RecordCompletion, processed ports.ProcessedEventRepo) *ApplyTaskCompleted {
	return &ApplyTaskCompleted{recordCompletion: recordCompletion, processed: processed}
}

// WithUnitOfWork brackets the processed-event mark and RecordCompletion's
// writes in one atomic scope (RecordCompletion's own scope joins it).
func (uc *ApplyTaskCompleted) WithUnitOfWork(u ports.UnitOfWork) *ApplyTaskCompleted {
	uc.uow = u
	return uc
}

type ApplyTaskCompletedRequest struct {
	EventId    string
	WorkUnitId string
	OccurredAt time.Time
}

// Execute applies one TaskCompleted. Two deterministic domain facts are
// converted into successful outcomes instead of errors (ADR-0043):
// ports.ErrNotFound from RecordCompletion (an unknown work unit) and
// workunit.ErrAlreadyCompleted (a second, DISTINCT task completed a work
// unit that was already completed — a stale fact that no longer applies,
// logged once at INFO). The event is marked processed either way, so a
// redelivery is a cheap no-op that does not re-log the skip. Every other
// error is returned and nothing is committed, so the caller retries and,
// once retries are exhausted, dead-letters the event.
func (uc *ApplyTaskCompleted) Execute(ctx context.Context, req ApplyTaskCompletedRequest) (TaskCompletedOutcome, error) {
	outcome := TaskCompletedApplied
	alreadyProcessed, err := onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.OccurredAt, func(ctx context.Context) error {
		_, err := uc.recordCompletion.Execute(ctx, RecordCompletionRequest{WorkUnitId: req.WorkUnitId})
		if errors.Is(err, ports.ErrNotFound) {
			outcome = TaskCompletedUnknownWorkUnit
			return nil
		}
		if errors.Is(err, workunit.ErrAlreadyCompleted) {
			outcome = TaskCompletedAlreadyCompleted
			slog.InfoContext(ctx, "TaskCompleted for a work unit that is already completed; skipping",
				"event_id", req.EventId, "work_unit_id", req.WorkUnitId)
			return nil
		}
		return err
	})
	if err != nil {
		return TaskCompletedApplied, err
	}
	if alreadyProcessed {
		return TaskCompletedAlreadyProcessed, nil
	}
	return outcome, nil
}
