package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
)

// onceAtomically is the ONE idempotent-consumer primitive every inbound
// integration-event use case shares (ADR-0028). It records eventId as
// processed and runs apply inside the SAME atomic scope, so the
// processed-event mark commits if and only if apply's own writes commit:
//
//   - apply returns nil   -> the mark and the effect commit together.
//   - apply returns error -> the scope rolls back, the mark with it, and a
//     retry of the same event re-runs apply instead of mistaking it for an
//     already-handled redelivery.
//
// The previous shape (mark committed first, effect applied afterwards as a
// separate write) turned every failed first attempt into a silently lost
// event: the retry saw the mark, reported success, and the offset was
// committed without the effect ever being applied.
//
// alreadyProcessed reports a genuine redelivery; apply was not called.
//
// A nil uow (the in-memory wiring) has no transaction to roll the mark
// back with, so on failure the mark is undone explicitly through
// ports.ProcessedEventReleaser when the repo supports it.
func onceAtomically(ctx context.Context, uow ports.UnitOfWork, processed ports.ProcessedEventRepo, eventId string, at time.Time, apply func(ctx context.Context) error) (alreadyProcessed bool, err error) {
	marked := false
	err = atomically(ctx, uow, func(ctx context.Context) error {
		seen, err := processed.TryMarkProcessed(ctx, eventId, at)
		if err != nil {
			return err
		}
		if seen {
			alreadyProcessed = true
			return nil
		}
		marked = true
		return apply(ctx)
	})
	if err == nil {
		return alreadyProcessed, nil
	}
	if uow == nil && marked {
		err = releaseMark(ctx, processed, eventId, err)
	}
	return false, err
}

// releaseMark undoes a non-transactional processed-event mark after the
// effect it guarded failed, joining any release failure onto cause.
func releaseMark(ctx context.Context, processed ports.ProcessedEventRepo, eventId string, cause error) error {
	releaser, ok := processed.(ports.ProcessedEventReleaser)
	if !ok {
		return cause
	}
	if err := releaser.ReleaseProcessed(ctx, eventId); err != nil {
		return errors.Join(cause, fmt.Errorf("release processed-event mark %s: %w", eventId, err))
	}
	return cause
}
