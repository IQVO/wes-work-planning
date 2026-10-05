// Package ports defines the driven (outbound) port interfaces the
// application layer depends on: repositories, event publishing, and clock.
package ports

import (
	"context"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/charge"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/plan"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ChargeRepo persists and retrieves ChargeForecast aggregates, one per path.
type ChargeRepo interface {
	Save(ctx context.Context, forecast *charge.ChargeForecast) error
	FindByPathId(ctx context.Context, pathId shared.PathId) (*charge.ChargeForecast, error)
}

// PlanRepo persists and retrieves ShiftPlan aggregates, one per path.
type PlanRepo interface {
	Save(ctx context.Context, pathId shared.PathId, shiftPlan *plan.ShiftPlan) error
	FindByPathId(ctx context.Context, pathId shared.PathId) (*plan.ShiftPlan, error)
}

// WorkPoolRepo persists and retrieves WorkPool aggregates, one per path.
type WorkPoolRepo interface {
	Save(ctx context.Context, pool *release.WorkPool) error
	FindByPathId(ctx context.Context, pathId shared.PathId) (*release.WorkPool, error)
}

// WorkUnitRepo persists and retrieves WorkUnit aggregates.
type WorkUnitRepo interface {
	Save(ctx context.Context, unit *workunit.WorkUnit) error
	FindById(ctx context.Context, id string) (*workunit.WorkUnit, error)
	FindByPathId(ctx context.Context, pathId shared.PathId) ([]*workunit.WorkUnit, error)
	// FindByReference returns every WorkUnit ever enqueued for the given
	// external reference (e.g. an order line). A reference can plausibly
	// have more than one WorkUnit across retries/history (e.g. a work unit
	// enqueued, its process abandoned, and a fresh one enqueued for the
	// same order line under a new workUnitId) — callers must treat the
	// result as a history, not assume at most one match. Returns an empty
	// slice (not ports.ErrNotFound) when no work unit carries this
	// reference; that mirrors FindByPathId's empty-collection semantics
	// rather than FindById's single-resource semantics.
	FindByReference(ctx context.Context, reference string) ([]*workunit.WorkUnit, error)
}

// EventPublisher publishes domain events raised by use cases.
type EventPublisher interface {
	Publish(ctx context.Context, events ...shared.DomainEvent) error
}

// UnitOfWork brackets a use case's state change and the domain events it
// raises so both commit or neither does (ADR-0014, transactional outbox).
//
// Execute runs fn inside one atomic scope. Every Repo.Save and
// EventPublisher.Publish made with the ctx handed to fn is bound to that
// same scope: if fn returns an error the scope is rolled back and nothing
// — neither the aggregate rows nor the outbox rows — is visible afterwards.
//
// Adapters that have no transactional backing (the in-memory repos, the
// log publisher, the direct Kafka publishers) need no implementation: the
// use cases treat a nil UnitOfWork as "run fn directly", so they stay
// adapter-agnostic either way.
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}

// Clock abstracts "now" so use cases and tests are deterministic.
type Clock interface {
	Now() time.Time
}

// ReleaseMetrics is the outbound telemetry port for the business signal
// ReleaseNextWork produces (ADR-0013 Tier 2): a work unit actually admitted
// into a path's pool. The OTel implementation lives in
// adapters/outbound/telemetry; the application layer never imports OTel. A
// nil ReleaseMetrics on the use case means "record nothing".
type ReleaseMetrics interface {
	WorkUnitReleased(ctx context.Context, pathId shared.PathId)
}

// PathCatalogue is the outbound port for the fleet's declared process-path
// catalogue — matches pathcatalog.Catalogue's own Lookup signature exactly,
// so *pathcatalog.Catalogue already satisfies this interface with no
// changes. Introduced so an alternative adapter (e.g. a Kafka-sourced
// catalogue, see internal/adapters/outbound/kafkacatalog) can be wired in
// wherever a *pathcatalog.Catalogue was previously required directly.
type PathCatalogue interface {
	Lookup(id string) (pathcatalog.PathDefinition, error)
}
