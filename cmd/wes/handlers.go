package main

import (
	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// newHandlers wires every use case into the inbound HTTP handlers,
// including the idempotency pool and readiness gate of their ADRs.
func newHandlers(repos repositories, publisher ports.EventPublisher, clock memory.SystemClock, catalogue ports.PathCatalogue, travelDistances ports.TravelDistanceLookup) *inboundhttp.Handlers {
	uow := repos.uow
	return &inboundhttp.Handlers{
		ReceiveChargeForecast:   usecases.NewReceiveChargeForecast(repos.charges, publisher, clock).WithUnitOfWork(uow),
		CommitShiftPlan:         usecases.NewCommitShiftPlan(repos.plans, publisher, clock).WithUnitOfWork(uow).WithTravelDistanceLookup(travelDistances).WithLaborPlanViews(repos.laborPlanViews),
		EnqueueWorkUnit:         usecases.NewEnqueueWorkUnit(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(uow),
		ReleaseNextWork:         usecases.NewReleaseNextWork(repos.pools, repos.workUnits, publisher, clock).WithUnitOfWork(uow).WithMetrics(telemetry.NewReleaseMetrics()),
		RecordCompletion:        usecases.NewRecordCompletion(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(uow),
		SampleBacklog:           usecases.NewSampleBacklog(repos.pools, publisher, clock).WithUnitOfWork(uow),
		RebalanceDecision:       usecases.NewRebalanceDecision(repos.pools, publisher, clock).WithUnitOfWork(uow),
		Catalogue:               catalogue,
		LaborPlanView:           usecases.NewLaborPlanView(repos.laborPlanViews),
		InventoryView:           usecases.NewInventoryView(repos.inventoryViews),
		GetWorkUnitsByReference: usecases.NewGetWorkUnitsByReference(repos.workUnits),
		GetWorkUnit:             usecases.NewGetWorkUnit(repos.workUnits),
		// IdempotencyPool wires RequireIdempotencyKey onto POST
		// /paths/{pathId}/work-units (see idempotency.go's ADR). nil in
		// the in-memory configuration (pgPool nil), matching every other
		// optional Postgres-backed capability's convention here.
		IdempotencyPool: repos.pgPool,
		// Readiness backs GET /readyz (ADR-0023 §graceful shutdown) --
		// flipped to not-ready as the FIRST step of the shutdown
		// sequence below, before anything else stops.
		Readiness: &inboundhttp.Readiness{},
	}
}
