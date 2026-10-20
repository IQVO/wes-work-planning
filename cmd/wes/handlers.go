package main

import (
	"context"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// newReadiness builds the /readyz gate (ADR-0044): the drain gate AND a
// Postgres ping AND a Kafka broker answering a topic-less metadata request.
// Topics are never required (the writers auto-create theirs, ADR-0042), the
// verdict is cached for 3s and one round is bounded by 2s.
func newReadiness(repos repositories, kafkaBrokers string) *inboundhttp.Readiness {
	deps := []inboundhttp.Dependency{
		{Name: "kafka", Check: outboundkafka.NewBrokerCheck(brokerList(kafkaBrokers)).Check},
	}
	if repos.pgPool != nil {
		deps = append([]inboundhttp.Dependency{{Name: "postgres", Check: func(ctx context.Context) error { return repos.pgPool.Ping(ctx) }}}, deps...)
	}
	return inboundhttp.NewReadiness(inboundhttp.DefaultReadinessTTL, inboundhttp.DefaultReadinessTimeout, deps...)
}

// newHandlers wires every use case into the inbound HTTP handlers,
// including the idempotency pool and readiness gate of their ADRs.
func newHandlers(repos repositories, publisher ports.EventPublisher, clock ports.Clock, catalogue ports.PathCatalogue, travelDistances ports.TravelDistanceLookup, kafkaBrokers string) *inboundhttp.Handlers {
	uow := repos.uow
	return &inboundhttp.Handlers{
		ReceiveChargeForecast:   usecases.NewReceiveChargeForecast(repos.charges, publisher, clock).WithUnitOfWork(uow),
		CommitShiftPlan:         usecases.NewCommitShiftPlan(repos.plans, publisher, clock).WithUnitOfWork(uow).WithTravelDistanceLookup(travelDistances).WithLaborPlanViews(repos.laborPlanViews),
		EnqueueWorkUnit:         usecases.NewEnqueueWorkUnit(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(uow),
		ReleaseNextWork:         usecases.NewReleaseNextWork(repos.pools, repos.workUnits, publisher, clock).WithUnitOfWork(uow).WithMetrics(telemetry.NewReleaseMetrics()),
		RecordCompletion:        usecases.NewRecordCompletion(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(uow).WithRelocationWork(repos.relocations),
		SampleBacklog:           usecases.NewSampleBacklog(repos.pools, publisher, clock).WithUnitOfWork(uow),
		RebalanceDecision:       usecases.NewRebalanceDecision(repos.pools, publisher, clock).WithUnitOfWork(uow),
		Catalogue:               catalogue,
		LaborPlanView:           usecases.NewLaborPlanView(repos.laborPlanViews),
		InventoryView:           usecases.NewInventoryView(repos.inventoryViews),
		GetWorkUnitsByReference: usecases.NewGetWorkUnitsByReference(repos.workUnits),
		GetWorkUnit:             usecases.NewGetWorkUnit(repos.workUnits),
		ConfigurePool:           usecases.NewConfigurePool(repos.pools),
		// IdempotencyPool wires RequireIdempotencyKey onto POST
		// /paths/{pathId}/work-units (see idempotency.go's ADR). Postgres is
		// the only runtime (ADR-0044), so the check is on in every deployment.
		IdempotencyPool: repos.pgPool,
		// Readiness backs GET /readyz (ADR-0023 §graceful shutdown): the
		// drain gate -- flipped to not-ready as the FIRST step of the
		// shutdown sequence -- plus the Postgres and Kafka checks.
		Readiness: newReadiness(repos, kafkaBrokers),
	}
}
