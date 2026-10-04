//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ADR-0019: PathPlan vs LaborPlanObserved reconciliation against a real
// Postgres + the transactional outbox. Both triggers raise
// PathPlanDriftDetected on BOTH topics through the outbox, persist the outcome
// on labor_plan_view, and commit/roll back together with the use case.

const driftType = "com.warehouse.wes.work-planning.pathplan.PathPlanDriftDetected"

func laborViewFixture(pathId shared.PathId, heads int) laborview.LaborPlanObserved {
	return laborview.LaborPlanObserved{
		PathId: pathId, PlannedHeads: heads, PlannedRate: 100, PlannedHours: 8,
		ObservedAt: time.Date(2026, 10, 4, 7, 0, 0, 0, time.UTC),
	}
}

func commitReq(t *testing.T, pathId shared.PathId, heads int) usecases.CommitShiftPlanRequest {
	t.Helper()
	ph, _ := shared.NewStationCount(heads)
	is, _ := shared.NewStationCount(20)
	rate, _ := shared.NewRate(100)
	return usecases.CommitShiftPlanRequest{PathId: pathId, PlannedHeads: ph, InstalledStations: is, Rate: rate, Hours: 8}
}

func TestDrift_BothTriggers_RaiseThroughOutboxAndPersistOnTheView(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	clock := memory.FixedClock{At: time.Date(2026, 10, 4, 8, 0, 0, 0, time.UTC)}
	integration, analytics := encoders(pool)
	pub := postgres.NewOutboxPublisher(pool, integration, analytics)
	uow := postgres.NewUnitOfWork(pool)
	plans := postgres.NewPlanRepo(pool)
	views := postgres.NewLaborPlanViewRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)

	commit := usecases.NewCommitShiftPlan(plans, pub, clock).WithUnitOfWork(uow).WithLaborPlanViews(views)
	observe := usecases.NewObserveLaborPlan(views, processed).WithUnitOfWork(uow).WithDriftReconciliation(plans, pub, clock)
	observedAt := time.Date(2026, 10, 4, 7, 0, 0, 0, time.UTC)

	// ---- trigger 1: Workforce committed FIRST, our plan commits second ----
	pathA := mustPath(t, "pick-drift-a")
	if err := observe.Execute(ctx, usecases.ObserveLaborPlanRequest{EventId: "evt-a", PathId: pathA, PlannedHeads: 7, PlannedRate: 100, PlannedHours: 8, ObservedAt: observedAt}); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if got := countOutbox(t, pool, "event_type = '"+driftType+"'"); got != 0 {
		t.Fatalf("no PathPlan yet: want 0 drift rows, got %d", got)
	}
	if v, _ := views.FindByPathId(ctx, pathA); v.DriftHeads != nil {
		t.Fatalf("no comparison yet: drift must be nil, got %d", *v.DriftHeads)
	}
	if _, err := commit.Execute(ctx, commitReq(t, pathA, 6)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for _, topic := range []string{cloudevents.TopicWorkPlanningEvents, outboundkafka.AnalyticsTopic} {
		if got := countOutbox(t, pool, "topic = '"+topic+"' AND event_type = '"+driftType+"'"); got != 1 {
			t.Fatalf("topic %s: want 1 PathPlanDriftDetected outbox row, got %d", topic, got)
		}
	}
	v, err := views.FindByPathId(ctx, pathA)
	if err != nil {
		t.Fatalf("find view: %v", err)
	}
	if v.DriftHeads == nil || *v.DriftHeads != 1 || v.DriftDetectedAt == nil || !v.DriftDetectedAt.Equal(clock.Now()) {
		t.Fatalf("view drift = %v @ %v, want +1 @ %v", v.DriftHeads, v.DriftDetectedAt, clock.Now())
	}
	if v.PlannedHeads != 7 || !v.ObservedAt.Equal(observedAt) {
		t.Fatalf("our commit must not rewrite the observed plan: %+v", v)
	}

	// ---- trigger 2: our plan committed FIRST, Workforce commits second ----
	pathB := mustPath(t, "pick-drift-b")
	if _, err := commit.Execute(ctx, commitReq(t, pathB, 6)); err != nil {
		t.Fatalf("commit b: %v", err)
	}
	if err := observe.Execute(ctx, usecases.ObserveLaborPlanRequest{EventId: "evt-b", PathId: pathB, PlannedHeads: 4, PlannedRate: 100, PlannedHours: 8, ObservedAt: observedAt}); err != nil {
		t.Fatalf("observe b: %v", err)
	}
	var value []byte
	if err := pool.QueryRow(ctx, "SELECT value FROM outbox_events WHERE topic = $1 AND event_type = $2 AND convert_from(value,'UTF8') LIKE '%pick-drift-b%'", cloudevents.TopicWorkPlanningEvents, driftType).Scan(&value); err != nil {
		t.Fatalf("read drift row for b: %v", err)
	}
	env, err := cloudevents.Decode(value)
	if err != nil {
		t.Fatalf("drift row is not a CloudEvent: %v", err)
	}
	var data map[string]any
	if err := env.DataAs(&data); err != nil {
		t.Fatal(err)
	}
	if env.Subject() != "pick-drift-b" || data["drift_heads"] != float64(-2) || data["wes_planned_heads"] != float64(6) || data["observed_planned_heads"] != float64(4) {
		t.Fatalf("unexpected drift payload: subject=%s data=%v", env.Subject(), data)
	}

	// ---- agreeing plans raise nothing but record driftHeads = 0 ----
	pathC := mustPath(t, "pick-drift-c")
	if _, err := commit.Execute(ctx, commitReq(t, pathC, 5)); err != nil {
		t.Fatal(err)
	}
	if err := observe.Execute(ctx, usecases.ObserveLaborPlanRequest{EventId: "evt-c", PathId: pathC, PlannedHeads: 5, PlannedRate: 100, PlannedHours: 8, ObservedAt: observedAt}); err != nil {
		t.Fatal(err)
	}
	if got := countOutbox(t, pool, "event_type = '"+driftType+"' AND convert_from(value,'UTF8') LIKE '%pick-drift-c%'"); got != 0 {
		t.Fatalf("agreeing plans raised %d drift rows", got)
	}
	if v, _ := views.FindByPathId(ctx, pathC); v.DriftHeads == nil || *v.DriftHeads != 0 || v.DriftDetectedAt != nil {
		t.Fatalf("agreeing view drift = %v @ %v, want 0 and no time", v.DriftHeads, v.DriftDetectedAt)
	}
}

// An encoder that rejects only the drift event, to prove the drift publish is
// inside the use case's transaction.
type driftRejectingEncoder struct {
	inner interface {
		Encode(context.Context, ...shared.DomainEvent) ([]outboundkafka.Encoded, error)
	}
	err error
}

func (e driftRejectingEncoder) Encode(ctx context.Context, evs ...shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	for _, ev := range evs {
		if _, ok := ev.(shared.PathPlanDriftDetected); ok {
			return nil, e.err
		}
	}
	return e.inner.Encode(ctx, evs...)
}

func TestDrift_PublishFailureRollsBackTheWholeUseCase(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	clock := memory.FixedClock{At: time.Date(2026, 10, 4, 8, 0, 0, 0, time.UTC)}
	integration, analytics := encoders(pool)
	boom := errors.New("encode exploded")
	pub := postgres.NewOutboxPublisher(pool, driftRejectingEncoder{integration, boom}, driftRejectingEncoder{analytics, boom})
	uow := postgres.NewUnitOfWork(pool)
	plans := postgres.NewPlanRepo(pool)
	views := postgres.NewLaborPlanViewRepo(pool)

	// Workforce committed 8; our commit of 6 must raise drift — which fails.
	pathId := mustPath(t, "pick-drift-rollback")
	if err := views.Save(ctx, laborViewFixture(pathId, 8)); err != nil {
		t.Fatalf("seed view: %v", err)
	}
	commit := usecases.NewCommitShiftPlan(plans, pub, clock).WithUnitOfWork(uow).WithLaborPlanViews(views)
	if _, err := commit.Execute(ctx, commitReq(t, pathId, 6)); !errors.Is(err, boom) {
		t.Fatalf("commit err = %v, want the encode failure", err)
	}
	if got := countRows(t, pool, "shift_plans", "path_id = 'pick-drift-rollback'"); got != 0 {
		t.Fatalf("the PathPlan row must roll back with the failed drift publish, got %d", got)
	}
	if got := countOutbox(t, pool, "convert_from(value,'UTF8') LIKE '%pick-drift-rollback%'"); got != 0 {
		t.Fatalf("no outbox rows may survive the rollback, got %d", got)
	}
	if v, _ := views.FindByPathId(ctx, pathId); v.DriftHeads != nil {
		t.Fatalf("drift must not be recorded when the use case rolled back, got %d", *v.DriftHeads)
	}
}
