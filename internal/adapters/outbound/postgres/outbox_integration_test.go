//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// These tests own their database: a throwaway testcontainers Postgres with
// the real migrations applied (startPostgres/migrationsDir live in
// migrate_integration_test.go). They never read DATABASE_URL and never
// skip — a broken outbox must fail CI, not silently pass.

// migratedDSN boots a throwaway Postgres, applies the real migrations, and
// returns its connection string — for tests that must build the pool
// themselves (e.g. after installing a tracer provider).
func migratedDSN(t *testing.T) string {
	t.Helper()
	dsn := startPostgres(t)
	if err := postgres.Migrate(dsn, migrationsDir(t)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return dsn
}

// outboxDB boots Postgres, migrates it, and returns a pool.
func outboxDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := migratedDSN(t)
	pool, err := postgres.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// noopWriter satisfies outboundkafka.Writer for publishers that are only
// ever used as Encoders here (the outbox never calls WriteMessages).
type noopWriter struct{}

func (noopWriter) WriteMessages(context.Context, ...kafkago.Message) error { return nil }
func (noopWriter) Close() error                                            { return nil }

// encoders builds the two production encoders exactly as the composition
// root does: the integration publisher reads workUnits at encode time.
func encoders(pool *pgxpool.Pool) (*outboundkafka.Publisher, *outboundkafka.AnalyticsPublisher) {
	ids := sequentialIDs()
	return outboundkafka.NewPublisherWithWriter(noopWriter{}, postgres.NewWorkUnitRepo(pool), nil, ids),
		outboundkafka.NewAnalyticsPublisherWithWriter(noopWriter{}, ids)
}

func sequentialIDs() outboundkafka.IDGenerator {
	n := 0
	return func() string {
		n++
		return "evt-" + strings.Repeat("0", 3-len(itoa(n))) + itoa(n)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

type recordingSink struct {
	sent    []outboundkafka.Encoded
	failOn  string // event_type to fail on, "" for never
	failErr error
}

func (s *recordingSink) Send(_ context.Context, msgs ...outboundkafka.Encoded) error {
	for _, m := range msgs {
		if s.failOn != "" && m.EventType == s.failOn {
			return s.failErr
		}
		s.sent = append(s.sent, m)
	}
	return nil
}

// failingEncoder forces the outbox insert path to error, so the use case's
// transaction must roll back everything it saved.
type failingEncoder struct{ err error }

func (f failingEncoder) Encode(context.Context, ...shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	return nil, f.err
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox_events WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

func countRows(t *testing.T, pool *pgxpool.Pool, table, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table+" WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func mustPath(t *testing.T, s string) shared.PathId {
	t.Helper()
	p, err := shared.NewPathId(s)
	if err != nil {
		t.Fatalf("path id: %v", err)
	}
	return p
}

// 1. commit-together: the aggregate rows AND one outbox row per topic are
// visible after the use case; the WorkReleased integration payload carries
// the enrichment that could only have been read inside the transaction.
func TestOutbox_EnqueueAndRelease_CommitAggregateAndBothTopicsTogether(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	clock := memory.FixedClock{At: time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC)}
	integration, analytics := encoders(pool)
	pub := postgres.NewOutboxPublisher(pool, integration, analytics)
	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	pathId := mustPath(t, "pick-a")

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, pub, clock).WithUnitOfWork(uow)
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Reference: "order-42", SKU: "sku-9",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if got := countRows(t, pool, "work_units", "id = 'wu-1'"); got != 1 {
		t.Fatalf("expected work unit persisted, got %d", got)
	}
	if got := countRows(t, pool, "work_pool_entries", "work_unit_id = 'wu-1'"); got != 1 {
		t.Fatalf("expected pool entry persisted (WorkPoolRepo joined the transaction), got %d", got)
	}
	if got := countOutbox(t, pool, "topic = '"+cloudevents.TopicWorkPlanningEvents+"' AND event_type = 'com.warehouse.wes.work-planning.workunit.WorkUnitCreated' AND published_at IS NULL"); got != 1 {
		t.Fatalf("expected 1 integration outbox row for WorkUnitCreated, got %d", got)
	}
	if got := countOutbox(t, pool, "topic = '"+outboundkafka.AnalyticsTopic+"' AND event_type = 'com.warehouse.wes.work-planning.workunit.WorkUnitCreated' AND published_at IS NULL"); got != 1 {
		t.Fatalf("expected 1 analytics outbox row for WorkUnitCreated, got %d", got)
	}

	releaseUC := usecases.NewReleaseNextWork(pools, workUnits, pub, clock).WithUnitOfWork(uow)
	if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("release: %v", err)
	}
	var value []byte
	if err := pool.QueryRow(ctx, "SELECT value FROM outbox_events WHERE topic = $1 AND event_type = 'com.warehouse.wes.work-planning.workunit.WorkReleased'", cloudevents.TopicWorkPlanningEvents).Scan(&value); err != nil {
		t.Fatalf("read WorkReleased row: %v", err)
	}
	// ref and cpt come from Publisher.dataFor reading work_units — only
	// visible because Encode ran inside the use case's transaction (the
	// row is not committed yet when the outbox insert happens).
	if !strings.Contains(string(value), `"ref":"order-42"`) || !strings.Contains(string(value), `"work_unit_id":"wu-1"`) || strings.Contains(string(value), `"cpt":""`) {
		t.Fatalf("WorkReleased payload was not enriched from the in-transaction work unit row: %s", value)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 4 {
		t.Fatalf("expected 4 pending rows (2 events x 2 topics), got %d", got)
	}
}

// 2. rollback: when the outbox insert fails, NONE of the aggregate rows
// the use case wrote survive — including WorkPoolRepo's own multi-statement
// save, which must have joined the outer transaction rather than committed
// on its own.
func TestOutbox_EncodeFailure_RollsBackEveryAggregateWrite(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	clock := memory.FixedClock{At: time.Now().UTC()}
	pub := postgres.NewOutboxPublisher(pool, failingEncoder{err: errors.New("encode exploded")})
	uow := postgres.NewUnitOfWork(pool)
	pathId := mustPath(t, "pick-b")

	enqueue := usecases.NewEnqueueWorkUnit(postgres.NewWorkUnitRepo(pool), postgres.NewWorkPoolRepo(pool), pub, clock).WithUnitOfWork(uow)
	_, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-rb", PathId: pathId, CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Reference: "order-rb",
	})
	if err == nil || !strings.Contains(err.Error(), "encode exploded") {
		t.Fatalf("expected the encode failure to surface, got %v", err)
	}
	if got := countRows(t, pool, "work_units", "id = 'wu-rb'"); got != 0 {
		t.Fatal("work unit row survived a failed publish: the unit of work did not roll back")
	}
	if got := countRows(t, pool, "work_pools", "path_id = 'pick-b'"); got != 0 {
		t.Fatal("work_pools row survived: WorkPoolRepo.Save committed its own transaction instead of joining")
	}
	if got := countRows(t, pool, "work_pool_entries", "work_unit_id = 'wu-rb'"); got != 0 {
		t.Fatal("work_pool_entries row survived a failed publish")
	}
	if got := countOutbox(t, pool, "true"); got != 0 {
		t.Fatalf("expected an empty outbox, got %d rows", got)
	}

	// The failed scope leaves nothing behind that blocks a retry.
	okIntegration, okAnalytics := encoders(pool)
	okPub := postgres.NewOutboxPublisher(pool, okIntegration, okAnalytics)
	retry := usecases.NewEnqueueWorkUnit(postgres.NewWorkUnitRepo(pool), postgres.NewWorkPoolRepo(pool), okPub, clock).WithUnitOfWork(uow)
	if _, err := retry.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-rb", PathId: pathId, CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Reference: "order-rb",
	}); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
}

// 3. relay publishes in id order across both topics, marks rows, and a
// second pass is a no-op.
func TestOutboxRelay_PublishesInOrderAcrossTopicsAndMarksRows(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	clock := memory.FixedClock{At: time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC)}
	integration, analytics := encoders(pool)
	pub := postgres.NewOutboxPublisher(pool, integration, analytics)
	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	pathId := mustPath(t, "pick-c")

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, pub, clock).WithUnitOfWork(uow)
	releaseUC := usecases.NewReleaseNextWork(pools, workUnits, pub, clock).WithUnitOfWork(uow)
	complete := usecases.NewRecordCompletion(workUnits, pools, pub, clock).WithUnitOfWork(uow)
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{WorkUnitId: "wu-1", PathId: pathId, CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Reference: "r1"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: "wu-1"}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	if n != 6 || len(sink.sent) != 6 {
		t.Fatalf("expected 6 published (3 events x 2 topics), got n=%d sent=%d", n, len(sink.sent))
	}
	want := []struct{ topic, eventType string }{
		{cloudevents.TopicWorkPlanningEvents, "com.warehouse.wes.work-planning.workunit.WorkUnitCreated"}, {outboundkafka.AnalyticsTopic, "com.warehouse.wes.work-planning.workunit.WorkUnitCreated"},
		{cloudevents.TopicWorkPlanningEvents, "com.warehouse.wes.work-planning.workunit.WorkReleased"}, {outboundkafka.AnalyticsTopic, "com.warehouse.wes.work-planning.workunit.WorkReleased"},
		{cloudevents.TopicWorkPlanningEvents, "com.warehouse.wes.work-planning.workunit.WorkUnitCompleted"}, {outboundkafka.AnalyticsTopic, "com.warehouse.wes.work-planning.workunit.WorkUnitCompleted"},
	}
	for i, w := range want {
		if sink.sent[i].Topic != w.topic || sink.sent[i].EventType != w.eventType {
			t.Fatalf("message %d: want %s on %s, got %s on %s", i, w.eventType, w.topic, sink.sent[i].EventType, sink.sent[i].Topic)
		}
	}
	// Both topics are keyed by the aggregate id (ADR-0024).
	if string(sink.sent[1].Key) != "wu-1" || string(sink.sent[0].Key) != "wu-1" {
		t.Fatalf("unexpected keys: integration=%q analytics=%q", sink.sent[0].Key, sink.sent[1].Key)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected every row marked published, %d still pending", got)
	}
	if got := countOutbox(t, pool, "attempts = 1 AND last_error IS NULL"); got != 6 {
		t.Fatalf("expected attempts=1/last_error=NULL on all 6 rows, got %d", got)
	}
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 0 || len(sink.sent) != 6 {
		t.Fatalf("second pass should be a no-op, got n=%d err=%v sent=%d", n, err, len(sink.sent))
	}
}

// 4. relay stops exactly at a failed row, records the attempt, keeps later
// rows pending, and drains them in order once the sink recovers. Headers
// persisted in the outbox round-trip to the sink.
func TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRecoversInOrder(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	clock := memory.FixedClock{At: time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC)}
	// Only the integration encoder here, so the row sequence is one row per
	// event and "the second row" is unambiguous.
	integration, _ := encoders(pool)
	pub := postgres.NewOutboxPublisher(pool, integration)
	uow := postgres.NewUnitOfWork(pool)
	pathId := mustPath(t, "pick-d")

	charges := postgres.NewChargeRepo(pool)
	plans := postgres.NewPlanRepo(pool)
	qty, _ := shared.NewQuantity(5)
	if _, err := usecases.NewReceiveChargeForecast(charges, pub, clock).WithUnitOfWork(uow).Execute(ctx, usecases.ReceiveChargeForecastRequest{
		PathId: pathId, Buckets: []usecases.CPTBucketInput{{CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Quantity: qty}},
	}); err != nil {
		t.Fatalf("charge: %v", err)
	}
	heads, _ := shared.NewStationCount(2)
	installed, _ := shared.NewStationCount(4)
	rate, _ := shared.NewRate(30)
	if _, err := usecases.NewCommitShiftPlan(plans, pub, clock).WithUnitOfWork(uow).Execute(ctx, usecases.CommitShiftPlanRequest{
		PathId: pathId, PlannedHeads: heads, InstalledStations: installed, Rate: rate, Hours: 8,
	}); err != nil {
		t.Fatalf("plan: %v", err)
	}
	if _, err := usecases.NewEnqueueWorkUnit(postgres.NewWorkUnitRepo(pool), postgres.NewWorkPoolRepo(pool), pub, clock).WithUnitOfWork(uow).Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-d", PathId: pathId, CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Reference: "r",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Persist a header on the middle row to prove the JSON round trip.
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET headers = '[{"key":"traceparent","value":"00-abc-def-01"}]' WHERE event_type = 'com.warehouse.wes.work-planning.plan.ShiftPlanCommitted'`); err != nil {
		t.Fatalf("set headers: %v", err)
	}

	sink := &recordingSink{failOn: "com.warehouse.wes.work-planning.plan.ShiftPlanCommitted", failErr: errors.New("broker down")}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err == nil {
		t.Fatal("expected the failing row to surface an error")
	}
	if n != 1 || len(sink.sent) != 1 || sink.sent[0].EventType != "com.warehouse.wes.work-planning.charge.ChargeForecastReceived" {
		t.Fatalf("expected only ChargeForecastReceived published before the failure, got n=%d sent=%v", n, sink.sent)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 2 {
		t.Fatalf("expected ShiftPlanCommitted and WorkUnitCreated still pending (ordering preserved), got %d pending", got)
	}
	var attempts int
	var lastErr string
	if err := pool.QueryRow(ctx, "SELECT attempts, coalesce(last_error,'') FROM outbox_events WHERE event_type = 'com.warehouse.wes.work-planning.plan.ShiftPlanCommitted'").Scan(&attempts, &lastErr); err != nil {
		t.Fatalf("read failed row: %v", err)
	}
	if attempts != 1 || !strings.Contains(lastErr, "broker down") {
		t.Fatalf("expected the failed row to record the attempt, got attempts=%d last_error=%q", attempts, lastErr)
	}
	if got := countOutbox(t, pool, "event_type = 'com.warehouse.wes.work-planning.workunit.WorkUnitCreated' AND attempts = 0"); got != 1 {
		t.Fatal("the row behind the failure must be untouched")
	}

	// Broker recovers: the next pass drains the rest, in order.
	sink.failOn = ""
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 2 {
		t.Fatalf("recovery pass: n=%d err=%v", n, err)
	}
	if sink.sent[1].EventType != "com.warehouse.wes.work-planning.plan.ShiftPlanCommitted" || sink.sent[2].EventType != "com.warehouse.wes.work-planning.workunit.WorkUnitCreated" {
		t.Fatalf("expected ShiftPlanCommitted then WorkUnitCreated after recovery, got %v", sink.sent)
	}
	if len(sink.sent[1].Headers) != 1 || sink.sent[1].Headers[0].Key != "traceparent" || string(sink.sent[1].Headers[0].Value) != "00-abc-def-01" {
		t.Fatalf("headers did not round-trip through the outbox: %+v", sink.sent[1].Headers)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected outbox drained, %d pending", got)
	}
	if err := pool.QueryRow(ctx, "SELECT attempts, coalesce(last_error,'') FROM outbox_events WHERE event_type = 'com.warehouse.wes.work-planning.plan.ShiftPlanCommitted'").Scan(&attempts, &lastErr); err != nil {
		t.Fatalf("re-read row: %v", err)
	}
	if attempts != 2 || lastErr != "" {
		t.Fatalf("expected attempts=2 and last_error cleared after recovery, got attempts=%d last_error=%q", attempts, lastErr)
	}
}

// Run drains continuously and stops on ctx cancellation.
func TestOutboxRelay_RunDrainsAndStopsOnCancel(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	clock := memory.FixedClock{At: time.Now().UTC()}
	integration, analytics := encoders(pool)
	pub := postgres.NewOutboxPublisher(pool, integration, analytics)
	uow := postgres.NewUnitOfWork(pool)
	pathId := mustPath(t, "pick-e")
	enqueue := usecases.NewEnqueueWorkUnit(postgres.NewWorkUnitRepo(pool), postgres.NewWorkPoolRepo(pool), pub, clock).WithUnitOfWork(uow)
	for _, id := range []string{"a", "b", "c"} {
		if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{WorkUnitId: id, PathId: pathId, CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Reference: id}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithInterval(20*time.Millisecond), postgres.WithBatchSize(2))
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay.Run(runCtx) }()

	deadline := time.Now().Add(5 * time.Second)
	for countOutbox(t, pool, "published_at IS NULL") != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected Run to return context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected Run to drain the outbox, %d pending", got)
	}
	if len(sink.sent) != 6 {
		t.Fatalf("expected 6 messages sent, got %d", len(sink.sent))
	}
}
