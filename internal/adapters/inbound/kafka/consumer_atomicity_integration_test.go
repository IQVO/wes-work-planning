//go:build integration

package kafka_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0028 end to end: the real consumer, a real Kafka broker and a real
// Postgres (both testcontainers), the real postgres UnitOfWork and repos.
// Failures are injected INSIDE the transaction (on the WorkPool Save that
// follows the WorkUnit Save), so these tests prove the processed_events
// insert, the WorkUnit row and the WorkPool rows commit or roll back as one.

// flakyPoolSaves fails WorkPoolRepo.Save for a path: `remaining` times, or
// forever when remaining < 0.
type flakyPoolSaves struct {
	ports.WorkPoolRepo
	mu        sync.Mutex
	remaining map[string]int
}

func (r *flakyPoolSaves) Save(ctx context.Context, pool *release.WorkPool) error {
	r.mu.Lock()
	n, ok := r.remaining[pool.PathId().String()]
	if ok && n != 0 {
		if n > 0 {
			r.remaining[pool.PathId().String()] = n - 1
		}
		r.mu.Unlock()
		return fmt.Errorf("injected transient failure saving pool %s", pool.PathId())
	}
	r.mu.Unlock()
	return r.WorkPoolRepo.Save(ctx, pool)
}

// lockedBuffer is a goroutine-safe log sink (the consumer logs from its
// own goroutines while the test reads).
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startAtomicityPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wes_work_planning"),
		tcpostgres.WithUsername("wes"),
		tcpostgres.WithPassword("wes"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	migrations, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "migrations"))
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}
	if err := postgres.Migrate(dsn, migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := postgres.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func processedRowExists(t *testing.T, pool *pgxpool.Pool, eventId string) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM processed_events WHERE event_id = $1`, eventId).Scan(&n); err != nil {
		t.Fatalf("query processed_events: %v", err)
	}
	return n == 1
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestConsumer_ProcessedMarkCommitsAtomicallyWithHandling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	pool := startAtomicityPostgres(t, ctx)

	kafkaContainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("wes-atomic-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, kafkaContainer)
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	fulfillmentDLQ := cloudevents.TopicFulfillmentEvents + ".dlq"
	orderDLQ := cloudevents.TopicOrderManagementEvents + ".dlq"
	if err := createTopics(ctx, brokers, cloudevents.TopicWorkforceEvents, cloudevents.TopicInventoryEvents,
		cloudevents.TopicFulfillmentEvents, cloudevents.TopicOrderManagementEvents, cloudevents.TopicNetworkDemandEvents, fulfillmentDLQ, orderDLQ); err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}

	// Real Postgres-backed stack, wired exactly like cmd/wes.
	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	realPools := postgres.NewWorkPoolRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	publisher := events.NewLogPublisher(nil)
	clock := memory.SystemClock{}

	healPath := "pick-atomic-heal"
	stuckPath := "pick-atomic-stuck"
	orderPath := "pick-atomic-order"
	pools := &flakyPoolSaves{WorkPoolRepo: realPools, remaining: map[string]int{}}

	// Seed: one released work unit on each completion path, via the real
	// use cases (before any failure is armed).
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock).WithUnitOfWork(uow)
	releaseNext := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock).WithUnitOfWork(uow)
	for _, seed := range []struct{ wu, path string }{{"wu-atomic-heal", healPath}, {"wu-atomic-stuck", stuckPath}} {
		pathId, _ := shared.NewPathId(seed.path)
		if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
			WorkUnitId: seed.wu, PathId: pathId, CPT: shared.NewCPT(time.Now().Add(2 * time.Hour)), Reference: "order-seed",
		}); err != nil {
			t.Fatalf("seed enqueue %s: %v", seed.wu, err)
		}
		if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
			t.Fatalf("seed release %s: %v", seed.wu, err)
		}
	}

	// Arm the failures: the heal path fails its first in-tx pool Save,
	// the stuck path fails every one, the order path fails its first.
	pools.mu.Lock()
	pools.remaining[healPath] = 1
	pools.remaining[stuckPath] = -1
	pools.remaining[orderPath] = 1
	pools.mu.Unlock()

	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}}})
	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, publisher, clock).WithUnitOfWork(uow)
	applyTaskCompleted := usecases.NewApplyTaskCompleted(recordCompletion, processed).WithUnitOfWork(uow)
	applyOrderAllocated := usecases.NewApplyOrderAllocated(enqueue, processed, catalogue).WithUnitOfWork(uow)
	applyWorkDemandReleased := usecases.NewApplyWorkDemandReleased(enqueue, processed, catalogue).WithUnitOfWork(uow)
	observeLabor := usecases.NewObserveLaborPlan(postgres.NewLaborPlanViewRepo(pool), processed).WithUnitOfWork(uow)
	observeInventory := usecases.NewObserveInventoryChange(postgres.NewInventoryViewRepo(pool), processed).WithUnitOfWork(uow)

	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	groupID := fmt.Sprintf("wes-atomic-itest-%d", time.Now().UnixNano())
	consumer := inboundkafka.NewConsumer(brokers, groupID, observeLabor, observeInventory, applyTaskCompleted, applyOrderAllocated, applyWorkDemandReleased, catalogue, logger)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	go func() { _ = consumer.Run(consumeCtx) }()

	fulfillment := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicFulfillmentEvents}
	defer fulfillment.Close()
	orders := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicOrderManagementEvents}
	defer orders.Close()

	suffix := time.Now().UnixNano()
	healEvt := fmt.Sprintf("evt-tc-heal-%d", suffix)
	stuckEvt := fmt.Sprintf("evt-tc-stuck-%d", suffix)
	packEvt := fmt.Sprintf("evt-tc-pack-%d", suffix)
	orderEvt := fmt.Sprintf("evt-oa-heal-%d", suffix)
	orderId := fmt.Sprintf("order-atomic-%d", suffix)

	taskCompleted := func(eventId, workUnitId, taskType string) kafkago.Message {
		return kafkago.Message{Key: []byte(eventId), Headers: cloudEventHeaders(),
			Value: mustCloudEventJSON(t, eventId, cloudevents.TypeTaskCompleted, "fulfillment-execution", map[string]any{
				"task_id": "task-" + eventId, "station_id": "station-1", "work_unit_id": workUnitId, "task_type": taskType,
			})}
	}
	if err := fulfillment.WriteMessages(ctx,
		taskCompleted(healEvt, "wu-atomic-heal", "PICK"),
		taskCompleted(stuckEvt, "wu-atomic-stuck", "PICK"),
		taskCompleted(packEvt, orderId, "PACK"),
	); err != nil {
		t.Fatalf("publish TaskCompleted: %v", err)
	}
	if err := orders.WriteMessages(ctx, kafkago.Message{Key: []byte(orderEvt), Headers: cloudEventHeaders(),
		Value: mustCloudEventJSON(t, orderEvt, cloudevents.TypeOrderAllocated, "order-management", map[string]any{
			"order_id": orderId, "promise_date": time.Now().Add(3 * time.Hour).UTC().Format(time.RFC3339),
			"lines": []map[string]any{
				{"line_no": 1, "sku": "SKU-A", "path_id": orderPath, "gift_wrap": false},
				{"line_no": 2, "sku": "SKU-B", "path_id": orderPath, "gift_wrap": true},
			},
		})}); err != nil {
		t.Fatalf("publish OrderAllocated: %v", err)
	}

	// (b) The persistent failure lands on the DLQ — and, the crux of the
	// fix, its processed_events row was NEVER committed.
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: fulfillmentDLQ,
		GroupID: fmt.Sprintf("dlq-reader-%d", suffix), StartOffset: kafkago.FirstOffset})
	defer dlqReader.Close()
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ: %v", err)
	}
	if string(dlqMsg.Key) != stuckEvt {
		t.Fatalf("DLQ key = %q, want %q (only the persistent failure may be dead-lettered)", dlqMsg.Key, stuckEvt)
	}

	// (a) The transient failure healed on retry: the WorkUnit is Completed,
	// its WIP slot freed, and only now is the event marked processed.
	eventually(t, "heal work unit Completed", func() bool {
		u, err := workUnits.FindById(ctx, "wu-atomic-heal")
		return err == nil && u.State() == workunit.Completed
	})
	healPathId, _ := shared.NewPathId(healPath)
	healPool, err := realPools.FindByPathId(ctx, healPathId)
	if err != nil {
		t.Fatalf("FindByPathId heal: %v", err)
	}
	if healPool.WIP() != 0 {
		t.Fatalf("heal pool WIP = %d, want 0 (the WIP slot was never freed)", healPool.WIP())
	}
	if !processedRowExists(t, pool, healEvt) {
		t.Fatal("healed event must be marked processed once it is applied")
	}

	// (d) OrderAllocated: the first in-tx failure rolled back BOTH the mark
	// and line 1's WorkUnit row; the retry enqueued every line.
	eventually(t, "order lines enqueued", func() bool {
		_, err1 := workUnits.FindById(ctx, orderId+"-line-1")
		_, err2 := workUnits.FindById(ctx, orderId+"-line-2")
		return err1 == nil && err2 == nil
	})
	eventually(t, "order event marked processed", func() bool { return processedRowExists(t, pool, orderEvt) })

	// (c) The PACK completion for an order id WES never planned is an INFO
	// skip: marked processed, never dead-lettered.
	eventually(t, "unknown-work-unit skip marked processed", func() bool { return processedRowExists(t, pool, packEvt) })
	if !strings.Contains(logs.String(), "never planned") || !strings.Contains(logs.String(), "task_type=PACK") {
		t.Fatalf("expected an INFO skip log for the PACK completion, got:\n%s", logs.String())
	}

	if processedRowExists(t, pool, stuckEvt) {
		t.Fatal("a dead-lettered event's processed mark was committed — a later replay would be silently swallowed")
	}
	stuck, err := workUnits.FindById(ctx, "wu-atomic-stuck")
	if err != nil {
		t.Fatalf("FindById stuck: %v", err)
	}
	if stuck.State() != workunit.Released {
		t.Fatalf("stuck work unit = %v, want Released (rolled back)", stuck.State())
	}

	// Nothing else reached either DLQ.
	drainCtx, drainCancel := context.WithTimeout(ctx, 3*time.Second)
	defer drainCancel()
	if extra, err := dlqReader.ReadMessage(drainCtx); err == nil {
		t.Fatalf("unexpected extra fulfillment DLQ message %q", extra.Key)
	}
	orderDLQReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: orderDLQ,
		GroupID: fmt.Sprintf("order-dlq-reader-%d", suffix), StartOffset: kafkago.FirstOffset})
	defer orderDLQReader.Close()
	orderDrainCtx, orderDrainCancel := context.WithTimeout(ctx, 3*time.Second)
	defer orderDrainCancel()
	if extra, err := orderDLQReader.ReadMessage(orderDrainCtx); err == nil {
		t.Fatalf("a healed OrderAllocated must not be dead-lettered, got %q", extra.Key)
	}
}
