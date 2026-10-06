//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// TestConsumer_WorkDemandReleased_CreatesTransferWorkUnitAndPublishesWorkReleased
// is the ADR-0033 end-to-end acceptance test: a real Kafka broker and a real
// Postgres (both testcontainers), the real postgres UnitOfWork/repos and the
// real outbox publisher stack wired exactly like cmd/wes.
//
// A WorkDemandReleased event on warehouse.network-inventory-planning.events
// must (1) create exactly one work unit with id == demand_id carrying the
// full transfer metadata, (2) release it via ReleaseNextWork and see the
// outboxed WorkReleased CloudEvent carry the transfer fields, and (3) treat
// a replay of the same event (same CloudEvents id) as a no-op — no second
// unit, no second WorkReleased.
func TestConsumer_WorkDemandReleased_CreatesTransferWorkUnitAndPublishesWorkReleased(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	pool := startAtomicityPostgres(t, ctx)

	kafkaContainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("wes-demand-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, kafkaContainer)
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	if err := createTopics(ctx, brokers, cloudevents.TopicWorkforceEvents, cloudevents.TopicInventoryEvents,
		cloudevents.TopicFulfillmentEvents, cloudevents.TopicOrderManagementEvents, cloudevents.TopicNetworkDemandEvents,
		cloudevents.TopicWorkPlanningEvents); err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}

	// Real Postgres-backed stack, wired exactly like cmd/wes (outbox
	// publisher over the real integration encoder, which reads the work
	// unit inside the same transaction).
	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	clock := memory.SystemClock{}
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	})

	integrationPublisher := outboundkafka.NewPublisherWithWriter(&kafkago.Writer{
		Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicWorkPlanningEvents,
		AllowAutoTopicCreation: false, Balancer: &kafkago.Hash{}, BatchTimeout: 10 * time.Millisecond,
	}, workUnits, nil, func() string { return fmt.Sprintf("evt-out-%d", time.Now().UnixNano()) })
	publisher := postgres.NewOutboxPublisher(pool, integrationPublisher)

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock).WithUnitOfWork(uow)
	releaseNext := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock).WithUnitOfWork(uow)
	applyWorkDemand := usecases.NewApplyWorkDemandReleased(enqueue, processed, catalogue).WithUnitOfWork(uow)
	observeLabor := usecases.NewObserveLaborPlan(postgres.NewLaborPlanViewRepo(pool), processed).WithUnitOfWork(uow)
	observeInventory := usecases.NewObserveInventoryChange(postgres.NewInventoryViewRepo(pool), processed).WithUnitOfWork(uow)
	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, publisher, clock).WithUnitOfWork(uow)
	applyTaskCompleted := usecases.NewApplyTaskCompleted(recordCompletion, processed).WithUnitOfWork(uow)
	applyOrderAllocated := usecases.NewApplyOrderAllocated(enqueue, processed, catalogue).WithUnitOfWork(uow)

	groupID := fmt.Sprintf("wes-demand-itest-%d", time.Now().UnixNano())
	consumer := inboundkafka.NewConsumer(brokers, groupID, observeLabor, observeInventory,
		applyTaskCompleted, applyOrderAllocated, applyWorkDemand, catalogue, nil)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	go func() { _ = consumer.Run(consumeCtx) }()

	suffix := time.Now().UnixNano()
	demandId := fmt.Sprintf("demand-itest-%d", suffix)
	eventId := fmt.Sprintf("evt-demand-itest-%d", suffix)
	pathIdValue := fmt.Sprintf("pick-demand-%d", suffix)

	demandWriter := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicNetworkDemandEvents, AllowAutoTopicCreation: false}
	defer demandWriter.Close()
	publish := func(id string) {
		t.Helper()
		if err := demandWriter.WriteMessages(ctx, kafkago.Message{
			Key:     []byte(id),
			Headers: cloudEventHeaders(),
			Value: mustCloudEventJSON(t, id, cloudevents.TypeWorkDemandReleased, "network-inventory-planning", map[string]any{
				"demand_id":    demandId,
				"work_kind":    "TRANSFER_PICK",
				"transfer_ref": "TRF-ITEST-042",
				"path_id":      pathIdValue,
				"site_id":      "site-north-1",
				"cpt":          time.Now().Add(6 * time.Hour).UTC().Format(time.RFC3339),
				"sku":          "SKU-ITEST-T1",
				"quantity":     17,
			}),
		}); err != nil {
			t.Fatalf("publish WorkDemandReleased: %v", err)
		}
	}
	publish(eventId)

	// (1) The work unit exists with the full transfer metadata.
	pathId, err := shared.NewPathId(pathIdValue)
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	eventually(t, "transfer work unit created", func() bool {
		unit, err := workUnits.FindById(ctx, demandId)
		return err == nil && unit != nil && unit.WorkKind().String() == "TRANSFER_PICK"
	})
	unit, err := workUnits.FindById(ctx, demandId)
	if err != nil {
		t.Fatalf("FindById %s: %v", demandId, err)
	}
	if unit.Reference() != demandId {
		t.Errorf("Reference = %q, want demand_id %q", unit.Reference(), demandId)
	}
	if unit.TransferRef() != "TRF-ITEST-042" || unit.SiteId() != "site-north-1" || unit.Quantity() != 17 || unit.SKU() != "SKU-ITEST-T1" {
		t.Errorf("transfer metadata incomplete: ref=%q site=%q qty=%d sku=%q",
			unit.TransferRef(), unit.SiteId(), unit.Quantity(), unit.SKU())
	}

	// (2) Release it and read the outboxed WorkReleased payload.
	if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("ReleaseNextWork: %v", err)
	}
	var payload []byte
	eventually(t, "WorkReleased outboxed", func() bool {
		row := pool.QueryRow(ctx, `SELECT value FROM outbox_events WHERE event_type LIKE '%WorkReleased%' ORDER BY id DESC LIMIT 1`)
		return row.Scan(&payload) == nil && len(payload) > 0
	})
	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("unmarshal outboxed WorkReleased: %v", err)
	}
	if envelope.Data["work_unit_id"] != demandId {
		t.Errorf("work_unit_id = %v, want %q", envelope.Data["work_unit_id"], demandId)
	}
	if envelope.Data["transfer_ref"] != "TRF-ITEST-042" {
		t.Errorf("transfer_ref = %v", envelope.Data["transfer_ref"])
	}
	if envelope.Data["work_kind"] != "TRANSFER_PICK" {
		t.Errorf("work_kind = %v", envelope.Data["work_kind"])
	}
	if envelope.Data["site_id"] != "site-north-1" {
		t.Errorf("site_id = %v", envelope.Data["site_id"])
	}
	if envelope.Data["quantity"] != float64(17) {
		t.Errorf("quantity = %v", envelope.Data["quantity"])
	}

	// (3) Replay the same CloudEvents id: a benign no-op — no second
	// unit, no second WorkReleased, no error.
	unitsBefore := countWorkUnits(t, pool, demandId)
	publish(eventId)
	time.Sleep(3 * time.Second) // allow the redelivery to be consumed
	if got := countWorkUnits(t, pool, demandId); got != unitsBefore {
		t.Fatalf("replay created a duplicate: %d units before, %d after", unitsBefore, got)
	}
}

func countWorkUnits(t *testing.T, pool *pgxpool.Pool, id string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM work_units WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count work units: %v", err)
	}
	return n
}
