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
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// feRelocation mirrors fulfillment-execution's WorkReleasedRelocation
// decoder (its ADR 0047) field for field.
type feRelocation struct {
	PlanId  string `json:"plan_id"`
	MoveSeq *int   `json:"move_seq"`
	Kind    string `json:"kind"`
	SKU     string `json:"sku"`
	FromBin string `json:"from_bin,omitempty"`
	ToBin   string `json:"to_bin,omitempty"`
}

// TestConsumer_SlotPlanApproved_ReleasesRelocateWorkInTheFulfillmentShape
// is ADR-0040's end-to-end acceptance test on a real Kafka broker and a real
// Postgres (testcontainers), wired like cmd/wes:
//
//  1. a SlotPlanApproved on warehouse.slotting-optimization.events plans one
//     RELOCATE unit per move; the move into the vacated slot is blocked;
//  2. releasing the RELOCATE path outboxes WorkReleased whose data.relocation
//     decodes exactly as fulfillment-execution's RELOCATE task expects;
//  3. a TaskCompleted for the vacate (warehouse.fulfillment.events) admits the
//     blocked move, which then releases with its own relocation block;
//  4. a replay of the plan's CloudEvents id is a no-op.
func TestConsumer_SlotPlanApproved_ReleasesRelocateWorkInTheFulfillmentShape(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	pool := startAtomicityPostgres(t, ctx)
	kafkaContainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("wes-slot-itest-%d", time.Now().UnixNano())))
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
		cloudevents.TopicSlottingEvents, cloudevents.TopicWorkPlanningEvents); err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}

	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	clock := memory.SystemClock{}
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "RELOCATE", MatchPrefix: "relocate", RequiredCapabilities: []string{"relocate"}, Intake: []string{pathcatalog.IntakeSlotRelocation}},
	})
	integrationPublisher := outboundkafka.NewPublisherWithWriter(&kafkago.Writer{
		Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicWorkPlanningEvents, Balancer: &kafkago.Hash{},
		BatchTimeout: 10 * time.Millisecond, RequiredAcks: kafkago.RequireAll,
	}, workUnits, nil, func() string { return fmt.Sprintf("evt-out-%d", time.Now().UnixNano()) })
	publisher := postgres.NewOutboxPublisher(pool, integrationPublisher)

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock).WithUnitOfWork(uow)
	releaseNext := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock).WithUnitOfWork(uow)
	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, publisher, clock).WithUnitOfWork(uow).WithRelocationWork(workUnits)
	applySlotPlan := usecases.NewApplySlotPlanApproved(workUnits, workUnits, pools, postgres.NewSlotPlanRepo(pool), processed, catalogue, publisher, clock).WithUnitOfWork(uow)
	consumer := inboundkafka.NewConsumer(brokers, fmt.Sprintf("wes-slot-itest-%d", time.Now().UnixNano()),
		usecases.NewObserveLaborPlan(postgres.NewLaborPlanViewRepo(pool), processed).WithUnitOfWork(uow),
		usecases.NewObserveInventoryChange(postgres.NewInventoryViewRepo(pool), processed).WithUnitOfWork(uow),
		usecases.NewApplyTaskCompleted(recordCompletion, processed).WithUnitOfWork(uow),
		usecases.NewApplyOrderAllocated(enqueue, processed, catalogue).WithUnitOfWork(uow),
		usecases.NewApplyWorkDemandReleased(enqueue, processed, catalogue).WithUnitOfWork(uow),
		catalogue, nil).WithSlotPlans(applySlotPlan)
	defer func() { _ = consumer.Close() }()
	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	go func() { _ = consumer.Run(consumeCtx) }()

	planId := fmt.Sprintf("plan-itest-%d", time.Now().UnixNano())
	planEventId := "evt-" + planId
	publishSlotPlan := func() {
		t.Helper()
		writeEvent(t, ctx, brokers, cloudevents.TopicSlottingEvents, planId, mustCloudEventJSON(t, planEventId, cloudevents.TypeSlotPlanApproved, "slotting-optimization", map[string]any{
			"plan_id": planId, "site_id": "SITE-ITEST", "approved_at": time.Now().UTC().Format(time.RFC3339),
			"assignments": []map[string]any{{"sku": "SKU-2", "slot": "S3"}},
			"moves": []map[string]any{
				{"sku": "SKU-2", "from_slot": "S2", "to_slot": "S3", "kind": "Relocate"},
				{"sku": "SKU-3", "from_slot": "S3", "kind": "Vacate"},
			},
		}))
	}
	publishSlotPlan()

	// (1) Both units planned; move 0 (into S3) waits on move 1 (vacates S3).
	moveInto, vacate := planId+"-move-0", planId+"-move-1"
	eventually(t, "RELOCATE units planned", func() bool {
		u, err := workUnits.FindById(ctx, vacate)
		return err == nil && u.WorkKind() == workunit.WorkKindRelocate
	})
	if u, _ := workUnits.FindById(ctx, moveInto); u == nil || u.BlockedBy() != vacate {
		t.Fatalf("the move into the vacated slot must wait on %s, got %+v", vacate, u)
	}

	// (2) Only the vacate is releasable; its WorkReleased has the FE shape.
	relocatePath, _ := shared.NewPathId(usecases.RelocatePathId)
	released, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: relocatePath})
	if err != nil || released.Id() != vacate {
		t.Fatalf("first release = (%v, %v), want %s", released, err, vacate)
	}
	assertFulfillmentShape(t, outboxedWorkReleased(t, ctx, pool, vacate), planId, 1, "Vacate", "SKU-3", "S3", "")

	// (3) fulfillment-execution completes the vacate: the blocked move is
	// admitted and releases with its own relocation block.
	writeEvent(t, ctx, brokers, cloudevents.TopicFulfillmentEvents, "task-vacate", mustCloudEventJSON(t, "evt-tc-"+planId, cloudevents.TypeTaskCompleted, "fulfillment-execution", map[string]any{
		"task_id": "task-vacate", "station_id": "relocate-1", "work_unit_id": vacate, "task_type": "RELOCATE",
		"sku": "SKU-3", "from_bin": "S3", "to_bin": "R-17", "quantity": 4, "relocation_ref": planId + ":1",
	}))
	eventually(t, "blocked move admitted after the vacate completed", func() bool {
		u, err := workUnits.FindById(ctx, moveInto)
		return err == nil && !u.IsBlocked()
	})
	released, err = releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: relocatePath})
	if err != nil || released.Id() != moveInto {
		t.Fatalf("second release = (%v, %v), want %s", released, err, moveInto)
	}
	assertFulfillmentShape(t, outboxedWorkReleased(t, ctx, pool, moveInto), planId, 0, "Relocate", "SKU-2", "S2", "S3")

	// (4) Replaying the plan's CloudEvents id plans nothing new.
	publishSlotPlan()
	time.Sleep(3 * time.Second)
	if got := countWorkUnits(t, pool, moveInto); got != 1 {
		t.Fatalf("replay duplicated the unit: %d rows", got)
	}
	if u, _ := workUnits.FindById(ctx, moveInto); u.State() != workunit.Released {
		t.Fatalf("replay touched the released unit: %v", u.State())
	}
}

func writeEvent(t *testing.T, ctx context.Context, brokers []string, topic, key string, value []byte) {
	t.Helper()
	w := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond}
	defer func() { _ = w.Close() }()
	if err := w.WriteMessages(ctx, kafkago.Message{Key: []byte(key), Headers: cloudEventHeaders(), Value: value}); err != nil {
		t.Fatalf("publish to %s: %v", topic, err)
	}
}

// outboxedWorkReleased returns the data object of the latest WorkReleased
// the outbox holds, checking it is workUnitId's (releases are sequential).
func outboxedWorkReleased(t *testing.T, ctx context.Context, pool *pgxpool.Pool, workUnitId string) json.RawMessage {
	t.Helper()
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	eventually(t, "WorkReleased outboxed for "+workUnitId, func() bool {
		var payload []byte
		row := pool.QueryRow(ctx, `SELECT value FROM outbox_events WHERE event_type LIKE '%WorkReleased%' ORDER BY id DESC LIMIT 1`)
		if row.Scan(&payload) != nil || json.Unmarshal(payload, &envelope) != nil {
			return false
		}
		var data struct {
			WorkUnitId string `json:"work_unit_id"`
		}
		return json.Unmarshal(envelope.Data, &data) == nil && data.WorkUnitId == workUnitId
	})
	return envelope.Data
}

func assertFulfillmentShape(t *testing.T, data json.RawMessage, planId string, seq int, kind, sku, from, to string) {
	t.Helper()
	var got struct {
		PathId      string        `json:"path_id"`
		Ref         string        `json:"ref"`
		WorkKind    string        `json:"work_kind,omitempty"`
		TransferRef string        `json:"transfer_ref,omitempty"`
		Relocation  *feRelocation `json:"relocation"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode WorkReleased data as fulfillment-execution: %v", err)
	}
	r := got.Relocation
	if got.PathId != "RELOCATE" || got.Ref != planId || got.WorkKind != "" || got.TransferRef != "" {
		t.Fatalf("WorkReleased data = %s", data)
	}
	if r == nil || r.MoveSeq == nil || r.PlanId != planId || *r.MoveSeq != seq || r.Kind != kind || r.SKU != sku || r.FromBin != from || r.ToBin != to {
		t.Fatalf("relocation = %s", data)
	}
}
