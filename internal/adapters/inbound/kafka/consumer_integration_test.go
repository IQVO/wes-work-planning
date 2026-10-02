//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// TestConsumer_ProjectsRealBrokerMessages runs the real consumer against an
// isolated Kafka broker. CI has no external Kafka service, so the test must
// execute through Testcontainers rather than silently skip.
func TestConsumer_ProjectsRealBrokerMessages(t *testing.T) {
	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("wes-inbound-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	if err := createTopics(ctx, brokers, cloudevents.TopicWorkforceEvents, cloudevents.TopicInventoryEvents, cloudevents.TopicFulfillmentEvents, cloudevents.TopicOrderManagementEvents); err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}

	pathIdValue := fmt.Sprintf("pick-integration-kafka-%d", time.Now().UnixNano())
	sku := fmt.Sprintf("integration-kafka-sku-%d", time.Now().UnixNano())
	workUnitIdValue := fmt.Sprintf("integration-kafka-wu-%d", time.Now().UnixNano())

	// Put the work unit into Released state via the existing use cases
	// before the TaskCompleted event arrives, so the consumer's call into
	// RecordCompletion has something valid to transition.
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.SystemClock{}
	pathIdForUnit, err := shared.NewPathId(pathIdValue + "-wu")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)
	releaseNext := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock)
	if _, err := enqueue.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: workUnitIdValue, PathId: pathIdForUnit, CPT: shared.NewCPT(time.Now().Add(2 * time.Hour)), Reference: "ref-1",
	}); err != nil {
		t.Fatalf("EnqueueWorkUnit: %v", err)
	}
	if _, err := releaseNext.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathIdForUnit}); err != nil {
		t.Fatalf("ReleaseNextWork: %v", err)
	}

	publishCtx, publishCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer publishCancel()

	workforceWriter := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicWorkforceEvents, AllowAutoTopicCreation: false}
	defer workforceWriter.Close()
	if err := workforceWriter.WriteMessages(publishCtx, kafkago.Message{
		Key:     []byte("evt-shift-" + pathIdValue),
		Headers: cloudEventHeaders(),
		Value: mustCloudEventJSON(t, "evt-shift-"+pathIdValue, cloudevents.TypeShiftPlanCommitted, "workforce-management", map[string]any{
			"building_id": "bldg-1", "shift_id": "shift-1", "path_id": pathIdValue,
			"planned_heads": 6, "planned_rate": 100.0, "planned_hours": 8.0,
		}),
	}); err != nil {
		t.Fatalf("publish ShiftPlanCommitted: %v", err)
	}

	inventoryWriter := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicInventoryEvents, AllowAutoTopicCreation: false}
	defer inventoryWriter.Close()
	if err := inventoryWriter.WriteMessages(publishCtx, kafkago.Message{
		Key:     []byte("evt-reserve-" + sku),
		Headers: cloudEventHeaders(),
		Value: mustCloudEventJSON(t, "evt-reserve-"+sku, cloudevents.TypeStockReserved, "inventory-storage", map[string]any{
			"sku": sku, "quantity": 5, "demand_ref": "demand-1",
		}),
	}); err != nil {
		t.Fatalf("publish StockReserved: %v", err)
	}

	fulfillmentWriter := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicFulfillmentEvents, AllowAutoTopicCreation: false}
	defer fulfillmentWriter.Close()
	if err := fulfillmentWriter.WriteMessages(publishCtx, kafkago.Message{
		Key:     []byte("evt-task-" + workUnitIdValue),
		Headers: cloudEventHeaders(),
		Value: mustCloudEventJSON(t, "evt-task-"+workUnitIdValue, cloudevents.TypeTaskCompleted, "fulfillment-execution", map[string]any{
			"task_id": "task-1", "station_id": "station-1", "work_unit_id": workUnitIdValue,
		}),
	}); err != nil {
		t.Fatalf("publish TaskCompleted: %v", err)
	}

	orderId := fmt.Sprintf("integration-kafka-order-%d", time.Now().UnixNano())
	orderManagementWriter := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicOrderManagementEvents, AllowAutoTopicCreation: false}
	defer orderManagementWriter.Close()
	if err := orderManagementWriter.WriteMessages(publishCtx, kafkago.Message{
		Key:     []byte("evt-order-" + orderId),
		Headers: cloudEventHeaders(),
		Value: mustCloudEventJSON(t, "evt-order-"+orderId, cloudevents.TypeOrderAllocated, "order-management", map[string]any{
			"order_id": orderId, "promise_date": time.Now().Add(3 * time.Hour).UTC().Format(time.RFC3339),
			"lines": []map[string]any{{"line_no": 1, "sku": sku, "path_id": pathIdValue, "gift_wrap": false}},
		}),
	}); err != nil {
		t.Fatalf("publish OrderAllocated: %v", err)
	}

	laborViews := memory.NewLaborPlanViewRepo()
	inventoryViews := memory.NewInventoryViewRepo()
	processed := memory.NewProcessedEventRepo()
	observeLabor := usecases.NewObserveLaborPlan(laborViews, processed)
	observeInventory := usecases.NewObserveInventoryChange(inventoryViews, processed)
	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, publisher, clock)

	groupID := fmt.Sprintf("wes-integration-test-%d", time.Now().UnixNano())
	// The catalogue only needs a "pick" family declared: every path_id
	// this test publishes is pick-prefixed (see pathIdValue above).
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	})
	consumer := inboundkafka.NewConsumer(brokers, groupID, observeLabor, observeInventory,
		usecases.NewApplyTaskCompleted(recordCompletion, processed),
		usecases.NewApplyOrderAllocated(enqueue, processed, catalogue),
		catalogue, nil)
	defer consumer.Close()

	consumeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = consumer.Run(consumeCtx) }()

	pathId, err := shared.NewPathId(pathIdValue)
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		view, err := laborViews.FindByPathId(context.Background(), pathId)
		if err == nil {
			if view.PlannedHeads != 6 {
				t.Fatalf("got planned heads %d, want 6", view.PlannedHeads)
			}
			break
		}
		if err != ports.ErrNotFound || time.Now().After(deadline) {
			t.Fatalf("labor plan view never projected: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	deadline = time.Now().Add(30 * time.Second)
	for {
		inv, err := inventoryViews.FindBySKU(context.Background(), sku)
		if err == nil {
			if inv.UsableQuantity != -5 {
				t.Fatalf("got usable quantity %d, want -5", inv.UsableQuantity)
			}
			break
		}
		if err != ports.ErrNotFound || time.Now().After(deadline) {
			t.Fatalf("inventory view never projected: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	deadline = time.Now().Add(30 * time.Second)
	for {
		unit, err := workUnits.FindById(context.Background(), workUnitIdValue)
		if err != nil {
			t.Fatalf("FindById: %v", err)
		}
		if unit.State() == workunit.Completed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("work unit never completed from TaskCompleted event, state=%v", unit.State())
		}
		time.Sleep(200 * time.Millisecond)
	}

	orderLineWorkUnitId := fmt.Sprintf("%s-line-1", orderId)
	deadline = time.Now().Add(30 * time.Second)
	for {
		unit, err := workUnits.FindById(context.Background(), orderLineWorkUnitId)
		if err == nil {
			if unit.SKU() != sku {
				t.Fatalf("got sku %q, want %q", unit.SKU(), sku)
			}
			break
		}
		if err != ports.ErrNotFound || time.Now().After(deadline) {
			t.Fatalf("work unit never enqueued from OrderAllocated event: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// mustCloudEventJSON builds the structured-mode CloudEvents 1.0 wire bytes
// the upstream service (repo name `source`) publishes for ceType.
func mustCloudEventJSON(t *testing.T, eventId, ceType, source string, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(eventId)
	e.SetType(ceType)
	e.SetSource("/warehouse/" + source)
	e.SetSubject(eventId)
	e.SetTime(time.Now().UTC())
	if err := e.SetData(ce.ApplicationJSON, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	body, err := e.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal CloudEvent: %v", err)
	}
	return body
}

// cloudEventHeaders is the content-type header every producer in the fleet
// stamps on a CloudEvents message.
func cloudEventHeaders() []kafkago.Header {
	return []kafkago.Header{cloudevents.ContentTypeHeader()}
}

func createTopics(ctx context.Context, brokers []string, topics ...string) error {
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial Kafka: %w", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("Kafka controller: %w", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial Kafka controller: %w", err)
	}
	defer controllerConn.Close()
	configs := make([]kafkago.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	if err := controllerConn.CreateTopics(configs...); err != nil {
		return fmt.Errorf("create Kafka topics: %w", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for _, topic := range topics {
		for {
			partitions, err := conn.ReadPartitions(topic)
			if err == nil && len(partitions) > 0 {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("Kafka topic %q leader was not ready: %w", topic, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}
