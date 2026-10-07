//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

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
)

// alwaysFailingProcessedFor wraps a real ports.ProcessedEventRepo so
// TryMarkProcessed fails with a genuine infrastructure error for
// exactly poisonEventID, on EVERY call, while every other event_id is
// delegated unchanged -- letting one poison message coexist in the
// SAME test with a normal, successfully-processed message on the SAME
// partition. This is the FIRST thing ObserveLaborPlan.Execute calls
// (see observe_labor_plan.go), so the injected failure never has a
// side effect to undo, keeping each of the 3 retry attempts cleanly
// idempotent.
type alwaysFailingProcessedFor struct {
	ports.ProcessedEventRepo
	poisonEventID string
}

func (p *alwaysFailingProcessedFor) TryMarkProcessed(ctx context.Context, eventID string, processedAt time.Time) (bool, error) {
	if eventID == p.poisonEventID {
		return false, fmt.Errorf("simulated poison-message infrastructure failure for event %s", eventID)
	}
	return p.ProcessedEventRepo.TryMarkProcessed(ctx, eventID, processedAt)
}

// TestConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition
// is the ADR-0023 §DLQ acceptance test (and the ADR-0027 legacy-flat
// rejection test): a message whose handler ALWAYS
// fails (a simulated infrastructure error from ProcessedEventRepo) must,
// after exactly maxHandlerAttempts (3) in-process retries, land on
// "<topic>.dlq" with the raw original payload plus error context, and
// the consumer must commit past it and keep processing -- a
// well-formed message published right after the poison one must be
// handled without delay, proving the partition was never blocked on
// the one bad message.
func TestConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("wes-dlq-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}

	topic := cloudevents.TopicWorkforceEvents
	dlqTopic := topic + ".dlq"
	if err := createTopics(ctx, brokers, cloudevents.TopicWorkforceEvents, cloudevents.TopicInventoryEvents, cloudevents.TopicFulfillmentEvents, cloudevents.TopicOrderManagementEvents, cloudevents.TopicNetworkDemandEvents, dlqTopic); err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}

	// Build the same stack the fulfillment consumer factory wires,
	// but point the workforce reader at our isolated topic and wrap
	// ProcessedEventRepo so exactly the poison event_id always fails.
	laborViews := memory.NewLaborPlanViewRepo()
	realProcessed := memory.NewProcessedEventRepo()

	poisonEventID := fmt.Sprintf("evt-dlq-poison-%d", time.Now().UnixNano())
	failingProcessed := &alwaysFailingProcessedFor{ProcessedEventRepo: realProcessed, poisonEventID: poisonEventID}

	observeLabor := usecases.NewObserveLaborPlan(laborViews, failingProcessed)
	observeInventory := usecases.NewObserveInventoryChange(memory.NewInventoryViewRepo(), realProcessed)

	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.SystemClock{}
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)
	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, publisher, clock)
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	})

	groupID := fmt.Sprintf("wes-dlq-itest-%d", time.Now().UnixNano())
	consumer := inboundkafka.NewConsumer(brokers, groupID, observeLabor, observeInventory,
		usecases.NewApplyTaskCompleted(recordCompletion, realProcessed),
		usecases.NewApplyOrderAllocated(enqueue, realProcessed, catalogue),
		usecases.NewApplyWorkDemandReleased(enqueue, realProcessed, catalogue),
		catalogue, nil)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	// Start reading the DLQ topic BEFORE publishing, so the poison
	// message's eventual dead-letter write is never missed to a race.
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	pathIdValue := fmt.Sprintf("pick-dlq-poison-%d", time.Now().UnixNano())
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(poisonEventID),
		Headers: cloudEventHeaders(),
		Value: mustCloudEventJSON(t, poisonEventID, cloudevents.TypeShiftPlanCommitted, "workforce-management", map[string]any{
			"building_id": "bldg-1", "shift_id": "shift-poison", "path_id": pathIdValue,
			"planned_heads": 6, "planned_rate": 100.0, "planned_hours": 8.0,
		}),
	}); err != nil {
		t.Fatalf("publish poison ShiftPlanCommitted: %v", err)
	}

	// A retired flat-envelope message right behind it: it is NOT a valid
	// CloudEvent, so it must be dead-lettered immediately (no retries,
	// never parsed as the legacy shape) and the partition must move on.
	legacyKey := fmt.Sprintf("evt-dlq-legacy-%d", time.Now().UnixNano())
	legacyPathIdValue := fmt.Sprintf("pick-dlq-legacy-%d", time.Now().UnixNano())
	legacyValue := []byte(fmt.Sprintf(`{"event_id":%q,"event_type":"ShiftPlanCommitted","occurred_at":"2026-09-30T12:00:00Z","source":"workforce-management","data":{"path_id":%q,"planned_heads":9}}`, legacyKey, legacyPathIdValue))
	if err := writer.WriteMessages(ctx, kafkago.Message{Key: []byte(legacyKey), Value: legacyValue}); err != nil {
		t.Fatalf("publish legacy flat message: %v", err)
	}

	// Assert the poison message lands on the DLQ topic with the raw
	// payload and error context, after the retry budget is exhausted.
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	if string(dlqMsg.Key) != poisonEventID {
		t.Errorf("DLQ message key = %q, want %q (raw key preserved)", string(dlqMsg.Key), poisonEventID)
	}
	assertHeader(t, dlqMsg.Headers, "x-dlq-source-topic", topic)
	if h := headerValue(dlqMsg.Headers, "x-dlq-error"); h == "" {
		t.Error("DLQ message missing x-dlq-error header with failure context")
	}
	if h := headerValue(dlqMsg.Headers, "x-dlq-failed-at"); h == "" {
		t.Error("DLQ message missing x-dlq-failed-at header")
	}

	legacyMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read legacy DLQ message: %v", err)
	}
	if string(legacyMsg.Key) != legacyKey || string(legacyMsg.Value) != string(legacyValue) {
		t.Errorf("legacy DLQ message = %q/%q, want the raw flat message", legacyMsg.Key, legacyMsg.Value)
	}
	if h := headerValue(legacyMsg.Headers, "x-dlq-error"); h == "" {
		t.Error("legacy DLQ message missing x-dlq-error header")
	}

	// Now publish a well-formed message right after the poison one,
	// and confirm it is projected without delay -- proving the
	// partition was not blocked behind the poison message.
	goodPathIdValue := fmt.Sprintf("pick-dlq-healthy-%d", time.Now().UnixNano())
	goodEventID := fmt.Sprintf("evt-dlq-good-%d", time.Now().UnixNano())
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(goodEventID),
		Headers: cloudEventHeaders(),
		Value: mustCloudEventJSON(t, goodEventID, cloudevents.TypeShiftPlanCommitted, "workforce-management", map[string]any{
			"building_id": "bldg-1", "shift_id": "shift-good", "path_id": goodPathIdValue,
			"planned_heads": 4, "planned_rate": 50.0, "planned_hours": 8.0,
		}),
	}); err != nil {
		t.Fatalf("publish well-formed ShiftPlanCommitted: %v", err)
	}

	goodPathId, err := shared.NewPathId(goodPathIdValue)
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		view, err := laborViews.FindByPathId(context.Background(), goodPathId)
		if err == nil {
			if view.PlannedHeads != 4 {
				t.Fatalf("got planned heads %d, want 4", view.PlannedHeads)
			}
			break
		}
		if err != ports.ErrNotFound || time.Now().After(deadline) {
			t.Fatalf("healthy labor plan view never projected (partition blocked?): %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil && ctx.Err() == nil {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}

	// The poison event was never applied to the read model -- the
	// DLQ path commits the offset (so the partition advances) without
	// ever having driven the use case successfully.
	poisonPathId, err := shared.NewPathId(pathIdValue)
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	if _, err := laborViews.FindByPathId(context.Background(), poisonPathId); err != ports.ErrNotFound {
		t.Errorf("poison path %s should not have a projected labor plan view, got err=%v", pathIdValue, err)
	}
	legacyPathId, err := shared.NewPathId(legacyPathIdValue)
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	if _, err := laborViews.FindByPathId(context.Background(), legacyPathId); err != ports.ErrNotFound {
		t.Errorf("legacy flat message must never be parsed/projected, got err=%v", err)
	}
}

func assertHeader(t *testing.T, headers []kafkago.Header, key, want string) {
	t.Helper()
	got := headerValue(headers, key)
	if got != want {
		t.Errorf("header %q = %q, want %q", key, got, want)
	}
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
