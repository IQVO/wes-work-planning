//go:build integration

package kafka_test

import (
	"context"
	"errors"
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
)

// TestConsumer_PoisonMessage_AutoCreatesMissingDeadLetterTopic reproduces
// the 2026-09-30 live stall: a fresh consumer group read a legacy
// non-CloudEvents message at offset 0 of warehouse.order-management.events,
// tried to dead-letter it to warehouse.order-management.events.dlq, and that
// topic did not exist. The DLQ writer did not set AllowAutoTopicCreation, so
// the publish failed with "[3] Unknown Topic Or Partition", Run returned,
// and no OrderAllocated behind it was ever turned into a work unit.
//
// Unlike the sibling DLQ test, this one deliberately creates ONLY the four
// source topics — never a ".dlq" topic — so the DLQ writer must create its
// own topic, exactly as every other writer in the fleet does
// (warehouse-infra/terraform/kafka.tf). It proves:
//  1. the ".dlq" topic is created and holds the raw poison message;
//  2. the consumer keeps running and processes the valid OrderAllocated
//     published right behind the poison message.
func TestConsumer_PoisonMessage_AutoCreatesMissingDeadLetterTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("wes-dlq-autocreate-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}

	topic := cloudevents.TopicOrderManagementEvents
	dlqTopic := topic + ".dlq"
	// Source topics only: the ".dlq" topic must NOT exist up front.
	if err := createTopics(ctx, brokers, cloudevents.TopicWorkforceEvents, cloudevents.TopicInventoryEvents, cloudevents.TopicFulfillmentEvents, topic, cloudevents.TopicNetworkDemandEvents); err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}
	if exists, err := topicExists(ctx, brokers, dlqTopic); err != nil {
		t.Fatalf("list topics: %v", err)
	} else if exists {
		t.Fatalf("precondition: %s must not exist before the poison message is consumed", dlqTopic)
	}

	// Offset 0: a legacy flat-envelope message, as found on the live topic.
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	legacyKey := fmt.Sprintf("legacy-order-%d", time.Now().UnixNano())
	legacyValue := []byte(fmt.Sprintf(`{"event_id":%q,"event_type":"OrderAllocated","occurred_at":"2026-09-30T12:00:00Z","source":"order-management","data":{"order_id":"legacy-order"}}`, legacyKey))
	if err := writer.WriteMessages(ctx, kafkago.Message{Key: []byte(legacyKey), Value: legacyValue}); err != nil {
		t.Fatalf("publish legacy flat message: %v", err)
	}

	// Offset 1: a valid OrderAllocated that must still become a work unit.
	orderId := fmt.Sprintf("dlq-autocreate-order-%d", time.Now().UnixNano())
	sku := fmt.Sprintf("dlq-autocreate-sku-%d", time.Now().UnixNano())
	pathIdValue := fmt.Sprintf("pick-dlq-autocreate-%d", time.Now().UnixNano())
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte("evt-" + orderId),
		Headers: cloudEventHeaders(),
		Value: mustCloudEventJSON(t, "evt-"+orderId, cloudevents.TypeOrderAllocated, "order-management", map[string]any{
			"order_id": orderId, "promise_date": time.Now().Add(3 * time.Hour).UTC().Format(time.RFC3339),
			"lines": []map[string]any{{"line_no": 1, "sku": sku, "path_id": pathIdValue, "gift_wrap": false}},
		}),
	}); err != nil {
		t.Fatalf("publish OrderAllocated: %v", err)
	}

	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.SystemClock{}
	processed := memory.NewProcessedEventRepo()
	enqueueWorkUnit := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)
	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, publisher, clock)
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	})
	consumer := inboundkafka.NewConsumer(brokers,
		fmt.Sprintf("wes-dlq-autocreate-itest-%d", time.Now().UnixNano()),
		usecases.NewObserveLaborPlan(memory.NewLaborPlanViewRepo(), processed),
		usecases.NewObserveInventoryChange(memory.NewInventoryViewRepo(), processed),
		usecases.NewApplyTaskCompleted(recordCompletion, processed),
		usecases.NewApplyOrderAllocated(enqueueWorkUnit, processed, catalogue),
		usecases.NewApplyWorkDemandReleased(enqueueWorkUnit, processed, catalogue),
		catalogue,
		nil)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	// The valid message behind the poison one must be enqueued. If the
	// DLQ publish fails, Run returns and this never happens.
	orderLineWorkUnitId := orderId + "-line-1"
	deadline := time.Now().Add(60 * time.Second)
	for {
		unit, err := workUnits.FindById(context.Background(), orderLineWorkUnitId)
		if err == nil {
			if unit.SKU() != sku {
				t.Fatalf("work unit sku = %q, want %q", unit.SKU(), sku)
			}
			break
		}
		select {
		case err := <-runErr:
			t.Fatalf("consumer stopped before processing the message behind the poison one: %v", err)
		default:
		}
		if !errors.Is(err, ports.ErrNotFound) || time.Now().After(deadline) {
			t.Fatalf("work unit never enqueued from OrderAllocated behind the poison message: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The DLQ topic now exists and holds the raw poison message.
	if exists, err := topicExists(ctx, brokers, dlqTopic); err != nil {
		t.Fatalf("list topics: %v", err)
	} else if !exists {
		t.Fatalf("%s was not created by the DLQ writer", dlqTopic)
	}
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-autocreate-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()
	dlqMsg, err := dlqReader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	if string(dlqMsg.Key) != legacyKey || string(dlqMsg.Value) != string(legacyValue) {
		t.Errorf("DLQ message = %q/%q, want the raw legacy message %q/%q", dlqMsg.Key, dlqMsg.Value, legacyKey, legacyValue)
	}
	assertHeader(t, dlqMsg.Headers, "x-dlq-source-topic", topic)
	if headerValue(dlqMsg.Headers, "x-dlq-error") == "" {
		t.Error("DLQ message missing x-dlq-error header")
	}

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("consumer Run returned an error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}
}

// topicExists reports whether topic is present in the broker's metadata.
func topicExists(ctx context.Context, brokers []string, topic string) (bool, error) {
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return false, fmt.Errorf("dial Kafka: %w", err)
	}
	defer func() { _ = conn.Close() }()
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return false, fmt.Errorf("read partitions: %w", err)
	}
	for _, p := range partitions {
		if p.Topic == topic {
			return true, nil
		}
	}
	return false, nil
}
