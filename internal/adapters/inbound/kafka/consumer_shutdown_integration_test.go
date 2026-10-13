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
)

// blockingProcessed parks every TryMarkProcessed call (the first thing
// ObserveLaborPlan.Execute does) until the handler's context is cancelled,
// then reports that cancellation -- exactly what a real Postgres-backed repo
// does when the pod is told to stop mid-transaction.
type blockingProcessed struct {
	ports.ProcessedEventRepo
	entered chan struct{}
}

func (p *blockingProcessed) TryMarkProcessed(ctx context.Context, _ string, _ time.Time) (bool, error) {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return false, ctx.Err()
}

// TestConsumer_ShutdownMidHandle_IsCleanAndDeadLettersNothing pins the
// shutdown contract: cancelling Run's context while a healthy message is
// being handled is a clean stop (Run returns nil), and that message is NOT
// dead-lettered -- its offset stays uncommitted, so it is redelivered on the
// next start and the use case's processed-events claim absorbs the replay.
// Before the fix the retry wrapper's cancellation error was routed into the
// dead-letter path as if the message were poison, and the DLQ publish
// failure on the cancelled context surfaced from Run as an error.
func TestConsumer_ShutdownMidHandle_IsCleanAndDeadLettersNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("wes-shutdown-itest-%d", time.Now().UnixNano())))
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

	realProcessed := memory.NewProcessedEventRepo()
	parked := &blockingProcessed{ProcessedEventRepo: realProcessed, entered: make(chan struct{}, 1)}

	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.SystemClock{}
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	})

	consumer := inboundkafka.NewConsumer(brokers, fmt.Sprintf("wes-shutdown-itest-%d", time.Now().UnixNano()),
		usecases.NewObserveLaborPlan(memory.NewLaborPlanViewRepo(), parked),
		usecases.NewObserveInventoryChange(memory.NewInventoryViewRepo(), realProcessed),
		usecases.NewApplyTaskCompleted(usecases.NewRecordCompletion(workUnits, pools, publisher, clock), realProcessed),
		usecases.NewApplyOrderAllocated(enqueue, realProcessed, catalogue),
		usecases.NewApplyWorkDemandReleased(enqueue, realProcessed, catalogue),
		catalogue, nil)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	eventID := fmt.Sprintf("evt-shutdown-%d", time.Now().UnixNano())
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(eventID),
		Headers: cloudEventHeaders(),
		Value: mustCloudEventJSON(t, eventID, cloudevents.TypeShiftPlanCommitted, "workforce-management", map[string]any{
			"building_id": "bldg-1", "shift_id": "shift-shutdown", "path_id": fmt.Sprintf("pick-shutdown-%d", time.Now().UnixNano()),
			"planned_heads": 6, "planned_rate": 100.0, "planned_hours": 8.0,
		}),
	}); err != nil {
		t.Fatalf("publish ShiftPlanCommitted: %v", err)
	}

	select {
	case <-parked.entered:
	case <-time.After(60 * time.Second):
		t.Fatal("the consumer never started handling the message")
	}
	consumeCancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run after its context was cancelled mid-handle = %v, want nil (clean shutdown)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}

	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	defer func() { _ = dlqReader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	if msg, err := dlqReader.ReadMessage(readCtx); err == nil {
		t.Fatalf("a healthy message was dead-lettered on shutdown: key=%q headers=%v", string(msg.Key), msg.Headers)
	}
}
