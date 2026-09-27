//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/envelope"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// completionSignalPublisher wraps a real ports.EventPublisher and, in
// addition to forwarding every event, sends on completions once per
// WorkUnitCompleted event it sees. The test uses this channel — rather than
// a plain FindById/State() polling loop reading the shared in-memory
// *workunit.WorkUnit across goroutines with no synchronization — as the
// signal that RecordCompletion's Execute has finished mutating the work
// unit, since Publish is only ever called AFTER that mutation and the
// repository Save. A channel receive establishes a proper happens-before
// edge for every memory write that precedes the corresponding Publish call
// in the same goroutine, so reading the completed work unit's fields after
// receiving on this channel is race-free by construction, not by timing
// luck.
type completionSignalPublisher struct {
	inner       *events.LogPublisher
	completions chan struct{}
}

func newCompletionSignalPublisher(inner *events.LogPublisher) *completionSignalPublisher {
	return &completionSignalPublisher{inner: inner, completions: make(chan struct{}, 8)}
}

func (p *completionSignalPublisher) Publish(ctx context.Context, evts ...shared.DomainEvent) error {
	if err := p.inner.Publish(ctx, evts...); err != nil {
		return err
	}
	for _, ev := range evts {
		if ev.EventName() == "WorkUnitCompleted" {
			select {
			case p.completions <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

// TestFulfillmentConsumer_DualReadsFlatAndCloudEventsWithIdempotency runs
// the real fulfillmentReader consume path against an isolated Kafka broker
// (testcontainers — this repo's CI has no external Kafka service, so a
// KAFKA_BROKERS-skip-gated test would silently prove nothing, per AGENTS.md
// and this fleet's standing testcontainers convention).
//
// It publishes one flat-shaped and one CloudEvents-shaped TaskCompleted
// message onto warehouse.fulfillment.events, both carrying the SAME
// event_id/id, and asserts the use case fires exactly once — the
// idempotency safety ADR-0021 §3 explicitly calls load-bearing for the
// later dual-write phase (a not-yet-upgraded flat-only consumer receiving
// two CloudEvents messages with the same key would otherwise poison the
// idempotency gate on an empty-string event id; this dual-read consumer
// must never fall into that trap).
func TestFulfillmentConsumer_DualReadsFlatAndCloudEventsWithIdempotency(t *testing.T) {
	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("wes-dualread-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}

	topic := fmt.Sprintf("%s-dualread-%d", envelope.TopicFulfillmentEvents, time.Now().UnixNano())
	if err := createTopics(ctx, brokers, topic, envelope.TopicWorkforceEvents, envelope.TopicInventoryEvents, envelope.TopicOrderManagementEvents); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	workUnitId := fmt.Sprintf("integration-dualread-wu-%d", time.Now().UnixNano())
	eventId := fmt.Sprintf("evt-dualread-%d", time.Now().UnixNano())

	// Put the work unit into Released state before either TaskCompleted
	// message arrives, so RecordCompletion has something valid to
	// transition — same setup shape as the existing full-consumer
	// integration test.
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.SystemClock{}
	pathId, err := shared.NewPathId("pick-dualread")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)
	releaseNext := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock)
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: workUnitId, PathId: pathId, CPT: shared.NewCPT(time.Now().Add(2 * time.Hour)), Reference: "ref-1",
	}); err != nil {
		t.Fatalf("EnqueueWorkUnit: %v", err)
	}
	if _, err := releaseNext.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("ReleaseNextWork: %v", err)
	}

	publishCtx, publishCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer publishCancel()

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false}
	defer writer.Close()

	flatData, err := json.Marshal(map[string]any{
		"task_id": "task-1", "station_id": "station-1", "work_unit_id": workUnitId,
	})
	if err != nil {
		t.Fatalf("marshal flat data: %v", err)
	}
	flatBody, err := json.Marshal(map[string]any{
		"event_id": eventId, "event_type": "TaskCompleted", "occurred_at": time.Now().UTC().Format(time.RFC3339),
		"source": "fulfillment-execution", "data": json.RawMessage(flatData),
	})
	if err != nil {
		t.Fatalf("marshal flat envelope: %v", err)
	}

	ceData, err := json.Marshal(map[string]any{
		"task_id": "task-1", "station_id": "station-1", "work_unit_id": workUnitId,
	})
	if err != nil {
		t.Fatalf("marshal cloudevents data: %v", err)
	}
	ceBody, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": eventId,
		"type":            "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"source":          "/warehouse/fulfillment-execution",
		"subject":         "task-1",
		"time":            time.Now().UTC().Format(time.RFC3339),
		"datacontenttype": "application/json",
		"data":            json.RawMessage(ceData),
	})
	if err != nil {
		t.Fatalf("marshal cloudevents envelope: %v", err)
	}

	// Same event_id/id key on both messages, same topic — exactly the
	// dual-write-mode wire shape ADR-0021 §3 describes.
	if err := writer.WriteMessages(publishCtx,
		kafkago.Message{Key: []byte(eventId), Value: flatBody},
		kafkago.Message{Key: []byte(eventId), Value: ceBody},
	); err != nil {
		t.Fatalf("publish flat + cloudevents TaskCompleted: %v", err)
	}

	processed := memory.NewProcessedEventRepo()
	completionPublisher := newCompletionSignalPublisher(publisher)
	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, completionPublisher, clock)
	// The other three consumers on this Consumer aren't needed for this
	// probe, so a no-op observer suffices for observeLabor/observeInventory
	// and no real events are published to the other three topics.
	laborViews := memory.NewLaborPlanViewRepo()
	inventoryViews := memory.NewInventoryViewRepo()
	observeLabor := usecases.NewObserveLaborPlan(laborViews, processed)
	observeInventory := usecases.NewObserveInventoryChange(inventoryViews, processed)
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	})

	groupID := fmt.Sprintf("wes-dualread-test-%d", time.Now().UnixNano())
	consumer := inboundkafka.NewConsumerForFulfillmentTopic(brokers, groupID, topic, observeLabor, observeInventory, recordCompletion, enqueue, processed, catalogue, nil)
	defer consumer.Close()

	consumeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = consumer.Run(consumeCtx) }()

	// Wait for the FIRST WorkUnitCompleted publish — this happens strictly
	// after RecordCompletion.Execute has mutated and saved the work unit,
	// so receiving here is a race-free synchronization point.
	select {
	case <-completionPublisher.completions:
	case <-time.After(30 * time.Second):
		t.Fatalf("work unit never completed from either TaskCompleted message")
	}

	unit, err := workUnits.FindById(context.Background(), workUnitId)
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.State() != workunit.Completed {
		t.Fatalf("expected work unit to be Completed, got %v", unit.State())
	}
	completedAtFirst := unit.CompletedAt()
	if completedAtFirst == nil {
		t.Fatalf("expected CompletedAt to be set")
	}

	// The second (duplicate-key) message must NOT drive a second
	// WorkUnitCompleted publish — the processed_events idempotency gate
	// must no-op it. Confirm no second signal arrives on the channel
	// within a bounded window (a positive absence check, not a sleep+
	// re-read of shared state, which would reintroduce the same
	// synchronization hazard this test is designed to avoid).
	select {
	case <-completionPublisher.completions:
		t.Fatalf("RecordCompletion fired a second time for the same event_id — idempotency gate did not hold")
	case <-time.After(3 * time.Second):
		// Expected: no second completion.
	}
}
