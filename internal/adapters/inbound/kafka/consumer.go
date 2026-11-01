// Package kafka is the inbound Kafka adapter: it consumes the integration
// events this service subscribes to and calls the additive projector use
// cases that maintain the labor-plan and usable-inventory read models.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// maxHandlerAttempts bounds a handler's in-process retry before a
// message is dead-lettered: 1 initial attempt plus up to 2 retries.
const maxHandlerAttempts = 3

const (
	retryInitialInterval = 100 * time.Millisecond
	retryMaxInterval     = 2 * time.Second
)

// Consumer consumes warehouse.workforce.events, warehouse.inventory.events,
// warehouse.fulfillment.events, warehouse.order-management.events,
// warehouse.network-inventory-planning.events and, once WithSlotPlans wires
// it, warehouse.slotting-optimization.events (SlotPlanApproved -> RELOCATE
// work, ADR-0040). The
// first two are projected into the labor-plan-view and inventory-view read
// models; TaskCompleted from the third is fed into ApplyTaskCompleted (which
// wraps the existing RecordCompletion) to close the control loop's feedback
// edge from Execution back to this service; OrderAllocated/
// OrderPartiallyAllocated from the fourth is fed into ApplyOrderAllocated
// (which wraps the existing EnqueueWorkUnit), replacing order-management's
// former synchronous HTTP call to POST /paths/{pathId}/work-units with
// event choreography; WorkDemandReleased from the fifth is fed into
// ApplyWorkDemandReleased, which enqueues one transfer-referenced work unit
// per demand leg under the deterministic id demand_id (ADR-0033).
//
// Idempotency lives in the use cases, never here: each one records the
// CloudEvents id as processed in the SAME atomic scope as its effect, so a
// failed attempt leaves no mark behind and the retry re-applies the event
// rather than skipping it as a redelivery (ADR-0028).
type Consumer struct {
	workforceReader       *kafkago.Reader
	inventoryReader       *kafkago.Reader
	fulfillmentReader     *kafkago.Reader
	orderManagementReader *kafkago.Reader
	networkDemandReader   *kafkago.Reader
	// slottingReader reads warehouse.slotting-optimization.events; nil
	// until WithSlotPlans wires the SlotPlanApproved use case (ADR-0040).
	slottingReader *kafkago.Reader
	brokers        []string
	groupID        string
	// dlqWriters holds one *kafkago.Writer per consumed topic
	// (topic -> writer), each publishing to that topic's own
	// "<topic>.dlq" — see dlqPublish's doc comment. Keyed by the
	// SOURCE topic name (reader.Config().Topic), not a fixed index,
	// so handleMessage/handleFulfillmentMessage can look up the right
	// writer generically regardless of which reader the message came
	// from.
	dlqWriters              map[string]dlqWriter
	observeLabor            *usecases.ObserveLaborPlan
	observeInventory        *usecases.ObserveInventoryChange
	applyTaskCompleted      *usecases.ApplyTaskCompleted
	applyOrderAllocated     *usecases.ApplyOrderAllocated
	applyWorkDemandReleased *usecases.ApplyWorkDemandReleased
	applySlotPlanApproved   *usecases.ApplySlotPlanApproved
	catalogue               ports.PathCatalogue
	logger                  *slog.Logger
}

func NewConsumer(brokers []string, groupID string, observeLabor *usecases.ObserveLaborPlan, observeInventory *usecases.ObserveInventoryChange, applyTaskCompleted *usecases.ApplyTaskCompleted, applyOrderAllocated *usecases.ApplyOrderAllocated, applyWorkDemandReleased *usecases.ApplyWorkDemandReleased, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	return newConsumer(brokers, groupID, cloudevents.TopicFulfillmentEvents, observeLabor, observeInventory, applyTaskCompleted, applyOrderAllocated, applyWorkDemandReleased, catalogue, logger)
}

// NewConsumerForFulfillmentTopic is NewConsumer with the
// warehouse.fulfillment.events topic overridden — needed so an integration
// test can point the real replay/idempotency logic at a
// throwaway, uniquely-named topic instead of the pinned production
// constant, mirroring this fleet's standing testcontainers pattern (see
// inventory-storage's facilitycache consumer).
func NewConsumerForFulfillmentTopic(brokers []string, groupID string, fulfillmentTopic string, observeLabor *usecases.ObserveLaborPlan, observeInventory *usecases.ObserveInventoryChange, applyTaskCompleted *usecases.ApplyTaskCompleted, applyOrderAllocated *usecases.ApplyOrderAllocated, applyWorkDemandReleased *usecases.ApplyWorkDemandReleased, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	return newConsumer(brokers, groupID, fulfillmentTopic, observeLabor, observeInventory, applyTaskCompleted, applyOrderAllocated, applyWorkDemandReleased, catalogue, logger)
}

func newConsumer(brokers []string, groupID string, fulfillmentTopic string, observeLabor *usecases.ObserveLaborPlan, observeInventory *usecases.ObserveInventoryChange, applyTaskCompleted *usecases.ApplyTaskCompleted, applyOrderAllocated *usecases.ApplyOrderAllocated, applyWorkDemandReleased *usecases.ApplyWorkDemandReleased, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	topics := []string{cloudevents.TopicWorkforceEvents, cloudevents.TopicInventoryEvents, fulfillmentTopic, cloudevents.TopicOrderManagementEvents, cloudevents.TopicNetworkDemandEvents}
	dlqWriters := make(map[string]dlqWriter, len(topics))
	for _, topic := range topics {
		dlqWriters[topic] = newDLQWriter(brokers, topic)
	}
	return &Consumer{
		workforceReader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   cloudevents.TopicWorkforceEvents,
		}),
		inventoryReader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   cloudevents.TopicInventoryEvents,
		}),
		fulfillmentReader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   fulfillmentTopic,
		}),
		orderManagementReader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   cloudevents.TopicOrderManagementEvents,
		}),
		networkDemandReader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   cloudevents.TopicNetworkDemandEvents,
		}),
		brokers:                 brokers,
		groupID:                 groupID,
		dlqWriters:              dlqWriters,
		observeLabor:            observeLabor,
		observeInventory:        observeInventory,
		applyTaskCompleted:      applyTaskCompleted,
		applyOrderAllocated:     applyOrderAllocated,
		applyWorkDemandReleased: applyWorkDemandReleased,
		catalogue:               catalogue,
		logger:                  logger,
	}
}

// WithSlotPlans adds the sixth stream (ADR-0040): SlotPlanApproved from
// warehouse.slotting-optimization.events, under the same consumer group,
// with its own "<topic>.dlq" writer, fed into ApplySlotPlanApproved.
func (c *Consumer) WithSlotPlans(uc *usecases.ApplySlotPlanApproved) *Consumer {
	return c.WithSlotPlansTopic(uc, cloudevents.TopicSlottingEvents)
}

// WithSlotPlansTopic is WithSlotPlans against an explicit topic, so an
// integration test can point the real handling at a throwaway topic.
func (c *Consumer) WithSlotPlansTopic(uc *usecases.ApplySlotPlanApproved, topic string) *Consumer {
	c.applySlotPlanApproved = uc
	c.slottingReader = kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: c.brokers,
		GroupID: c.groupID,
		Topic:   topic,
	})
	c.dlqWriters[topic] = newDLQWriter(c.brokers, topic)
	return c
}

func (c *Consumer) Close() error {
	errs := []error{}
	for _, r := range c.readers() {
		errs = append(errs, r.reader.Close())
	}
	for _, w := range c.dlqWriters {
		errs = append(errs, w.Close())
	}
	return errors.Join(errs...)
}

// stream pairs one consumed topic's reader with its handler.
type stream struct {
	reader *kafkago.Reader
	handle func(context.Context, ce.Event) error
}

// readers lists every stream this consumer reads: the five fixed topics,
// plus slotting-optimization's when WithSlotPlans wired it.
func (c *Consumer) readers() []stream {
	out := []stream{
		{c.workforceReader, c.handleWorkforceEvent},
		{c.inventoryReader, c.handleInventoryEvent},
		{c.fulfillmentReader, c.handleFulfillmentEvent},
		{c.orderManagementReader, c.handleOrderManagementEvent},
		{c.networkDemandReader, c.handleNetworkDemandEvent},
	}
	if c.slottingReader != nil {
		out = append(out, stream{c.slottingReader, c.handleSlottingEvent})
	}
	return out
}

// Run consumes every stream until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	streams := c.readers()
	errCh := make(chan error, len(streams))
	for _, s := range streams {
		go func() { errCh <- c.consumeLoop(ctx, s.reader, s.handle) }()
	}
	for range streams {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) consumeLoop(ctx context.Context, reader *kafkago.Reader, handle func(context.Context, ce.Event) error) error {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if err := c.handleMessage(ctx, reader, msg, handle); err != nil {
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				// Shutdown landed mid-handle: the offset was not committed,
				// so the message is redelivered on the next start and the
				// use case's processed-events claim absorbs the replay.
				return nil
			}
			return err
		}
	}
}

// handleMessage processes one fetched message inside a
// "kafka.consume <topic>" span whose parent is the producing service's
// publish span, recovered from the message's W3C trace-context headers.
// A message that is not a valid CloudEvents 1.0 event (bad JSON, the
// retired flat envelope, a missing required attribute) is a deterministic
// poison message: it is published straight to "<topic>.dlq" (no retry —
// retrying a decode failure can never succeed) and committed, never
// parsed as any legacy shape (ADR-0027). An
// unhandleable message (a genuine infrastructure error from the use
// case, e.g. a Postgres hiccup) is retried in-process with jittered
// backoff up to maxHandlerAttempts total attempts — a transient blip
// heals itself without ever reaching the DLQ. Only once ALL attempts
// are exhausted is the raw message published, byte-for-byte, to
// "<topic>.dlq" with error-context headers, and the offset is committed
// anyway: one poison message must never block every other event behind
// it on this partition (mirrors order-management's RepromiseConsumer
// DLQ design, ADR-0025 there). Only a commit failure or a DLQ publish
// failure aborts the consume loop, which is the error this returns.
func (c *Consumer) handleMessage(ctx context.Context, reader *kafkago.Reader, msg kafkago.Message, handle func(context.Context, ce.Event) error) error {
	topic := reader.Config().Topic

	msgCtx, span := otelkafka.StartConsumeSpan(otelkafka.Extract(ctx, &msg), topic,
		semconv.MessagingKafkaOffset(int(msg.Offset)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(msg.Partition)),
	)
	defer span.End()

	if err := c.dispatch(ctx, msgCtx, span, topic, msg, handle); err != nil {
		return err
	}
	if err := reader.CommitMessages(ctx, msg); err != nil {
		recordSpanError(span, err)
		return err
	}
	return nil
}

// dispatch is handleMessage minus the offset commit: decode, then either
// dead-letter a non-CloudEvent immediately or run handle with bounded
// retry and dead-letter it once every attempt has failed. It returns an
// error only when the DLQ publish itself fails (the message must then NOT
// be committed) or when ctx was cancelled mid-handle (ctx.Err(): a message
// only cancellation interrupted is never dead-lettered); a handled,
// skipped, or dead-lettered message returns nil and the caller commits it.
func (c *Consumer) dispatch(ctx, msgCtx context.Context, span trace.Span, topic string, msg kafkago.Message, handle func(context.Context, ce.Event) error) error {
	env, err := cloudevents.Decode(msg.Value)
	if err != nil {
		recordSpanError(span, err)
		c.logError(msgCtx, "invalid CloudEvent, sending to dead-letter topic",
			"topic", topic, "partition", msg.Partition, "offset", msg.Offset,
			"dlq_topic", topic+dlqTopicSuffix, "error", err)
		if dlqErr := c.dlqPublish(ctx, topic, msg, err); dlqErr != nil {
			return fmt.Errorf("kafka: publish to dead-letter topic: %w", dlqErr)
		}
		return nil
	}

	span.SetAttributes(
		attribute.String("messaging.message.id", env.ID()),
		attribute.String("cloudevents.event_type", env.Type()),
		attribute.String("cloudevents.event_source", env.Source()),
	)

	if err := c.handleWithRetry(msgCtx, handle, env); err != nil {
		if ctx.Err() != nil {
			return ctx.Err() // shutting down: never dead-letter a message only cancellation interrupted
		}
		recordSpanError(span, err)
		c.logError(msgCtx, "exhausted retries, sending to dead-letter topic",
			"topic", topic, "dlq_topic", topic+dlqTopicSuffix,
			"id", env.ID(), "type", env.Type(), "attempts", maxHandlerAttempts, "error", err)
		if dlqErr := c.dlqPublish(ctx, topic, msg, err); dlqErr != nil {
			return fmt.Errorf("kafka: publish to dead-letter topic: %w", dlqErr)
		}
	}
	return nil
}

// handleWithRetry retries handle up to maxHandlerAttempts times with
// jittered exponential backoff, bounded by ctx's own
// deadline/cancellation — mirrors order-management's
// RepromiseConsumer.handleWithRetry (ADR-0025).
func (c *Consumer) handleWithRetry(ctx context.Context, handle func(context.Context, ce.Event) error, env ce.Event) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	return backoff.Retry(func() error {
		return handle(ctx, env)
	}, bounded)
}

// recordSpanError marks span as failed without changing any control flow.
func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// logError emits a structured ERROR record (ADR-0023: a DLQ hand-off is an
// operator-actionable failure, not a warning), carrying the consume span's
// trace_id/span_id via ctx. A nil logger silences output, as the tests rely on.
func (c *Consumer) logError(ctx context.Context, msg string, args ...any) {
	if c.logger != nil {
		c.logger.ErrorContext(ctx, msg, args...)
	}
}

// logInfo is log at INFO: for deliberate, expected skips that are not
// failures (e.g. a TaskCompleted for a work unit this context never
// planned).
func (c *Consumer) logInfo(ctx context.Context, msg string, args ...any) {
	if c.logger != nil {
		c.logger.InfoContext(ctx, msg, args...)
	}
}
