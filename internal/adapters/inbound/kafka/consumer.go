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
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// shiftPlanCommittedData is Workforce Management's ShiftPlanCommitted
// payload — a DIFFERENT model from this service's own ShiftPlan aggregate.
type shiftPlanCommittedData struct {
	BuildingId   string  `json:"building_id"`
	ShiftId      string  `json:"shift_id"`
	PathId       string  `json:"path_id"`
	PlannedHeads int     `json:"planned_heads"`
	PlannedRate  float64 `json:"planned_rate"`
	PlannedHours float64 `json:"planned_hours"`
}

// inventoryEventData is shared by StockReserved and ReservationRevoked.
type inventoryEventData struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demand_ref"`
}

// taskCompletedData is fulfillment-execution's TaskCompleted payload (the
// CloudEvents `data` object).
type taskCompletedData struct {
	TaskId     string `json:"task_id"`
	StationId  string `json:"station_id"`
	WorkUnitId string `json:"work_unit_id"`
}

// orderAllocatedData is order-management's OrderAllocated /
// OrderPartiallyAllocated payload — both event types share this identical
// shape, since both mean "these lines are ready to enqueue".
type orderAllocatedData struct {
	OrderId     string          `json:"order_id"`
	PromiseDate time.Time       `json:"promise_date"`
	Lines       []orderLineData `json:"lines"`
}

// orderLineData is one allocated-and-released order line within an
// OrderAllocated/OrderPartiallyAllocated payload.
type orderLineData struct {
	LineNo   int    `json:"line_no"`
	SKU      string `json:"sku"`
	PathId   string `json:"path_id"`
	GiftWrap bool   `json:"gift_wrap"`
}

// dlqTopicSuffix names the dead-letter topic a poison message is
// published to, relative to its OWN source topic (never a fixed
// constant): each of the four consumed topics gets its own
// "<topic>.dlq" — mirroring order-management's RepromiseConsumer DLQ
// design (ADR-0025 there) so an isolated integration-test topic
// automatically gets its own isolated DLQ topic for free.
const dlqTopicSuffix = ".dlq"

// maxHandlerAttempts bounds a handler's in-process retry before a
// message is dead-lettered: 1 initial attempt plus up to 2 retries.
const maxHandlerAttempts = 3

const (
	retryInitialInterval = 100 * time.Millisecond
	retryMaxInterval     = 2 * time.Second
)

// Consumer consumes warehouse.workforce.events, warehouse.inventory.events,
// warehouse.fulfillment.events, and warehouse.order-management.events. The
// first two are projected into the labor-plan-view and inventory-view read
// models; TaskCompleted from the third is fed directly into the existing
// RecordCompletion use case to close the control loop's feedback edge from
// Execution back to this service; OrderAllocated/OrderPartiallyAllocated
// from the fourth is fed directly into the existing EnqueueWorkUnit use
// case, replacing order-management's former synchronous HTTP call to
// POST /paths/{pathId}/work-units with event choreography.
type Consumer struct {
	workforceReader       *kafkago.Reader
	inventoryReader       *kafkago.Reader
	fulfillmentReader     *kafkago.Reader
	orderManagementReader *kafkago.Reader
	// dlqWriters holds one *kafkago.Writer per consumed topic
	// (topic -> writer), each publishing to that topic's own
	// "<topic>.dlq" — see dlqPublish's doc comment. Keyed by the
	// SOURCE topic name (reader.Config().Topic), not a fixed index,
	// so handleMessage/handleFulfillmentMessage can look up the right
	// writer generically regardless of which reader the message came
	// from.
	dlqWriters       map[string]*kafkago.Writer
	observeLabor     *usecases.ObserveLaborPlan
	observeInventory *usecases.ObserveInventoryChange
	recordCompletion *usecases.RecordCompletion
	enqueueWorkUnit  *usecases.EnqueueWorkUnit
	processed        ports.ProcessedEventRepo
	catalogue        ports.PathCatalogue
	logger           *slog.Logger
}

func NewConsumer(brokers []string, groupID string, observeLabor *usecases.ObserveLaborPlan, observeInventory *usecases.ObserveInventoryChange, recordCompletion *usecases.RecordCompletion, enqueueWorkUnit *usecases.EnqueueWorkUnit, processed ports.ProcessedEventRepo, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	return newConsumer(brokers, groupID, cloudevents.TopicFulfillmentEvents, observeLabor, observeInventory, recordCompletion, enqueueWorkUnit, processed, catalogue, logger)
}

// NewConsumerForFulfillmentTopic is NewConsumer with the
// warehouse.fulfillment.events topic overridden — needed so an integration
// test can point the real replay/idempotency logic at a
// throwaway, uniquely-named topic instead of the pinned production
// constant, mirroring this fleet's standing testcontainers pattern (see
// inventory-storage's facilitycache consumer).
func NewConsumerForFulfillmentTopic(brokers []string, groupID string, fulfillmentTopic string, observeLabor *usecases.ObserveLaborPlan, observeInventory *usecases.ObserveInventoryChange, recordCompletion *usecases.RecordCompletion, enqueueWorkUnit *usecases.EnqueueWorkUnit, processed ports.ProcessedEventRepo, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	return newConsumer(brokers, groupID, fulfillmentTopic, observeLabor, observeInventory, recordCompletion, enqueueWorkUnit, processed, catalogue, logger)
}

func newConsumer(brokers []string, groupID string, fulfillmentTopic string, observeLabor *usecases.ObserveLaborPlan, observeInventory *usecases.ObserveInventoryChange, recordCompletion *usecases.RecordCompletion, enqueueWorkUnit *usecases.EnqueueWorkUnit, processed ports.ProcessedEventRepo, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	topics := []string{cloudevents.TopicWorkforceEvents, cloudevents.TopicInventoryEvents, fulfillmentTopic, cloudevents.TopicOrderManagementEvents}
	dlqWriters := make(map[string]*kafkago.Writer, len(topics))
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
		dlqWriters:       dlqWriters,
		observeLabor:     observeLabor,
		observeInventory: observeInventory,
		recordCompletion: recordCompletion,
		enqueueWorkUnit:  enqueueWorkUnit,
		processed:        processed,
		catalogue:        catalogue,
		logger:           logger,
	}
}

func (c *Consumer) Close() error {
	err1 := c.workforceReader.Close()
	err2 := c.inventoryReader.Close()
	err3 := c.fulfillmentReader.Close()
	err4 := c.orderManagementReader.Close()
	errs := []error{err1, err2, err3, err4}
	for _, w := range c.dlqWriters {
		errs = append(errs, w.Close())
	}
	return errors.Join(errs...)
}

// Run consumes all four topics until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	errCh := make(chan error, 4)
	go func() { errCh <- c.consumeLoop(ctx, c.workforceReader, c.handleWorkforceEvent) }()
	go func() { errCh <- c.consumeLoop(ctx, c.inventoryReader, c.handleInventoryEvent) }()
	go func() { errCh <- c.consumeLoop(ctx, c.fulfillmentReader, c.handleFulfillmentEvent) }()
	go func() { errCh <- c.consumeLoop(ctx, c.orderManagementReader, c.handleOrderManagementEvent) }()

	for i := 0; i < 4; i++ {
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

	env, err := cloudevents.Decode(msg.Value)
	if err != nil {
		recordSpanError(span, err)
		c.log(msgCtx, "invalid CloudEvent, sending to dead-letter topic",
			"topic", topic, "partition", msg.Partition, "offset", msg.Offset,
			"dlq_topic", topic+dlqTopicSuffix, "error", err)
		if dlqErr := c.dlqPublish(ctx, topic, msg, err); dlqErr != nil {
			return fmt.Errorf("kafka: publish to dead-letter topic: %w", dlqErr)
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			recordSpanError(span, err)
			return err
		}
		return nil
	}

	span.SetAttributes(
		attribute.String("messaging.message.id", env.ID()),
		attribute.String("cloudevents.event_type", env.Type()),
		attribute.String("cloudevents.event_source", env.Source()),
	)

	if err := c.handleWithRetry(msgCtx, handle, env); err != nil {
		recordSpanError(span, err)
		c.log(msgCtx, "exhausted retries, sending to dead-letter topic",
			"topic", topic, "dlq_topic", topic+dlqTopicSuffix,
			"id", env.ID(), "type", env.Type(), "attempts", maxHandlerAttempts, "error", err)
		if dlqErr := c.dlqPublish(ctx, topic, msg, err); dlqErr != nil {
			return fmt.Errorf("kafka: publish to dead-letter topic: %w", dlqErr)
		}
	}

	if err := reader.CommitMessages(ctx, msg); err != nil {
		recordSpanError(span, err)
		return err
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

// dlqPublish writes the raw, unmodified message payload plus error
// context (as headers, so the raw body stays byte-identical for a
// manual replay tool) to topic's dead-letter topic. A topic with no
// registered writer (should never happen in production — every
// constructor seeds dlqWriters for every topic it reads) is a
// documented no-op rather than a nil-pointer panic, mirroring this
// fleet's nil-optional-dependency convention.
func (c *Consumer) dlqPublish(ctx context.Context, topic string, msg kafkago.Message, cause error) error {
	writer, ok := c.dlqWriters[topic]
	if !ok || writer == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return writeDLQ(ctx, writer, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// recordSpanError marks span as failed without changing any control flow.
func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func (c *Consumer) handleWorkforceEvent(ctx context.Context, env ce.Event) error {
	if env.Type() != cloudevents.TypeShiftPlanCommitted {
		return nil
	}

	var data shiftPlanCommittedData
	if err := env.DataAs(&data); err != nil {
		return err
	}
	pathId, err := shared.NewPathId(data.PathId)
	if err != nil {
		return err
	}
	// Validate against the declared process-path catalogue: a
	// ShiftPlanCommitted for a path_id no declared path family
	// recognizes is rejected outright rather than silently accepted
	// into a labor-plan-view nothing else will ever route real work
	// through. This is validation only — the WorkPool/labor-plan-view
	// key stays the original granular pathId (e.g. "pick-zone-a"), not
	// the catalogue's coarser family id, since each zone/station is a
	// genuinely distinct queue. See fulfillment-execution's ADR-0017
	// for the full catalogue rationale.
	if _, err := c.catalogue.Lookup(pathId.String()); err != nil {
		return err
	}

	return c.observeLabor.Execute(ctx, usecases.ObserveLaborPlanRequest{
		EventId:      env.ID(),
		PathId:       pathId,
		PlannedHeads: data.PlannedHeads,
		PlannedRate:  data.PlannedRate,
		PlannedHours: data.PlannedHours,
		ObservedAt:   env.Time(),
	})
}

func (c *Consumer) handleInventoryEvent(ctx context.Context, env ce.Event) error {
	var delta int
	switch env.Type() {
	case cloudevents.TypeStockReserved:
		delta = -1
	case cloudevents.TypeReservationRevoked:
		delta = 1
	default:
		return nil
	}

	var data inventoryEventData
	if err := env.DataAs(&data); err != nil {
		return err
	}

	_, err := c.observeInventory.Execute(ctx, usecases.ObserveInventoryChangeRequest{
		EventId:    env.ID(),
		SKU:        data.SKU,
		Quantity:   data.Quantity,
		Delta:      delta * data.Quantity,
		ObservedAt: env.Time(),
	})
	return err
}

// handleFulfillmentEvent filters for TaskCompleted and feeds it into the
// existing RecordCompletion use case. Idempotency reuses the same
// processed_events mechanism as the Task 7 projectors: RecordCompletion
// itself already rejects a double-complete at the domain level, but marking
// the CloudEvents id here avoids a spurious error/retry on mere redelivery.
func (c *Consumer) handleFulfillmentEvent(ctx context.Context, env ce.Event) error {
	if env.Type() != cloudevents.TypeTaskCompleted {
		return nil
	}

	var data taskCompletedData
	if err := env.DataAs(&data); err != nil {
		return err
	}

	alreadyProcessed, err := c.processed.TryMarkProcessed(ctx, env.ID(), env.Time())
	if err != nil {
		return err
	}
	if alreadyProcessed {
		return nil
	}

	_, err = c.recordCompletion.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: data.WorkUnitId})
	return err
}

// handleOrderManagementEvent filters for OrderAllocated / OrderPartially-
// Allocated — both event types share an identical payload shape and both
// mean "these lines are ready to enqueue", so this handler does not
// distinguish between them — and feeds each line in the payload into the
// existing EnqueueWorkUnit use case. This is the event-choreography
// replacement for order-management's former synchronous call to
// POST /paths/{pathId}/work-units: order-management now publishes here
// instead of calling this service's HTTP API directly.
//
// Idempotency mirrors handleFulfillmentEvent exactly: the CloudEvents id is
// marked processed BEFORE any EnqueueWorkUnit call, so a redelivery of the
// same OrderAllocated/OrderPartiallyAllocated event does not attempt to
// re-enqueue its lines.
func (c *Consumer) handleOrderManagementEvent(ctx context.Context, env ce.Event) error {
	if env.Type() != cloudevents.TypeOrderAllocated && env.Type() != cloudevents.TypeOrderPartiallyAllocated {
		return nil
	}

	var data orderAllocatedData
	if err := env.DataAs(&data); err != nil {
		return err
	}

	alreadyProcessed, err := c.processed.TryMarkProcessed(ctx, env.ID(), env.Time())
	if err != nil {
		return err
	}
	if alreadyProcessed {
		return nil
	}

	cpt := shared.NewCPT(data.PromiseDate)
	for _, line := range data.Lines {
		pathId, err := shared.NewPathId(line.PathId)
		if err != nil {
			return err
		}
		// Validate against the declared process-path catalogue before
		// ever creating a WorkPool for this pathId: an unrecognized
		// path_id from order-management (e.g. a typo or a stale
		// deploy referencing a path that was retired) must fail loud
		// here rather than silently seed a real WorkPool queue nothing
		// downstream will ever service. See fulfillment-execution's
		// ADR-0017 for the full catalogue rationale.
		if _, err := c.catalogue.Lookup(pathId.String()); err != nil {
			return err
		}

		workUnitId := fmt.Sprintf("%s-line-%d", data.OrderId, line.LineNo)
		_, err = c.enqueueWorkUnit.Execute(ctx, usecases.EnqueueWorkUnitRequest{
			WorkUnitId: workUnitId,
			PathId:     pathId,
			CPT:        cpt,
			Reference:  data.OrderId,
			SKU:        line.SKU,
			GiftWrap:   line.GiftWrap,
		})
		// release.ErrDuplicateEntry means WorkPool.Enqueue saw this
		// WorkUnitId already present in the pool. The processed_events
		// guard above already covers the common cause (redelivery of the
		// exact same CloudEvents id), but a collision could in principle also
		// arise some other way (e.g. a prior partial-allocation event for
		// the same order/line reprocessed under a different id, or
		// operator replay). Since the deterministic WorkUnitId means a
		// duplicate can only ever refer to the SAME logical work unit,
		// treating it as a benign no-op here (rather than a hard failure)
		// is the safe, idempotent choice — it must never crash or stall
		// the consumer for what is, by construction, the same unit of
		// work already known to the pool.
		if errors.Is(err, release.ErrDuplicateEntry) {
			continue
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// log emits a structured record through the configured logger, carrying the
// consume span's trace_id/span_id via ctx. A nil logger silences output, as
// the tests rely on.
func (c *Consumer) log(ctx context.Context, msg string, args ...any) {
	if c.logger != nil {
		c.logger.WarnContext(ctx, msg, args...)
	}
}

// newDLQWriter builds the dead-letter writer for topic. It MUST set
// AllowAutoTopicCreation: the fleet creates every topic on first write
// (warehouse-infra kafka.tf, num.partitions=8) and "<topic>.dlq" is only
// written on the rare poison path, so it usually does not exist yet.
// Without the flag the first poison message fails its DLQ publish with
// "[3] Unknown Topic Or Partition", the offset is (correctly) not
// committed, and Run returns -- observed live: the consumer stopped and
// no OrderAllocated ever became a work unit.
func newDLQWriter(brokers []string, topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic + dlqTopicSuffix,
		AllowAutoTopicCreation: true,
		// BatchTimeout: a DLQ write is a synchronous single message; with
		// kafka-go's 1s default the writer holds every write for a full second
		// waiting to fill a batch, capping dead-lettering at ~1 msg/s/partition
		// (observed live: a backlog of legacy messages took hours to drain while
		// the consumer processed nothing else).
		BatchTimeout: dlqBatchTimeout,
	}
}

// dlqTopicReadyAttempts / dlqTopicReadyBackoff bound how long a DLQ publish
// waits for an auto-created "<topic>.dlq" to become writable.
const (
	dlqTopicReadyAttempts = 40
	dlqTopicReadyBackoff  = 250 * time.Millisecond
)

// dlqMessageWriter is the slice of *kafkago.Writer writeDLQ needs.
type dlqMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// writeDLQ publishes msg to the dead-letter topic, retrying (bounded) while
// the topic is still being auto-created. AllowAutoTopicCreation alone is not
// enough: the first write races partition leader election and the broker
// answers UnknownTopicOrPartition / LeaderNotAvailable for a few hundred
// milliseconds. Any other error -- or exhausting the budget -- is returned,
// so the caller still refuses to commit the offset (no message loss).
func writeDLQ(ctx context.Context, w dlqMessageWriter, msg kafkago.Message) error {
	var err error
	for attempt := 0; attempt < dlqTopicReadyAttempts; attempt++ {
		if err = w.WriteMessages(ctx, msg); err == nil || !isTopicNotReady(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(dlqTopicReadyBackoff):
		}
	}
	return err
}

// isTopicNotReady reports whether err only means the (auto-created) topic
// has no leader yet.
func isTopicNotReady(err error) bool {
	var werrs kafkago.WriteErrors
	if errors.As(err, &werrs) {
		for _, e := range werrs {
			if e != nil && !isTopicNotReady(e) {
				return false
			}
		}
		return werrs.Count() > 0
	}
	return errors.Is(err, kafkago.UnknownTopicOrPartition) || errors.Is(err, kafkago.LeaderNotAvailable)
}

// dlqBatchTimeout flushes a dead-letter write almost immediately.
const dlqBatchTimeout = 10 * time.Millisecond
