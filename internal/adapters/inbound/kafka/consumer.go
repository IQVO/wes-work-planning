// Package kafka is the inbound Kafka adapter: it consumes the integration
// events this service subscribes to and calls the additive projector use
// cases that maintain the labor-plan and usable-inventory read models.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/envelope"
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

// taskCompletedData is fulfillment-execution's TaskCompleted payload. The
// shape is unchanged by ADR-0021's envelope migration: whether the message
// arrives in the legacy flat envelope or the CloudEvents 1.0 envelope, this
// struct decodes the identical `data` object either way.
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

// cloudEventProbe is a minimal decode target used only to detect whether a
// raw warehouse.fulfillment.events message is CloudEvents 1.0-shaped
// (specversion present) or the legacy flat envelope shape (specversion
// absent). See ADR-0021 Phase 2, Design decision #3: specversion is the
// sole dual-read discriminator and never appears on a flat message.
type cloudEventProbe struct {
	SpecVersion string `json:"specversion"`
}

// cloudEventEnvelope is the CloudEvents 1.0 structured envelope shape
// documented in fulfillment-execution's apis/asyncapi.yaml (ADR-0021 and
// its fulfillment-execution companion ADR-0027). Only the fields needed to
// normalize back to the legacy envelope.Envelope shape are decoded here.
type cloudEventEnvelope struct {
	SpecVersion     string          `json:"specversion"`
	Id              string          `json:"id"`
	Type            string          `json:"type"`
	Source          string          `json:"source"`
	Subject         string          `json:"subject"`
	Time            time.Time       `json:"time"`
	DataContentType string          `json:"datacontenttype"`
	Data            json.RawMessage `json:"data"`
}

// fulfillmentCloudEventTypePrefix is the reverse-DNS prefix
// fulfillment-execution's TaskCompleted `type` string carries on the wire
// (fulfillment-execution's apis/asyncapi.yaml,
// components.messages.TaskCompleted.examples), stripped back to the bare
// "TaskCompleted" name envelope.EventTypeTaskCompleted and
// handleFulfillmentEvent's switch already key on.
const fulfillmentCloudEventTypePrefix = "com.warehouse.wes.fulfillment-execution.task."

// decodeFulfillmentEnvelope dual-reads one warehouse.fulfillment.events
// message in either the legacy flat envelope shape or the CloudEvents 1.0
// structured envelope shape (ADR-0021 Phase 2, Task 2c), normalizing either
// one to the identical envelope.Envelope representation
// handleFulfillmentEvent already consumes unchanged — the `data` payload
// itself is byte-identical either way, so no business logic is duplicated
// across the two decode paths. A specversion that is present but not
// exactly "1.0" is treated as malformed/unrecognized and returns an error,
// which handleFulfillmentMessage handles the same way handleMessage already
// handles any other unparseable message: log and commit, no redelivery
// loop.
func decodeFulfillmentEnvelope(raw []byte) (envelope.Envelope, error) {
	var probe cloudEventProbe
	if err := json.Unmarshal(raw, &probe); err != nil {
		return envelope.Envelope{}, err
	}

	if probe.SpecVersion == "" {
		var env envelope.Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return envelope.Envelope{}, err
		}
		return env, nil
	}

	if probe.SpecVersion != "1.0" {
		return envelope.Envelope{}, fmt.Errorf("unsupported CloudEvents specversion %q", probe.SpecVersion)
	}

	var ce cloudEventEnvelope
	if err := json.Unmarshal(raw, &ce); err != nil {
		return envelope.Envelope{}, err
	}

	return envelope.Envelope{
		EventId:    ce.Id,
		EventType:  strings.TrimPrefix(ce.Type, fulfillmentCloudEventTypePrefix),
		OccurredAt: ce.Time,
		Source:     ce.Source,
		Data:       ce.Data,
	}, nil
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
	return newConsumer(brokers, groupID, envelope.TopicFulfillmentEvents, observeLabor, observeInventory, recordCompletion, enqueueWorkUnit, processed, catalogue, logger)
}

// NewConsumerForFulfillmentTopic is NewConsumer with the
// warehouse.fulfillment.events topic overridden — needed so an integration
// test can point the real dual-read replay/idempotency logic at a
// throwaway, uniquely-named topic instead of the pinned production
// constant, mirroring this fleet's standing testcontainers pattern (see
// inventory-storage's facilitycache consumer).
func NewConsumerForFulfillmentTopic(brokers []string, groupID string, fulfillmentTopic string, observeLabor *usecases.ObserveLaborPlan, observeInventory *usecases.ObserveInventoryChange, recordCompletion *usecases.RecordCompletion, enqueueWorkUnit *usecases.EnqueueWorkUnit, processed ports.ProcessedEventRepo, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	return newConsumer(brokers, groupID, fulfillmentTopic, observeLabor, observeInventory, recordCompletion, enqueueWorkUnit, processed, catalogue, logger)
}

func newConsumer(brokers []string, groupID string, fulfillmentTopic string, observeLabor *usecases.ObserveLaborPlan, observeInventory *usecases.ObserveInventoryChange, recordCompletion *usecases.RecordCompletion, enqueueWorkUnit *usecases.EnqueueWorkUnit, processed ports.ProcessedEventRepo, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	topics := []string{envelope.TopicWorkforceEvents, envelope.TopicInventoryEvents, fulfillmentTopic, envelope.TopicOrderManagementEvents}
	dlqWriters := make(map[string]*kafkago.Writer, len(topics))
	for _, topic := range topics {
		dlqWriters[topic] = &kafkago.Writer{
			Addr:  kafkago.TCP(brokers...),
			Topic: topic + dlqTopicSuffix,
		}
	}
	return &Consumer{
		workforceReader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   envelope.TopicWorkforceEvents,
		}),
		inventoryReader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   envelope.TopicInventoryEvents,
		}),
		fulfillmentReader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   fulfillmentTopic,
		}),
		orderManagementReader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   envelope.TopicOrderManagementEvents,
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
	go func() { errCh <- c.consumeFulfillmentLoop(ctx, c.fulfillmentReader, c.handleFulfillmentEvent) }()
	go func() { errCh <- c.consumeLoop(ctx, c.orderManagementReader, c.handleOrderManagementEvent) }()

	for i := 0; i < 4; i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) consumeLoop(ctx context.Context, reader *kafkago.Reader, handle func(context.Context, envelope.Envelope) error) error {
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

// consumeFulfillmentLoop is consumeLoop's counterpart for the
// warehouse.fulfillment.events topic: it dual-reads either the legacy flat
// envelope or the CloudEvents 1.0 envelope (ADR-0021 Phase 2, Task 2c) via
// decodeFulfillmentEnvelope before handing off to the identical envelope-
// shaped handling logic every other topic's consumeLoop already uses.
func (c *Consumer) consumeFulfillmentLoop(ctx context.Context, reader *kafkago.Reader, handle func(context.Context, envelope.Envelope) error) error {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if err := c.handleFulfillmentMessage(ctx, reader, msg, handle); err != nil {
			return err
		}
	}
}

// handleFulfillmentMessage mirrors handleMessage exactly, except the raw
// message is decoded via decodeFulfillmentEnvelope (dual-read: legacy flat
// envelope or CloudEvents 1.0) rather than a single json.Unmarshal into
// envelope.Envelope. An unparseable message, or one whose specversion is
// present but not "1.0", is logged and committed rather than redelivered
// forever — the same fail-soft posture handleMessage already applies to any
// other malformed message on any other topic.
func (c *Consumer) handleFulfillmentMessage(ctx context.Context, reader *kafkago.Reader, msg kafkago.Message, handle func(context.Context, envelope.Envelope) error) error {
	topic := reader.Config().Topic

	msgCtx, span := otelkafka.StartConsumeSpan(otelkafka.Extract(ctx, &msg), topic,
		semconv.MessagingKafkaOffset(int(msg.Offset)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(msg.Partition)),
	)
	defer span.End()

	env, err := decodeFulfillmentEnvelope(msg.Value)
	if err != nil {
		recordSpanError(span, err)
		c.log(msgCtx, "skipping unparseable kafka message", "topic", topic, "error", err)
		_ = reader.CommitMessages(ctx, msg)
		return nil
	}

	span.SetAttributes(
		attribute.String("messaging.message.event_id", env.EventId),
		attribute.String("messaging.message.event_type", env.EventType),
		attribute.String("messaging.message.source", env.Source),
	)

	if err := c.handleWithRetry(msgCtx, handle, env); err != nil {
		recordSpanError(span, err)
		c.log(msgCtx, "exhausted retries, sending to dead-letter topic",
			"topic", topic, "dlq_topic", topic+dlqTopicSuffix,
			"event_id", env.EventId, "event_type", env.EventType, "attempts", maxHandlerAttempts, "error", err)
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

// handleMessage processes one fetched message inside a
// "kafka.consume <topic>" span whose parent is the producing service's
// publish span, recovered from the message's W3C trace-context headers.
// An unparseable message is logged and committed immediately (never
// redelivered — retrying a decode failure can never succeed). An
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
func (c *Consumer) handleMessage(ctx context.Context, reader *kafkago.Reader, msg kafkago.Message, handle func(context.Context, envelope.Envelope) error) error {
	topic := reader.Config().Topic

	msgCtx, span := otelkafka.StartConsumeSpan(otelkafka.Extract(ctx, &msg), topic,
		semconv.MessagingKafkaOffset(int(msg.Offset)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(msg.Partition)),
	)
	defer span.End()

	var env envelope.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		recordSpanError(span, err)
		c.log(msgCtx, "skipping unparseable kafka message", "topic", topic, "error", err)
		_ = reader.CommitMessages(ctx, msg)
		return nil
	}

	span.SetAttributes(
		attribute.String("messaging.message.event_id", env.EventId),
		attribute.String("messaging.message.event_type", env.EventType),
		attribute.String("messaging.message.source", env.Source),
	)

	if err := c.handleWithRetry(msgCtx, handle, env); err != nil {
		recordSpanError(span, err)
		c.log(msgCtx, "exhausted retries, sending to dead-letter topic",
			"topic", topic, "dlq_topic", topic+dlqTopicSuffix,
			"event_id", env.EventId, "event_type", env.EventType, "attempts", maxHandlerAttempts, "error", err)
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
func (c *Consumer) handleWithRetry(ctx context.Context, handle func(context.Context, envelope.Envelope) error, env envelope.Envelope) error {
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
	return writer.WriteMessages(ctx, kafkago.Message{
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

func (c *Consumer) handleWorkforceEvent(ctx context.Context, env envelope.Envelope) error {
	if env.EventType != envelope.EventTypeShiftPlanCommitted {
		return nil
	}

	var data shiftPlanCommittedData
	if err := json.Unmarshal(env.Data, &data); err != nil {
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
		EventId:      env.EventId,
		PathId:       pathId,
		PlannedHeads: data.PlannedHeads,
		PlannedRate:  data.PlannedRate,
		PlannedHours: data.PlannedHours,
		ObservedAt:   env.OccurredAt,
	})
}

func (c *Consumer) handleInventoryEvent(ctx context.Context, env envelope.Envelope) error {
	var delta int
	switch env.EventType {
	case envelope.EventTypeStockReserved:
		delta = -1
	case envelope.EventTypeReservationRevoked:
		delta = 1
	default:
		return nil
	}

	var data inventoryEventData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return err
	}

	_, err := c.observeInventory.Execute(ctx, usecases.ObserveInventoryChangeRequest{
		EventId:    env.EventId,
		SKU:        data.SKU,
		Quantity:   data.Quantity,
		Delta:      delta * data.Quantity,
		ObservedAt: env.OccurredAt,
	})
	return err
}

// handleFulfillmentEvent filters for TaskCompleted and feeds it into the
// existing RecordCompletion use case. Idempotency reuses the same
// processed_events mechanism as the Task 7 projectors: RecordCompletion
// itself already rejects a double-complete at the domain level, but marking
// the event_id here avoids a spurious error/retry on mere redelivery.
func (c *Consumer) handleFulfillmentEvent(ctx context.Context, env envelope.Envelope) error {
	if env.EventType != envelope.EventTypeTaskCompleted {
		return nil
	}

	var data taskCompletedData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return err
	}

	alreadyProcessed, err := c.processed.TryMarkProcessed(ctx, env.EventId, env.OccurredAt)
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
// Idempotency mirrors handleFulfillmentEvent exactly: the event_id is
// marked processed BEFORE any EnqueueWorkUnit call, so a redelivery of the
// same OrderAllocated/OrderPartiallyAllocated message does not attempt to
// re-enqueue its lines.
func (c *Consumer) handleOrderManagementEvent(ctx context.Context, env envelope.Envelope) error {
	if env.EventType != envelope.EventTypeOrderAllocated && env.EventType != envelope.EventTypeOrderPartiallyAllocated {
		return nil
	}

	var data orderAllocatedData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return err
	}

	alreadyProcessed, err := c.processed.TryMarkProcessed(ctx, env.EventId, env.OccurredAt)
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
		// exact same event_id), but a collision could in principle also
		// arise some other way (e.g. a prior partial-allocation event for
		// the same order/line reprocessed under a different event_id, or
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
