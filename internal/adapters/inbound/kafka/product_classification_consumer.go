package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// productClassifiedData is product-master's ProductClassified payload (the
// CloudEvents `data` object), mirrored read-only from its AsyncAPI contract:
// a full-state replacement of one SKU's classification at `version`.
// temperature_class and dot_hazard_class are omitted when unset;
// classification_source is a migration artefact this context ignores.
type productClassifiedData struct {
	SKU              string   `json:"sku"`
	HandlingTags     []string `json:"handling_tags"`
	TemperatureClass string   `json:"temperature_class,omitempty"`
	DOTHazardClass   int      `json:"dot_hazard_class,omitempty"`
	Version          int64    `json:"version"`
}

// errInvalidProductClassified marks a payload that can never be applied.
var errInvalidProductClassified = errors.New("invalid ProductClassified payload")

func (d productClassifiedData) validate() error {
	switch {
	case d.SKU == "":
		return fmt.Errorf("%w: empty sku", errInvalidProductClassified)
	case d.Version < 1:
		return fmt.Errorf("%w: version %d < 1", errInvalidProductClassified, d.Version)
	case d.DOTHazardClass < 0 || d.DOTHazardClass > 9:
		return fmt.Errorf("%w: dot_hazard_class %d outside 1..9", errInvalidProductClassified, d.DOTHazardClass)
	default:
		return nil
	}
}

// messageReader is the slice of *kafkago.Reader the consumer uses, so the
// fetch/commit discipline can be unit-tested with a fake reader.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// Retry bounds for a transient failure applying one message: the SAME
// message is retried, with exponential backoff capped at the max, until it
// succeeds or the consumer is stopped.
const (
	classificationRetryInitial = 200 * time.Millisecond
	classificationRetryMax     = 5 * time.Second
)

// ProductClassificationConsumer feeds this context's local classification
// copy from product-master's warehouse.product-master.events (ADR-0035).
//
//   - Dispatches on the FULL type cloudevents.TypeProductClassified; every
//     other type on the topic is committed past untouched.
//   - A message that is not a valid CloudEvent, or a ProductClassified whose
//     data is undecodable or invalid (no sku, version < 1, DOT class outside
//     1..9), is deterministic poison: WARN with topic/partition/offset, then
//     committed past.
//   - Applying goes through usecases.ObserveProductClassification, which
//     claims the CloudEvents id and runs the version-guarded upsert in one
//     UnitOfWork (ADR-0028). Any error from it is transient: the same message
//     is retried until it succeeds; nothing is committed before that
//     (FetchMessage + CommitMessages, never ReadMessage).
//   - Stopping mid-retry returns without committing, so the message is
//     redelivered to whichever member owns the partition next.
type ProductClassificationConsumer struct {
	reader       messageReader
	observe      *usecases.ObserveProductClassification
	logger       *slog.Logger
	retryInitial time.Duration
	retryMax     time.Duration
}

// NewProductClassificationConsumer reads cloudevents.TopicProductMasterEvents
// under groupID — a STABLE group shared by every replica (the copy is shared
// state in Postgres), read by cmd/wes from PRODUCT_CLASSIFICATION_CONSUMER_GROUP.
func NewProductClassificationConsumer(brokers []string, groupID string, observe *usecases.ObserveProductClassification, logger *slog.Logger) *ProductClassificationConsumer {
	return NewProductClassificationConsumerForTopic(brokers, groupID, cloudevents.TopicProductMasterEvents, observe, logger)
}

// NewProductClassificationConsumerForTopic is NewProductClassificationConsumer
// with the topic overridden, so integration tests can use an isolated topic.
func NewProductClassificationConsumerForTopic(brokers []string, groupID, topic string, observe *usecases.ObserveProductClassification, logger *slog.Logger) *ProductClassificationConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
		// A brand-new group starts at the beginning of the topic so the
		// copy fills from history; afterwards the committed offsets win.
		StartOffset: kafkago.FirstOffset,
	})
	return newProductClassificationConsumer(reader, observe, logger)
}

func newProductClassificationConsumer(reader messageReader, observe *usecases.ObserveProductClassification, logger *slog.Logger) *ProductClassificationConsumer {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &ProductClassificationConsumer{
		reader:       reader,
		observe:      observe,
		logger:       logger,
		retryInitial: classificationRetryInitial,
		retryMax:     classificationRetryMax,
	}
}

// Close releases the Kafka reader.
func (c *ProductClassificationConsumer) Close() error {
	return c.reader.Close()
}

// Run consumes until ctx is cancelled (returning nil) or the reader fails.
func (c *ProductClassificationConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("product classification consumer: fetch: %w", err)
		}
		if err := c.handle(ctx, msg); err != nil {
			// Only a cancellation mid-retry gets here: leave the offset
			// uncommitted so the message is redelivered.
			return nil
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("product classification consumer: commit: %w", err)
		}
	}
}

// handle processes one message inside a "kafka.consume <topic>" span. It
// returns nil when the message may be committed (applied, stale, redelivered,
// ignored or skipped as poison) and an error only when ctx was cancelled
// before a transient failure healed.
func (c *ProductClassificationConsumer) handle(ctx context.Context, msg kafkago.Message) error {
	msgCtx, span := otelkafka.StartConsumeSpan(otelkafka.Extract(ctx, &msg), msg.Topic,
		semconv.MessagingKafkaOffset(int(msg.Offset)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(msg.Partition)),
	)
	defer span.End()

	req, ok := c.decode(msgCtx, msg)
	if !ok {
		return nil
	}
	span.SetAttributes(attribute.String("messaging.message.id", req.EventId), attribute.String("product.sku", req.SKU))
	if err := c.applyWithRetry(msgCtx, req); err != nil {
		recordSpanError(span, err)
		return err
	}
	return nil
}

// decode turns msg into a use-case request; ok=false means "commit past it".
func (c *ProductClassificationConsumer) decode(ctx context.Context, msg kafkago.Message) (usecases.ObserveProductClassificationRequest, bool) {
	env, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.logger.WarnContext(ctx, "skipping invalid CloudEvent on product-master topic",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return usecases.ObserveProductClassificationRequest{}, false
	}
	if env.Type() != cloudevents.TypeProductClassified {
		return usecases.ObserveProductClassificationRequest{}, false
	}
	var data productClassifiedData
	err = env.DataAs(&data)
	if err == nil {
		err = data.validate()
	}
	if err != nil {
		c.logger.WarnContext(ctx, "skipping invalid ProductClassified payload",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "id", env.ID(), "error", err)
		return usecases.ObserveProductClassificationRequest{}, false
	}
	observedAt := env.Time()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	return usecases.ObserveProductClassificationRequest{
		EventId:          env.ID(),
		SKU:              data.SKU,
		HandlingTags:     data.HandlingTags,
		TemperatureClass: data.TemperatureClass,
		DOTHazardClass:   data.DOTHazardClass,
		Version:          data.Version,
		ObservedAt:       observedAt,
	}, true
}

// applyWithRetry runs the use case until it succeeds, backing off between
// attempts; it returns ctx's error if the consumer stops first.
func (c *ProductClassificationConsumer) applyWithRetry(ctx context.Context, req usecases.ObserveProductClassificationRequest) error {
	delay := c.retryInitial
	for attempt := 1; ; attempt++ {
		outcome, err := c.observe.Execute(ctx, req)
		if err == nil {
			c.logger.DebugContext(ctx, "ProductClassified handled",
				"id", req.EventId, "sku", req.SKU, "version", req.Version, "outcome", outcome.String())
			return nil
		}
		c.logger.ErrorContext(ctx, "applying ProductClassified failed; retrying the same message",
			"id", req.EventId, "sku", req.SKU, "attempt", attempt, "retry_in", delay.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, c.retryMax)
	}
}
