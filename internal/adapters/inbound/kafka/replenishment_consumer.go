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
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// replenishmentRequestedData is pick-face-replenishment's
// ReplenishmentRequested payload (the CloudEvents `data` object), mirrored
// read-only from its apis/asyncapi.yaml: only the fields this context plans
// from. policy_min and policy_max are not needed (ADR-0042).
type replenishmentRequestedData struct {
	RequestId   string    `json:"request_id"`
	SiteId      string    `json:"site_id"`
	SKU         string    `json:"sku"`
	ForwardBin  string    `json:"forward_bin"`
	SourceBin   string    `json:"source_bin"`
	Quantity    int       `json:"quantity"`
	Priority    string    `json:"priority"`
	RequestedAt time.Time `json:"requested_at"`
}

// replenishmentCancelledData is pick-face-replenishment's
// ReplenishmentCancelled payload: reason is OPERATOR, ASSIGNMENT_CHANGED or
// POLICY_REMOVED.
type replenishmentCancelledData struct {
	RequestId string `json:"request_id"`
	Reason    string `json:"reason"`
}

// errBadReplenishmentPayload marks a CloudEvent of a handled type whose
// `data` cannot be decoded: deterministic poison, never retried.
var errBadReplenishmentPayload = errors.New("malformed replenishment event payload")

// ReplenishmentConsumer feeds REPLENISH work from pick-face-replenishment's
// warehouse.pick-face-replenishment.events (ADR-0042).
//
//   - It dispatches on the FULL CloudEvents type: ReplenishmentRequested
//     plans one REPLENISH work unit, ReplenishmentCancelled withdraws a
//     Pending one; ReplenishmentCompleted and every unknown type are
//     committed past untouched.
//   - A message that is not a valid CloudEvent (bad JSON, the retired flat
//     envelope, a missing attribute) is WARN-logged and committed past,
//     never parsed as a legacy shape.
//   - The use cases claim the CloudEvents id and write their effect in ONE
//     atomic scope (ADR-0028). A payload that can never be applied (a broken
//     request) is dead-lettered at once; any other error is retried with
//     jittered backoff up to maxHandlerAttempts, then the raw message goes to
//     "<topic>.dlq" and the offset is committed so one poison message never
//     blocks the partition.
//   - The offset is committed only after success (FetchMessage +
//     CommitMessages, never ReadMessage), and a message that only shutdown
//     interrupted is never dead-lettered: Run returns without committing.
type ReplenishmentConsumer struct {
	reader    messageReader
	dlq       dlqWriter
	topic     string
	requested *usecases.ApplyReplenishmentRequested
	cancelled *usecases.ApplyReplenishmentCancelled
	logger    *slog.Logger
}

// NewReplenishmentConsumer reads cloudevents.TopicPickFaceReplenishmentEvents
// under groupID: a STABLE group shared by every replica, read by cmd/wes from
// REPLENISHMENT_CONSUMER_GROUP (never a literal).
func NewReplenishmentConsumer(brokers []string, groupID string, requested *usecases.ApplyReplenishmentRequested, cancelled *usecases.ApplyReplenishmentCancelled, logger *slog.Logger) *ReplenishmentConsumer {
	return NewReplenishmentConsumerForTopic(brokers, groupID, cloudevents.TopicPickFaceReplenishmentEvents, requested, cancelled, logger)
}

// NewReplenishmentConsumerForTopic is NewReplenishmentConsumer with the topic
// overridden, so integration tests can use an isolated topic (and with it an
// isolated "<topic>.dlq").
func NewReplenishmentConsumerForTopic(brokers []string, groupID, topic string, requested *usecases.ApplyReplenishmentRequested, cancelled *usecases.ApplyReplenishmentCancelled, logger *slog.Logger) *ReplenishmentConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
		// A brand-new group starts at the beginning of the topic: every
		// request still open is work to plan; afterwards the committed
		// offsets win.
		StartOffset: kafkago.FirstOffset,
	})
	return newReplenishmentConsumer(reader, newDLQWriter(brokers, topic), topic, requested, cancelled, logger)
}

func newReplenishmentConsumer(reader messageReader, dlq dlqWriter, topic string, requested *usecases.ApplyReplenishmentRequested, cancelled *usecases.ApplyReplenishmentCancelled, logger *slog.Logger) *ReplenishmentConsumer {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &ReplenishmentConsumer{reader: reader, dlq: dlq, topic: topic, requested: requested, cancelled: cancelled, logger: logger}
}

// Close releases the reader and the dead-letter writer.
func (c *ReplenishmentConsumer) Close() error {
	return errors.Join(c.reader.Close(), c.dlq.Close())
}

// Run consumes until ctx is cancelled (returning nil) or the reader or the
// dead-letter topic fails.
func (c *ReplenishmentConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("replenishment consumer: fetch: %w", err)
		}
		if err := c.settle(ctx, msg); err != nil {
			if ctx.Err() != nil {
				// Shutdown landed mid-handle: nothing was committed, so the
				// message is redelivered on the next start and the use
				// case's processed-event claim absorbs the replay.
				return nil
			}
			return err
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("replenishment consumer: commit: %w", err)
		}
	}
}

// settle handles msg and, when handling failed for good, dead-letters it. It
// returns nil when msg may be committed, ctx's error when shutdown
// interrupted the handling (a message only cancellation interrupted is never
// dead-lettered), and an error when the dead-letter publish itself failed
// (the message must then NOT be committed).
func (c *ReplenishmentConsumer) settle(ctx context.Context, msg kafkago.Message) error {
	err := c.handle(ctx, msg)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.logger.ErrorContext(ctx, "replenishment event failed, sending to dead-letter topic",
		"topic", c.topic, "partition", msg.Partition, "offset", msg.Offset, "dlq_topic", c.topic+dlqTopicSuffix, "error", err)
	if dlqErr := c.sendToDeadLetter(ctx, msg, err); dlqErr != nil {
		return fmt.Errorf("replenishment consumer: publish to dead-letter topic: %w", dlqErr)
	}
	return nil
}

// handle processes one message inside a "kafka.consume <topic>" span. It
// returns nil when the message may be committed (applied, redelivered,
// ignored or skipped) and the failure otherwise.
func (c *ReplenishmentConsumer) handle(ctx context.Context, msg kafkago.Message) error {
	msgCtx, span := otelkafka.StartConsumeSpan(otelkafka.Extract(ctx, &msg), c.topic,
		semconv.MessagingKafkaOffset(int(msg.Offset)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(msg.Partition)),
	)
	defer span.End()

	env, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.logger.WarnContext(msgCtx, "skipping a message that is not a valid CloudEvent on the pick-face-replenishment topic",
			"topic", c.topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	span.SetAttributes(attribute.String("messaging.message.id", env.ID()), attribute.String("cloudevents.event_type", env.Type()))

	apply := c.applierFor(env.Type())
	if apply == nil {
		return nil
	}
	if err := c.applyWithRetry(msgCtx, env, apply); err != nil {
		recordSpanError(span, err)
		return err
	}
	return nil
}

// applierFor selects the use-case call for a FULL CloudEvents type, nil for
// a type this consumer ignores.
func (c *ReplenishmentConsumer) applierFor(eventType string) func(context.Context, ce.Event) error {
	if eventType == cloudevents.TypeReplenishmentRequested {
		return c.applyRequested
	}
	if eventType == cloudevents.TypeReplenishmentCancelled {
		return c.applyCancelled
	}
	return nil
}

// applyWithRetry runs apply with jittered exponential backoff, up to
// maxHandlerAttempts attempts, bounded by ctx. A payload that can never be
// applied is permanent: it is not retried.
func (c *ReplenishmentConsumer) applyWithRetry(ctx context.Context, env ce.Event, apply func(context.Context, ce.Event) error) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)
	return backoff.Retry(func() error {
		err := apply(ctx, env)
		if err != nil && isReplenishmentPoison(err) {
			return backoff.Permanent(err)
		}
		return err
	}, bounded)
}

// isReplenishmentPoison reports whether err can never succeed on a retry: a
// payload that does not decode or a request the domain refuses.
func isReplenishmentPoison(err error) bool {
	return errors.Is(err, errBadReplenishmentPayload) ||
		errors.Is(err, workunit.ErrInvalidReplenishmentMove) ||
		errors.Is(err, workunit.ErrUnknownReplenishmentPriority) ||
		errors.Is(err, usecases.ErrInvalidReplenishmentRequest)
}

func (c *ReplenishmentConsumer) applyRequested(ctx context.Context, env ce.Event) error {
	var data replenishmentRequestedData
	if err := env.DataAs(&data); err != nil {
		return fmt.Errorf("%w: %v", errBadReplenishmentPayload, err)
	}
	outcome, err := c.requested.Execute(ctx, usecases.ApplyReplenishmentRequestedRequest{
		EventId:     env.ID(),
		OccurredAt:  env.Time(),
		RequestId:   data.RequestId,
		SiteId:      data.SiteId,
		SKU:         data.SKU,
		FromBin:     data.SourceBin,
		ToBin:       data.ForwardBin,
		Quantity:    data.Quantity,
		Priority:    data.Priority,
		RequestedAt: data.RequestedAt,
	})
	if err != nil {
		return err
	}
	c.logger.InfoContext(ctx, "ReplenishmentRequested handled",
		"event_id", env.ID(), "request_id", data.RequestId, "priority", data.Priority, "outcome", outcome.String())
	return nil
}

func (c *ReplenishmentConsumer) applyCancelled(ctx context.Context, env ce.Event) error {
	var data replenishmentCancelledData
	if err := env.DataAs(&data); err != nil {
		return fmt.Errorf("%w: %v", errBadReplenishmentPayload, err)
	}
	outcome, err := c.cancelled.Execute(ctx, usecases.ApplyReplenishmentCancelledRequest{
		EventId:    env.ID(),
		OccurredAt: env.Time(),
		RequestId:  data.RequestId,
		Reason:     data.Reason,
	})
	if err != nil {
		return err
	}
	c.logger.InfoContext(ctx, "ReplenishmentCancelled handled",
		"event_id", env.ID(), "request_id", data.RequestId, "reason", data.Reason, "outcome", outcome.String())
	return nil
}

// sendToDeadLetter publishes the raw, unmodified message to "<topic>.dlq"
// with error context as headers (so the body stays byte-identical for a
// manual replay). The writer pins the dead-letter topic.
func (c *ReplenishmentConsumer) sendToDeadLetter(ctx context.Context, msg kafkago.Message, cause error) error {
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.topic)},
		kafkago.Header{Key: "x-dlq-source-partition", Value: []byte(strconv.Itoa(msg.Partition))},
		kafkago.Header{Key: "x-dlq-source-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return writeDLQ(ctx, c.dlq, kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers})
}
