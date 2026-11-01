package kafka

import (
	"context"
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// dlqTopicSuffix names the dead-letter topic a poison message is
// published to, relative to its OWN source topic (never a fixed
// constant): each of the four consumed topics gets its own
// "<topic>.dlq" — mirroring order-management's RepromiseConsumer DLQ
// design (ADR-0025 there) so an isolated integration-test topic
// automatically gets its own isolated DLQ topic for free.
const dlqTopicSuffix = ".dlq"

// dlqWriter is the slice of *kafkago.Writer the dead-letter path uses.
// An interface (rather than the concrete writer) so the retry-then-DLQ
// decision can be unit-tested with a fake, without a broker.
type dlqWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
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
		// RequiredAcks: kafka-go defaults to RequireNone (acks=0), so the
		// broker never confirms the write and a broker blip silently drops the
		// poison message while the consumer commits past it.
		RequiredAcks: dlqRequiredAcks,
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

// dlqRequiredAcks makes a dead-letter write wait for every in-sync replica.
const dlqRequiredAcks = kafkago.RequireAll
