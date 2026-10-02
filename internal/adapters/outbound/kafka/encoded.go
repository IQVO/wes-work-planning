package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// Encoded is one wire-ready Kafka message: everything a producer needs
// except the connection. Topic is carried explicitly so a writer with NO
// fixed topic (the outbox relay's RelaySink) can route each message
// itself, while Publisher and AnalyticsPublisher — whose writers DO pin a
// topic — leave kafkago.Message.Topic empty (kafka-go rejects a message
// that sets Topic when its Writer also does, and vice versa).
type Encoded struct {
	Topic     string
	EventType string
	Key       []byte
	Value     []byte
	Headers   []kafkago.Header
}

// Encoder turns domain events into their Kafka wire form WITHOUT sending
// them. Both Publisher (integration topic) and AnalyticsPublisher
// (analytics topic) implement it, and the transactional outbox
// (ADR-0014) calls Encode inside the use case's transaction — which is
// what lets Publisher.dataFor read the just-saved WorkUnit row — and
// persists the result for the relay to send later. An Encoder may return
// fewer Encoded than events it was given: AnalyticsPublisher skips event
// types outside its contract.
type Encoder interface {
	Encode(ctx context.Context, events ...shared.DomainEvent) ([]Encoded, error)
}

// message converts an Encoded into a kafkago.Message. topicSet controls
// whether msg.Topic is populated: true for a topic-less writer (RelaySink),
// false for a writer that already pins its topic.
func (e Encoded) message(topicSet bool) kafkago.Message {
	msg := kafkago.Message{Key: e.Key, Value: e.Value, Headers: e.Headers}
	if topicSet {
		msg.Topic = e.Topic
	}
	return msg
}

// RelaySink writes already-encoded messages to whichever topic each one
// names. It is the outbox relay's Sink: one kafka-go Writer with no fixed
// Topic serves both the integration and the analytics topic, so the relay
// needs no per-topic routing of its own.
type RelaySink struct {
	writer Writer
}

// NewRelaySink constructs a RelaySink over a topic-less writer on brokers.
//
// Balancer is kafkago.Hash — same as Publisher's and AnalyticsPublisher's
// own writers (see NewPublisher's doc comment for why Hash and not
// LeastBytes): every Encoded this sink relays was already keyed by its
// originating publisher, so the relay's own writer must also use a
// key-aware balancer or that key is silently discarded for partition
// placement on the second hop too.
func NewRelaySink(brokers []string) *RelaySink {
	return NewRelaySinkWithWriter(&kafkago.Writer{
		BatchTimeout:           syncWriterBatchTimeout,
		RequiredAcks:           syncWriterRequiredAcks,
		Addr:                   kafkago.TCP(brokers...),
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	})
}

// NewRelaySinkWithWriter builds a RelaySink against an already-constructed
// Writer — the seam unit tests use to substitute a fake without a broker.
// The writer must NOT have a fixed Topic, or kafka-go rejects every
// message RelaySink hands it.
func NewRelaySinkWithWriter(writer Writer) *RelaySink {
	return &RelaySink{writer: writer}
}

// Send writes msgs in one WriteMessages call, each routed to its own
// Encoded.Topic. An empty msgs is a no-op.
func (s *RelaySink) Send(ctx context.Context, msgs ...Encoded) error {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		out[i] = m.message(true)
	}
	return s.writer.WriteMessages(ctx, out...)
}

// Close releases the underlying writer.
func (s *RelaySink) Close() error {
	return s.writer.Close()
}

// Compile-time assertions: both publishers are Encoders the outbox can use.
var (
	_ Encoder = (*Publisher)(nil)
	_ Encoder = (*AnalyticsPublisher)(nil)
)
