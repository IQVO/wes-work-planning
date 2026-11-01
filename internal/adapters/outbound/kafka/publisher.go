// Package kafka is the outbound Kafka adapter: it implements
// ports.EventPublisher on top of github.com/segmentio/kafka-go, encoding
// each domain event as a CloudEvents 1.0 event (structured content mode,
// via internal/adapters/kafka/cloudevents) and writing it to this
// service's own topics.
package kafka

import (
	"context"
	"encoding/json"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// hazmatTag and fragileTag are the ProductClassification.HandlingTags
// values product-master publishes (ADR-0035; formerly inventory-storage's)
// that this service maps onto the
// WorkReleased integration event's derived hints (see ADR-0009). Named as
// constants here — not shared as a Go type across the repository boundary —
// because this is exactly the same translate-at-the-ACL discipline this
// adapter already applies to every other cross-service payload.
const (
	hazmatTag  = "Hazmat"
	fragileTag = "Fragile"
)

// IDGenerator returns a fresh event ID (a UUID v4 in production).
type IDGenerator func() string

// Writer is the subset of *kafkago.Writer this adapter depends on, so unit
// tests can substitute a fake without a real broker (mirrors
// inventory-storage's own outbound/kafka.Writer interface).
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// Publisher publishes domain events to warehouse.work-planning.events.
type Publisher struct {
	writer          Writer
	newID           IDGenerator
	workUnits       ports.WorkUnitRepo
	classifications ports.ProductClassificationLookup
}

// NewPublisher constructs a Publisher writing to TopicWorkPlanningEvents on
// brokers. workUnits is used to enrich WorkReleased events with the CPT and
// reference the published schema requires (the WorkReleased domain event
// itself only carries the work unit ID and path ID). classifications is
// used to enrich WorkReleased with derived hazmat-capability/fragile hints
// by looking up the released unit's SKU once at publish time (see
// ADR-0009); a nil classifications is treated exactly like
// productclassificationcopy.PermissiveLookup — those two optional fields are
// simply omitted/false, so every existing caller of NewPublisher keeps
// compiling and behaving unchanged.
//
// Balancer is kafkago.Hash (FNV-1a over Message.Key), not LeastBytes:
// kafka-go's Writer does not automatically route by key just because a
// Message carries one — LeastBytes balances purely by cumulative byte
// volume and ignores Message.Key entirely for partition placement. Hash
// is the balancer that actually gives "same Key always maps to the same
// partition", which every message this adapter builds relies on for
// per-aggregate ordering (see encodeCloudEvent, which keys every message
// by the aggregate id — work unit id or path id) now that warehouse-infra PR #42 scaled
// this topic from 1 to 8 partitions.
func NewPublisher(brokers []string, workUnits ports.WorkUnitRepo, classifications ports.ProductClassificationLookup, newID IDGenerator) *Publisher {
	return NewPublisherWithWriter(&kafkago.Writer{
		BatchTimeout:           syncWriterBatchTimeout,
		RequiredAcks:           syncWriterRequiredAcks,
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  cloudevents.TopicWorkPlanningEvents,
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	}, workUnits, classifications, newID)
}

// NewPublisherWithWriter builds a Publisher against an already-constructed
// Writer — the seam unit tests use to substitute a fake without a real
// broker; production code should use NewPublisher.
func NewPublisherWithWriter(writer Writer, workUnits ports.WorkUnitRepo, classifications ports.ProductClassificationLookup, newID IDGenerator) *Publisher {
	return &Publisher{
		writer:          writer,
		newID:           newID,
		workUnits:       workUnits,
		classifications: classifications,
	}
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

// Publish writes one CloudEvent per domain event inside a single
// "kafka.publish <topic>" producer span, injecting that span's W3C trace
// context into every message's headers so the consuming service continues
// the same distributed trace. It is Encode followed by WriteMessages.
func (p *Publisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
	if len(events) == 0 {
		return nil
	}

	ctx, span := otelkafka.StartPublishSpan(ctx, cloudevents.TopicWorkPlanningEvents,
		semconv.MessagingBatchMessageCount(len(events)),
	)
	defer span.End()

	encoded, err := p.Encode(ctx, events...)
	if err != nil {
		return recordErr(span, err)
	}

	msgs := make([]kafkago.Message, len(encoded))
	for i, e := range encoded {
		// The writer pins its Topic, so the message must not (kafka-go
		// rejects the combination); Encoded.Topic is for the relay.
		msgs[i] = e.message(false)
	}

	span.SetAttributes(attribute.StringSlice("messaging.event_types", eventTypes(events)))

	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return recordErr(span, err)
	}
	return nil
}

// Encode builds the integration-topic wire form of every event — one
// CloudEvents 1.0 event per domain event (ADR-0027), the enriched
// WorkReleased data payload (which READS the WorkUnit repo, so under the
// transactional outbox this must run inside the use case's transaction to
// see the just-saved row), the content-type header and the W3C trace
// headers of whatever span is active on ctx. It never touches the broker.
//
// The CloudEvents `id` is minted here exactly once per domain event; it is
// what the outbox persists and the relay republishes verbatim, so a
// redelivery carries the same id.
func (p *Publisher) Encode(ctx context.Context, events ...shared.DomainEvent) ([]Encoded, error) {
	out := make([]Encoded, 0, len(events))
	for _, e := range events {
		data, err := p.dataFor(ctx, e)
		if err != nil {
			return nil, err
		}
		enc, err := encodeCloudEvent(ctx, cloudevents.TopicWorkPlanningEvents, cloudevents.StreamEvents, p.newID(), e, subjectFor(e), data)
		if err != nil {
			return nil, err
		}
		out = append(out, enc)
	}
	return out, nil
}

// encodeCloudEvent builds one structured-mode CloudEvents Encoded for e on
// topic/stream. Key is the aggregate id — the same value as the CloudEvents
// subject (work unit id for WorkUnit events, path id otherwise), so every
// event of one aggregate lands on one partition (ADR-0024). The analytics
// publisher passes its own (identical) aggregate-id key via
// encodeCloudEventKeyed.
func encodeCloudEvent(ctx context.Context, topic, stream, id string, e shared.DomainEvent, subject string, data json.RawMessage) (Encoded, error) {
	key := subject
	if key == "" {
		// An event type with no aggregate id (none exist today) falls back
		// to the event id: still a valid, non-nil key.
		key = id
	}
	return encodeCloudEventKeyed(ctx, topic, stream, id, key, e, subject, data)
}

// encodeCloudEventKeyed is encodeCloudEvent with an explicit Kafka key.
func encodeCloudEventKeyed(ctx context.Context, topic, stream, id, key string, e shared.DomainEvent, subject string, data json.RawMessage) (Encoded, error) {
	entity := entityFor(e)
	body, err := cloudevents.New(cloudevents.Spec{
		ID:        id,
		Entity:    entity,
		EventName: e.EventName(),
		Subject:   subject,
		Time:      e.OccurredAt(),
		Stream:    stream,
		Version:   1,
		Data:      data,
	})
	if err != nil {
		return Encoded{}, err
	}

	msg := kafkago.Message{
		Key:     []byte(key),
		Value:   body,
		Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
	}
	otelkafka.Inject(ctx, &msg)
	return Encoded{
		Topic:     topic,
		EventType: cloudevents.Type(entity, e.EventName()),
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   msg.Headers,
	}, nil
}

// eventTypeEntity maps each domain event's bare EventName to the
// CloudEvents "type" entity segment apis/asyncapi.yaml documents for it
// (that message's own `tags` entry) — read straight off the spec, never
// invented. The same type is used on the integration and analytics topics.
var eventTypeEntity = map[string]string{
	"ChargeForecastReceived":   "charge",
	"ShiftPlanCommitted":       "plan",
	"WorkUnitCreated":          "workunit",
	"WorkReleased":             "workunit",
	"WorkUnitCompleted":        "workunit",
	"BacklogThresholdBreached": "workpool",
	"RateDeviationDetected":    "workpool",
	"PathThrottled":            "workpool",
	"LaborReassignmentFlagged": "workpool",
	"PathCapacityChanged":      "workpool",
	"PathPlanDriftDetected":    "pathplan",
}

// entityFor looks up e's CloudEvents type entity segment in
// eventTypeEntity. An event type with no documented entity (none exist
// today) falls back to "unknown" rather than panicking.
func entityFor(e shared.DomainEvent) string {
	if entity, ok := eventTypeEntity[e.EventName()]; ok {
		return entity
	}
	return "unknown"
}

// subjectFor returns the raising aggregate's instance id for the
// CloudEvents "subject" context attribute — the work unit id for
// WorkUnit-aggregate events, the path id for every other event type this
// adapter encodes — matching every worked example in apis/asyncapi.yaml.
func subjectFor(e shared.DomainEvent) string {
	switch ev := e.(type) {
	case shared.WorkUnitCreated:
		return ev.WorkUnitId
	case shared.WorkReleased:
		return ev.WorkUnitId
	case shared.WorkUnitCompleted:
		return ev.WorkUnitId
	case shared.ChargeForecastReceived:
		return ev.PathId.String()
	case shared.ShiftPlanCommitted:
		return ev.PathId.String()
	case shared.BacklogThresholdBreached:
		return ev.PathId.String()
	case shared.RateDeviationDetected:
		return ev.PathId.String()
	case shared.PathThrottled:
		return ev.PathId.String()
	case shared.LaborReassignmentFlagged:
		return ev.PathId.String()
	case shared.PathCapacityChanged:
		return ev.PathId.String()
	case shared.PathPlanDriftDetected:
		return ev.PathId.String()
	default:
		return ""
	}
}

// recordErr marks span failed and returns err unchanged, so instrumentation
// never alters the error the caller sees.
func recordErr(span trace.Span, err error) error {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	return err
}

func eventTypes(events []shared.DomainEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.EventName()
	}
	return out
}
