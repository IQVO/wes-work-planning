package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// AnalyticsTopic is the dedicated topic the analytics data product consumes.
// It is separate from the integration topic (cloudevents.TopicWorkPlanningEvents)
// so the OLTP integration contract and the analytical read-model stream evolve
// independently (ADR-0011). Every message on it is a CloudEvents 1.0 event
// whose dataschema is urn:warehouse:wes-work-planning:analytics:<Event>:v1.
const AnalyticsTopic = cloudevents.TopicAnalytics

// AnalyticsPublisher publishes every wes-work-planning domain event onto
// AnalyticsTopic as a CloudEvents 1.0 event. It satisfies ports.EventPublisher
// and is a SEPARATE adapter from Publisher: the integration publisher
// (publisher.go) and warehouse.work-planning.events are left untouched.
//
// Every event this report is built from already carries its own PathId (the
// aggregate key), so no repo lookup is needed to populate the report's path
// dimension — the publisher stays thin (contrast the fulfillment pilot, whose
// task-scoped events needed a TaskRepo lookup to recover task_type; see
// ADR-0011).
type AnalyticsPublisher struct {
	writer Writer
	newID  IDGenerator
}

// NewAnalyticsPublisher constructs an AnalyticsPublisher writing to
// AnalyticsTopic on brokers. newID mints the CloudEvents id.
//
// Balancer is kafkago.Hash, matching Publisher.NewPublisher's choice (see
// its doc comment): this publisher already keys every message by its
// aggregate id (marshalAnalyticsData), but LeastBytes would silently
// discard that key for partition routing — Hash is what actually turns
// the key into a same-aggregate-same-partition guarantee now that
// AnalyticsTopic has more than one partition.
func NewAnalyticsPublisher(brokers []string, newID IDGenerator) *AnalyticsPublisher {
	return NewAnalyticsPublisherWithWriter(&kafkago.Writer{
		BatchTimeout:           syncWriterBatchTimeout,
		RequiredAcks:           syncWriterRequiredAcks,
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  AnalyticsTopic,
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	}, newID)
}

// NewAnalyticsPublisherWithWriter builds an AnalyticsPublisher against an
// already-constructed Writer — the seam unit tests use to substitute a fake
// without a real broker; production code should use NewAnalyticsPublisher.
func NewAnalyticsPublisherWithWriter(writer Writer, newID IDGenerator) *AnalyticsPublisher {
	return &AnalyticsPublisher{writer: writer, newID: newID}
}

// Close releases the underlying Kafka writer.
func (p *AnalyticsPublisher) Close() error {
	return p.writer.Close()
}

// Publish emits every event in events onto AnalyticsTopic. Events with no
// analytics payload (an unrecognised type) are skipped rather than erroring,
// so the caller can hand it the full event stream indiscriminately. It is
// Encode followed by WriteMessages.
func (p *AnalyticsPublisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
	if len(events) == 0 {
		return nil
	}

	ctx, span := otelkafka.StartPublishSpan(ctx, AnalyticsTopic,
		semconv.MessagingBatchMessageCount(len(events)),
	)
	defer span.End()

	encoded, err := p.Encode(ctx, events...)
	if err != nil {
		return recordErr(span, err)
	}
	if len(encoded) == 0 {
		return nil
	}

	msgs := make([]kafkago.Message, len(encoded))
	emitted := make([]string, len(encoded))
	for i, e := range encoded {
		// The writer pins its Topic, so the message must not (kafka-go
		// rejects the combination); Encoded.Topic is for the relay.
		msgs[i] = e.message(false)
		emitted[i] = e.EventType
	}

	span.SetAttributes(attribute.StringSlice("messaging.event_types", emitted))

	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return recordErr(span, fmt.Errorf("kafka: publish analytics events: %w", err))
	}
	return nil
}

// Encode builds the analytics-topic wire form of every event in the
// analytics contract — CloudEvent, aggregate-id key, content-type header, W3C trace headers of
// whatever span is active on ctx — and silently drops the rest, so the
// returned slice may be shorter than events. It never touches the broker.
func (p *AnalyticsPublisher) Encode(ctx context.Context, events ...shared.DomainEvent) ([]Encoded, error) {
	out := make([]Encoded, 0, len(events))
	for _, e := range events {
		key, data, ok := marshalAnalyticsData(e)
		if !ok {
			continue
		}
		// subject == key: both are the aggregate id.
		enc, err := encodeCloudEventKeyed(ctx, AnalyticsTopic, cloudevents.StreamAnalytics, p.newID(), key, e, key, data)
		if err != nil {
			return nil, fmt.Errorf("kafka: encode analytics event: %w", err)
		}
		out = append(out, enc)
	}
	return out, nil
}

// marshalAnalyticsData maps a domain event to its aggregate-id message key
// (also the CloudEvents subject) and snake_case JSON payload. The bool return is
// false for an event type outside the analytics contract, so Publish skips
// it. The message key is the aggregate id: PathId for path-scoped events and
// the WorkUnit id for work-unit events, so a partition holds an aggregate's
// events in order.
func marshalAnalyticsData(e shared.DomainEvent) (key string, data json.RawMessage, ok bool) {
	switch ev := e.(type) {
	case shared.WorkReleased:
		return ev.WorkUnitId, mustMarshal(map[string]any{
			"path_id":      ev.PathId.String(),
			"work_unit_id": ev.WorkUnitId,
		}), true
	case shared.WorkUnitCompleted:
		return ev.WorkUnitId, mustMarshal(map[string]any{
			"path_id":      ev.PathId.String(),
			"work_unit_id": ev.WorkUnitId,
		}), true
	case shared.WorkUnitCreated:
		return ev.WorkUnitId, mustMarshal(map[string]any{
			"path_id":      ev.PathId.String(),
			"work_unit_id": ev.WorkUnitId,
		}), true
	case shared.BacklogThresholdBreached:
		return ev.PathId.String(), mustMarshal(map[string]any{
			"path_id": ev.PathId.String(),
		}), true
	case shared.PathThrottled:
		return ev.PathId.String(), mustMarshal(map[string]any{
			"path_id": ev.PathId.String(),
		}), true
	case shared.RateDeviationDetected:
		return ev.PathId.String(), mustMarshal(map[string]any{
			"path_id": ev.PathId.String(),
		}), true
	case shared.ChargeForecastReceived:
		return ev.PathId.String(), mustMarshal(map[string]any{
			"path_id": ev.PathId.String(),
		}), true
	case shared.ShiftPlanCommitted:
		return ev.PathId.String(), mustMarshal(map[string]any{
			"path_id": ev.PathId.String(),
		}), true
	case shared.LaborReassignmentFlagged:
		return ev.PathId.String(), mustMarshal(map[string]any{
			"path_id": ev.PathId.String(),
		}), true
	case shared.PathCapacityChanged:
		return ev.PathId.String(), mustMarshal(map[string]any{
			"path_id":         ev.PathId.String(),
			"cutoff_at":       ev.CutoffAt.Format(time.RFC3339),
			"remaining_units": ev.RemainingUnits,
			"known":           ev.Known,
		}), true
	default:
		return "", nil, false
	}
}

// mustMarshal marshals a map whose shape is fully controlled by
// marshalAnalyticsData, so an error here is a programming mistake rather than
// a runtime condition.
func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("kafka: marshal analytics data: %v", err))
	}
	return b
}

// Compile-time assertion that AnalyticsPublisher satisfies the outbound
// event-publishing port.
var _ ports.EventPublisher = (*AnalyticsPublisher)(nil)
