package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/wes-work-planning/internal/analytics/report"
)

// AnalyticsConsumerGroup is the Kafka consumer group the analytics projector
// reads under. It is distinct from the OLTP consumer group so the two
// pipelines track their offsets independently.
const AnalyticsConsumerGroup = "wes-analytics"

// Full CloudEvents `type` strings of this service's own analytics events
// the projector applies. Same type on the analytics and integration topics;
// the dataschema (urn:warehouse:wes-work-planning:analytics:<Event>:v1)
// names the analytics payload shape. Declared here rather than imported
// from the outbound publisher so this inbound adapter does not depend on
// an outbound adapter.
var (
	typeWorkReleased             = cloudevents.Type("workunit", "WorkReleased")
	typeWorkUnitCompleted        = cloudevents.Type("workunit", "WorkUnitCompleted")
	typeBacklogThresholdBreached = cloudevents.Type("workpool", "BacklogThresholdBreached")
	typePathThrottled            = cloudevents.Type("workpool", "PathThrottled")
	typeRateDeviationDetected    = cloudevents.Type("workpool", "RateDeviationDetected")
)

// analyticsData is the union of fields the projecting event payloads carry.
// Every event this report is built from carries its own path_id.
type analyticsData struct {
	PathId     string `json:"path_id"`
	WorkUnitId string `json:"work_unit_id"`
}

// AnalyticsConsumer reads analytics events off the analytics topic and
// applies each to the throughput ProjectionStore, exactly once per CloudEvents id
// despite Kafka's at-least-once delivery.
type AnalyticsConsumer struct {
	Reader     *kafkago.Reader
	Projection report.ProjectionStore
	Processed  report.ProcessedEvents
	Logger     *slog.Logger
}

// NewAnalyticsConsumer constructs an AnalyticsConsumer reading topic from
// brokers under AnalyticsConsumerGroup.
func NewAnalyticsConsumer(brokers []string, topic string, projection report.ProjectionStore, processed report.ProcessedEvents, logger *slog.Logger) *AnalyticsConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: AnalyticsConsumerGroup,
		// Start a brand-new consumer group at the EARLIEST offset. The
		// analytics projection must see the full history of the topic (it is a
		// replayable read model, not a live integration reaction), so a fresh
		// projector — or a backfill into a new group — reads from the
		// beginning rather than kafka-go's default of the latest offset, which
		// would silently drop every event produced before the group first
		// committed an offset. Once the group has committed offsets, those
		// take precedence and this only affects the first join.
		StartOffset: kafkago.FirstOffset,
	})
	return &AnalyticsConsumer{Reader: reader, Projection: projection, Processed: processed, Logger: logger}
}

// Run reads and handles messages until ctx is cancelled or the reader
// returns a fatal error. A handling error is logged and the loop continues
// so one bad message cannot wedge the projector.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.Handle(ctx, msg); err != nil {
			if errors.Is(err, cloudevents.ErrNotCloudEvent) {
				// Deterministic poison message (e.g. the retired flat
				// envelope): skip it — ReadMessage already committed.
				c.Logger.WarnContext(ctx, "skipping invalid CloudEvent on analytics topic",
					"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
				continue
			}
			c.Logger.ErrorContext(ctx, "analytics message handling failed", "error", err)
		}
	}
}

// Close releases the underlying Kafka reader.
func (c *AnalyticsConsumer) Close() error {
	return c.Reader.Close()
}

// Handle processes one consumed message inside a "kafka.consume <topic>"
// span whose parent is the producer's span, read from the message headers.
// It is exported separately from Run so the propagation can be tested without
// a live broker.
func (c *AnalyticsConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	msgCtx, span := otelkafka.StartConsumeSpan(otelkafka.Extract(ctx, &msg), msg.Topic,
		semconv.MessagingKafkaOffset(int(msg.Offset)),
		attribute.Int("messaging.kafka.partition", msg.Partition),
	)
	defer span.End()

	if err := c.HandleMessage(msgCtx, msg.Value); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// HandleMessage decodes raw as a CloudEvents 1.0 event and applies the
// matching projection method for its full `type`. Event types outside the
// projection contract are ignored (and not marked processed). For a
// projecting event it dedupes on the CloudEvents id via ProcessedEvents
// before applying, so a redelivery is a no-op. A message that is not a
// valid CloudEvent returns an error wrapping cloudevents.ErrNotCloudEvent
// and is never parsed as a legacy shape. It is exported separately from Run
// so tests can feed raw events without a live broker.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, raw []byte) error {
	evt, err := cloudevents.Decode(raw)
	if err != nil {
		return fmt.Errorf("analytics: decode event: %w", err)
	}

	// Only the five throughput-moving events project; everything else
	// (WorkUnitCreated, ChargeForecastReceived, ShiftPlanCommitted,
	// LaborReassignmentFlagged, PathCapacityChanged, PathPlanDriftDetected) is acknowledged without
	// touching the read model or the processed set.
	switch evt.Type() {
	case typeWorkReleased, typeWorkUnitCompleted, typeBacklogThresholdBreached, typePathThrottled, typeRateDeviationDetected:
	default:
		return nil
	}

	isNew, err := c.Processed.MarkProcessed(ctx, evt.ID())
	if err != nil {
		return fmt.Errorf("analytics: mark processed: %w", err)
	}
	if !isNew {
		return nil
	}

	var data analyticsData
	if err := evt.DataAs(&data); err != nil {
		return fmt.Errorf("analytics: decode data: %w", err)
	}

	id, at := evt.ID(), evt.Time()
	switch evt.Type() {
	case typeWorkReleased:
		return c.Projection.ApplyWorkReleased(ctx, id, data.PathId, at)
	case typeWorkUnitCompleted:
		return c.Projection.ApplyWorkUnitCompleted(ctx, id, data.PathId, at)
	case typeBacklogThresholdBreached:
		return c.Projection.ApplyBacklogThresholdBreached(ctx, id, data.PathId, at)
	case typePathThrottled:
		return c.Projection.ApplyPathThrottled(ctx, id, data.PathId, at)
	case typeRateDeviationDetected:
		return c.Projection.ApplyRateDeviationDetected(ctx, id, data.PathId, at)
	default:
		return nil
	}
}
