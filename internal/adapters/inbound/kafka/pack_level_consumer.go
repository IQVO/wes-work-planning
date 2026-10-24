package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// packDimsData is product-master's PackDimensions: all four members are
// required there, and any missing one here is poison.
type packDimsData struct {
	LengthMm int `json:"length_mm"`
	WidthMm  int `json:"width_mm"`
	HeightMm int `json:"height_mm"`
	WeightG  int `json:"weight_g"`
}

func (d packDimsData) dims() productclassificationview.Dims {
	return productclassificationview.Dims{LengthMm: d.LengthMm, WidthMm: d.WidthMm, HeightMm: d.HeightMm, WeightG: d.WeightG}
}

// valid reports the PackDimensions schema: every member at least 1.
func (d packDimsData) valid() bool {
	return d.LengthMm >= 1 && d.WidthMm >= 1 && d.HeightMm >= 1 && d.WeightG >= 1
}

// productPackLevelDeclaredData is product-master's ProductPackLevelDeclared
// payload (the CloudEvents `data` object), mirrored read-only from its
// apis/asyncapi.yaml: the FULL level after a change (snake_case, as shipped).
type productPackLevelDeclaredData struct {
	SKU            string        `json:"sku"`
	Level          string        `json:"level"`
	GTIN           string        `json:"gtin"`
	ChildGTIN      string        `json:"child_gtin,omitempty"`
	QtyOfChild     int           `json:"qty_of_child,omitempty"`
	UnitsPerPack   int           `json:"units_per_pack"`
	Dims           *packDimsData `json:"dims,omitempty"`
	IsOrderable    bool          `json:"is_orderable"`
	IsDespatchUnit bool          `json:"is_despatch_unit"`
	EffectiveFrom  time.Time     `json:"effective_from"`
	Version        int64         `json:"version"`
}

// productPackLevelRetiredData is ProductPackLevelRetired: the level is
// end-dated, its GTIN stays known.
type productPackLevelRetiredData struct {
	SKU         string    `json:"sku"`
	Level       string    `json:"level"`
	GTIN        string    `json:"gtin"`
	EffectiveTo time.Time `json:"effective_to"`
	Reason      string    `json:"reason"`
	Version     int64     `json:"version"`
}

// productPackLevelMeasuredData is ProductPackLevelMeasured.
type productPackLevelMeasuredData struct {
	SKU        string       `json:"sku"`
	Level      string       `json:"level"`
	GTIN       string       `json:"gtin"`
	Dims       packDimsData `json:"dims"`
	MeasuredAt time.Time    `json:"measured_at"`
	Version    int64        `json:"version"`
}

// errBadPackLevelPayload marks a CloudEvent of a handled type whose `data`
// cannot be decoded or is invalid: deterministic poison, never retried.
var errBadPackLevelPayload = errors.New("malformed pack level event payload")

// packGTINLen is the length of the 14-digit zero-padded GTIN (contracts G2).
const packGTINLen = 14

func validGTIN(g string) bool {
	if len(g) != packGTINLen {
		return false
	}
	for _, r := range g {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (d productPackLevelDeclaredData) record() (productclassificationview.PackLevelRecord, error) {
	level := productclassificationview.PackLevel(d.Level)
	switch {
	case d.SKU == "":
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: empty sku", errBadPackLevelPayload)
	case !level.Valid():
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: level %q", errBadPackLevelPayload, d.Level)
	case !validGTIN(d.GTIN):
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: gtin %q is not 14 digits", errBadPackLevelPayload, d.GTIN)
	case d.UnitsPerPack < 1:
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: units_per_pack %d < 1", errBadPackLevelPayload, d.UnitsPerPack)
	case d.Version < 1:
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: version %d < 1", errBadPackLevelPayload, d.Version)
	case d.EffectiveFrom.IsZero():
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: effective_from is required", errBadPackLevelPayload)
	case d.ChildGTIN != "" && !validGTIN(d.ChildGTIN):
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: child_gtin %q is not 14 digits", errBadPackLevelPayload, d.ChildGTIN)
	case d.Dims != nil && !d.Dims.valid():
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: dims need every member >= 1", errBadPackLevelPayload)
	}
	rec := productclassificationview.PackLevelRecord{
		SKU: d.SKU, Level: level, GTIN: d.GTIN, ChildGTIN: d.ChildGTIN, QtyOfChild: d.QtyOfChild,
		UnitsPerPack: d.UnitsPerPack, IsOrderable: d.IsOrderable, IsDespatch: d.IsDespatchUnit,
		EffectiveFrom: d.EffectiveFrom, Version: d.Version,
	}
	if d.Dims != nil {
		rec.Declared = d.Dims.dims()
	}
	return rec, nil
}

// PackLevelConsumer feeds this context's local pack-level copy from
// product-master's warehouse.product-master.events (ADR-0045).
//
//   - It dispatches on the FULL CloudEvents type: ProductPackLevelDeclared,
//     ProductPackLevelRetired and ProductPackLevelMeasured are applied; every
//     other type on the topic (ProductClassified, ProductRegistered, ...) is
//     committed past untouched.
//   - A message that is not a valid CloudEvent is WARN-logged and committed
//     past, never parsed as a legacy shape. A handled type whose payload is
//     undecodable or invalid is POISON: the raw message goes to
//     "<topic>.dlq" at once and the offset is committed, so it never blocks
//     the partition.
//   - Applying goes through usecases.ObservePackLevel (CloudEvents id claim +
//     version-guarded write, ONE UnitOfWork, ADR-0028). Any other error is
//     transient: the SAME message is retried with capped backoff until it
//     succeeds, because dropping a pack event would leave the copy stale.
//   - FetchMessage + CommitMessages after success; stopping mid-retry returns
//     without committing, so the message is redelivered.
//
// It runs under its own STABLE group (PACK_VIEW_CONSUMER_GROUP): a group that
// already committed past the pack events (the classification group) could not
// replay them, and a new group starts at the earliest offset so the copy
// fills from history.
type PackLevelConsumer struct {
	reader       messageReader
	dlq          dlqWriter
	topic        string
	observe      *usecases.ObservePackLevel
	logger       *slog.Logger
	retryInitial time.Duration
	retryMax     time.Duration
}

// packLevelReaderMaxWait bounds how long the reader waits for a full batch,
// so a single new event is seen within half a second.
const packLevelReaderMaxWait = 500 * time.Millisecond

// NewPackLevelConsumer reads cloudevents.TopicProductMasterEvents under
// groupID, read by cmd/wes from PACK_VIEW_CONSUMER_GROUP (never a literal).
func NewPackLevelConsumer(brokers []string, groupID string, observe *usecases.ObservePackLevel, logger *slog.Logger) *PackLevelConsumer {
	return NewPackLevelConsumerForTopic(brokers, groupID, cloudevents.TopicProductMasterEvents, observe, logger)
}

// NewPackLevelConsumerForTopic is NewPackLevelConsumer with the topic
// overridden, so integration tests can use an isolated topic (and with it an
// isolated "<topic>.dlq").
func NewPackLevelConsumerForTopic(brokers []string, groupID, topic string, observe *usecases.ObservePackLevel, logger *slog.Logger) *PackLevelConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MaxWait:     packLevelReaderMaxWait,
		StartOffset: kafkago.FirstOffset,
	})
	return newPackLevelConsumer(reader, newDLQWriter(brokers, topic), topic, observe, logger)
}

func newPackLevelConsumer(reader messageReader, dlq dlqWriter, topic string, observe *usecases.ObservePackLevel, logger *slog.Logger) *PackLevelConsumer {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &PackLevelConsumer{
		reader: reader, dlq: dlq, topic: topic, observe: observe, logger: logger,
		retryInitial: classificationRetryInitial, retryMax: classificationRetryMax,
	}
}

// Close releases the reader and the dead-letter writer.
func (c *PackLevelConsumer) Close() error {
	return errors.Join(c.reader.Close(), c.dlq.Close())
}

// Run consumes until ctx is cancelled (returning nil) or the reader or the
// dead-letter topic fails.
func (c *PackLevelConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("pack level consumer: fetch: %w", err)
		}
		if err := c.settle(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("pack level consumer: commit: %w", err)
		}
	}
}

// settle handles msg; poison is dead-lettered. It returns nil when msg may be
// committed, ctx's error when shutdown interrupted a retry, and an error when
// the dead-letter publish itself failed (the message must then NOT be
// committed).
func (c *PackLevelConsumer) settle(ctx context.Context, msg kafkago.Message) error {
	err := c.handle(ctx, msg)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.logger.ErrorContext(ctx, "pack level event is poison, sending to dead-letter topic",
		"topic", c.topic, "partition", msg.Partition, "offset", msg.Offset, "dlq_topic", c.topic+dlqTopicSuffix, "error", err)
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.topic)},
		kafkago.Header{Key: "x-dlq-source-partition", Value: []byte(strconv.Itoa(msg.Partition))},
		kafkago.Header{Key: "x-dlq-source-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(err.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	if dlqErr := writeDLQ(ctx, c.dlq, kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers}); dlqErr != nil {
		return fmt.Errorf("pack level consumer: publish to dead-letter topic: %w", dlqErr)
	}
	return nil
}

// handle processes one message inside a "kafka.consume <topic>" span. It
// returns nil when the message may be committed (applied, stale, redelivered,
// ignored or skipped), errBadPackLevelPayload-wrapped poison to dead-letter,
// and ctx's error when shutdown interrupted a transient retry.
func (c *PackLevelConsumer) handle(ctx context.Context, msg kafkago.Message) error {
	msgCtx, span := otelkafka.StartConsumeSpan(otelkafka.Extract(ctx, &msg), c.topic,
		semconv.MessagingKafkaOffset(int(msg.Offset)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(msg.Partition)),
	)
	defer span.End()

	env, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.logger.WarnContext(msgCtx, "skipping a message that is not a valid CloudEvent on the product-master topic",
			"topic", c.topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	span.SetAttributes(attribute.String("messaging.message.id", env.ID()), attribute.String("cloudevents.event_type", env.Type()))

	req, ok, err := c.requestFor(env)
	if err != nil {
		recordSpanError(span, err)
		return err
	}
	if !ok {
		return nil
	}
	if err := c.applyWithRetry(msgCtx, req); err != nil {
		recordSpanError(span, err)
		return err
	}
	return nil
}

// requestFor maps a CloudEvent to the use-case request; ok=false for a type
// this consumer ignores.
func (c *PackLevelConsumer) requestFor(env ce.Event) (usecases.ObservePackLevelRequest, bool, error) {
	observedAt := env.Time()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	req := usecases.ObservePackLevelRequest{EventId: env.ID(), ObservedAt: observedAt}
	var err error
	switch env.Type() {
	case cloudevents.TypeProductPackLevelDeclared:
		req.Event = usecases.PackLevelDeclared
		req.Record, err = declaredRecord(env)
	case cloudevents.TypeProductPackLevelRetired:
		req.Event = usecases.PackLevelRetired
		req.Record, err = retiredRecord(env)
	case cloudevents.TypeProductPackLevelMeasured:
		req.Event = usecases.PackLevelMeasured
		req.Record, err = measuredRecord(env)
	default:
		return req, false, nil
	}
	return req, err == nil, err
}

func declaredRecord(env ce.Event) (productclassificationview.PackLevelRecord, error) {
	var d productPackLevelDeclaredData
	if err := env.DataAs(&d); err != nil {
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: %v", errBadPackLevelPayload, err)
	}
	return d.record()
}

func retiredRecord(env ce.Event) (productclassificationview.PackLevelRecord, error) {
	var d productPackLevelRetiredData
	if err := env.DataAs(&d); err != nil {
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: %v", errBadPackLevelPayload, err)
	}
	if !validGTIN(d.GTIN) || d.Version < 1 || d.EffectiveTo.IsZero() {
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: retired needs a 14-digit gtin, effective_to and version >= 1", errBadPackLevelPayload)
	}
	return productclassificationview.PackLevelRecord{SKU: d.SKU, GTIN: d.GTIN, EffectiveTo: d.EffectiveTo, Version: d.Version}, nil
}

func measuredRecord(env ce.Event) (productclassificationview.PackLevelRecord, error) {
	var d productPackLevelMeasuredData
	if err := env.DataAs(&d); err != nil {
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: %v", errBadPackLevelPayload, err)
	}
	if !validGTIN(d.GTIN) || d.Version < 1 || !d.Dims.valid() {
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("%w: measured needs a 14-digit gtin, version >= 1 and dims with every member >= 1", errBadPackLevelPayload)
	}
	return productclassificationview.PackLevelRecord{SKU: d.SKU, GTIN: d.GTIN, Measured: d.Dims.dims(), Version: d.Version}, nil
}

// applyWithRetry runs the use case until it succeeds, backing off between
// attempts; it returns ctx's error if the consumer stops first.
func (c *PackLevelConsumer) applyWithRetry(ctx context.Context, req usecases.ObservePackLevelRequest) error {
	delay := c.retryInitial
	for attempt := 1; ; attempt++ {
		outcome, err := c.observe.Execute(ctx, req)
		if err == nil {
			c.logger.DebugContext(ctx, "pack level event handled",
				"id", req.EventId, "gtin", req.Record.GTIN, "outcome", outcome.String())
			return nil
		}
		c.logger.ErrorContext(ctx, "applying a pack level event failed; retrying the same message",
			"id", req.EventId, "gtin", req.Record.GTIN, "attempt", attempt, "retry_in", delay.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, c.retryMax)
	}
}
