// Package kafka is the outbound Kafka adapter: it implements
// ports.EventPublisher on top of github.com/segmentio/kafka-go, serializing
// each domain event into the shared integration-event envelope and writing
// it to this service's own topic.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/envelope"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// hazmatTag and fragileTag are the ProductClassification.HandlingTags
// values inventory-storage uses that this service maps onto the
// WorkReleased integration event's derived hints (see ADR-0009). Named as
// constants here — not shared as a Go type across the repository boundary —
// because this is exactly the same translate-at-the-ACL discipline this
// adapter already applies to every other cross-service payload.
const (
	hazmatTag  = "Hazmat"
	fragileTag = "Fragile"
)

// EnvelopeMode selects which wire shape(s) Encode produces onto the
// integration topic (ADR-0021). The zero value is EnvelopeModeFlat, so a
// Publisher constructed without WithEnvelopeMode keeps writing today's
// envelope unchanged.
type EnvelopeMode string

const (
	// EnvelopeModeFlat emits only the legacy Envelope shape — today's
	// wire format, byte-identical. Default.
	EnvelopeModeFlat EnvelopeMode = "flat"
	// EnvelopeModeCloudEvents emits only the new CloudEvents 1.0 shape.
	EnvelopeModeCloudEvents EnvelopeMode = "cloudevents"
	// EnvelopeModeDual emits BOTH shapes as two physical Kafka messages
	// per domain event, same topic, same key.
	EnvelopeModeDual EnvelopeMode = "dual"
)

// ParseEnvelopeMode maps EVENT_ENVELOPE_MODE's raw env var value onto an
// EnvelopeMode, defaulting to EnvelopeModeFlat for an empty or unrecognized
// value — mirroring this fleet's existing *_MODE convention (e.g.
// PRODUCT_CLASSIFICATION_MODE, PATH_CATALOGUE_SOURCE), which fails soft to
// today's behavior rather than refusing to boot on a typo.
func ParseEnvelopeMode(raw string) EnvelopeMode {
	switch EnvelopeMode(raw) {
	case EnvelopeModeCloudEvents:
		return EnvelopeModeCloudEvents
	case EnvelopeModeDual:
		return EnvelopeModeDual
	default:
		return EnvelopeModeFlat
	}
}

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
	envelopeMode    EnvelopeMode
}

// PublisherOption customises a Publisher at construction time.
type PublisherOption func(*Publisher)

// WithEnvelopeMode sets which wire shape(s) the Publisher's Encode
// produces (ADR-0021). Omitting this option keeps EnvelopeModeFlat —
// today's byte-identical output — for every existing caller.
func WithEnvelopeMode(mode EnvelopeMode) PublisherOption {
	return func(p *Publisher) { p.envelopeMode = mode }
}

// NewPublisher constructs a Publisher writing to TopicWorkPlanningEvents on
// brokers. workUnits is used to enrich WorkReleased events with the CPT and
// reference the published schema requires (the WorkReleased domain event
// itself only carries the work unit ID and path ID). classifications is
// used to enrich WorkReleased with derived hazmat-capability/fragile hints
// by looking up the released unit's SKU once at publish time (see
// ADR-0009); a nil classifications is treated exactly like
// productclassification.PermissiveLookup — those two optional fields are
// simply omitted/false, so every existing caller of NewPublisher keeps
// compiling and behaving unchanged.
func NewPublisher(brokers []string, workUnits ports.WorkUnitRepo, classifications ports.ProductClassificationLookup, newID IDGenerator, opts ...PublisherOption) *Publisher {
	return NewPublisherWithWriter(&kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  envelope.TopicWorkPlanningEvents,
		Balancer:               &kafkago.LeastBytes{},
		AllowAutoTopicCreation: true,
	}, workUnits, classifications, newID, opts...)
}

// NewPublisherWithWriter builds a Publisher against an already-constructed
// Writer — the seam unit tests use to substitute a fake without a real
// broker; production code should use NewPublisher.
func NewPublisherWithWriter(writer Writer, workUnits ports.WorkUnitRepo, classifications ports.ProductClassificationLookup, newID IDGenerator, opts ...PublisherOption) *Publisher {
	p := &Publisher{
		writer:          writer,
		newID:           newID,
		workUnits:       workUnits,
		classifications: classifications,
		envelopeMode:    EnvelopeModeFlat,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

// Publish writes one envelope per domain event inside a single
// "kafka.publish <topic>" producer span, injecting that span's W3C trace
// context into every message's headers so the consuming service continues
// the same distributed trace. It is Encode followed by WriteMessages.
func (p *Publisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
	if len(events) == 0 {
		return nil
	}

	ctx, span := otelkafka.StartPublishSpan(ctx, envelope.TopicWorkPlanningEvents,
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

// Encode builds the integration-topic wire form of every event — the
// envelope, the enriched WorkReleased data payload (which READS the
// WorkUnit repo, so under the transactional outbox this must run inside
// the use case's transaction to see the just-saved row), and the W3C trace
// headers of whatever span is active on ctx. It never touches the broker.
//
// The wire SHAPE(S) built depend on p.envelopeMode (ADR-0021):
// EnvelopeModeFlat (default) emits one legacy Envelope-shaped message per
// event, byte-identical to this adapter's original output.
// EnvelopeModeCloudEvents emits one CloudEvents 1.0-shaped message per
// event instead. EnvelopeModeDual emits BOTH — two physical messages per
// event, same topic, same key (the event id) — so a dual-read-capable
// consumer's existing event_id-keyed idempotency gate silently no-ops
// whichever message arrives second.
func (p *Publisher) Encode(ctx context.Context, events ...shared.DomainEvent) ([]Encoded, error) {
	out := make([]Encoded, 0, len(events))
	for _, e := range events {
		data, err := p.dataFor(ctx, e)
		if err != nil {
			return nil, err
		}
		id := p.newID()

		switch p.envelopeMode {
		case EnvelopeModeCloudEvents:
			ce, err := p.encodeCloudEvent(ctx, e, id, data)
			if err != nil {
				return nil, err
			}
			out = append(out, ce)
		case EnvelopeModeDual:
			flat, err := p.encodeFlat(ctx, e, id, data)
			if err != nil {
				return nil, err
			}
			ce, err := p.encodeCloudEvent(ctx, e, id, data)
			if err != nil {
				return nil, err
			}
			out = append(out, flat, ce)
		default:
			flat, err := p.encodeFlat(ctx, e, id, data)
			if err != nil {
				return nil, err
			}
			out = append(out, flat)
		}
	}
	return out, nil
}

// encodeFlat builds one legacy envelope.Envelope-shaped Encoded for e,
// keyed and traced identically to this adapter's original (pre-ADR-0021)
// Encode. id is the event id minted once by the caller, shared with
// encodeCloudEvent in dual mode so both physical messages carry the same
// Kafka key.
func (p *Publisher) encodeFlat(ctx context.Context, e shared.DomainEvent, id string, data json.RawMessage) (Encoded, error) {
	env := envelope.Envelope{
		EventId:    id,
		EventType:  e.EventName(),
		OccurredAt: e.OccurredAt(),
		Source:     envelope.Source,
		Data:       data,
	}
	body, err := json.Marshal(env)
	if err != nil {
		return Encoded{}, err
	}

	msg := kafkago.Message{Key: []byte(env.EventId), Value: body}
	otelkafka.Inject(ctx, &msg)
	return Encoded{
		Topic:     envelope.TopicWorkPlanningEvents,
		EventType: env.EventType,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   msg.Headers,
	}, nil
}

// encodeCloudEvent builds one CloudEvents 1.0-shaped Encoded for e,
// matching apis/asyncapi.yaml's documented schema verbatim (ADR-0021). id
// and data are shared with encodeFlat in dual mode: the "data" payload is
// byte-identical either way, and both physical messages carry the same
// Kafka key.
func (p *Publisher) encodeCloudEvent(ctx context.Context, e shared.DomainEvent, id string, data json.RawMessage) (Encoded, error) {
	ce := envelope.CloudEvent{
		SpecVersion:     envelope.CloudEventsSpecVersion,
		Id:              id,
		Type:            cloudEventsType(entityFor(e), e.EventName()),
		Source:          envelope.SourceURI,
		Subject:         subjectFor(e),
		Time:            e.OccurredAt(),
		DataContentType: envelope.CloudEventsDataContentType,
		Data:            data,
	}
	body, err := json.Marshal(ce)
	if err != nil {
		return Encoded{}, err
	}

	msg := kafkago.Message{Key: []byte(ce.Id), Value: body}
	otelkafka.Inject(ctx, &msg)
	return Encoded{
		Topic:     envelope.TopicWorkPlanningEvents,
		EventType: e.EventName(),
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   msg.Headers,
	}, nil
}

// cloudEventsSubdomain and cloudEventsBoundedContext are the two fixed
// segments of this service's CloudEvents "type" convention — already named
// in apis/asyncapi.yaml's "type naming convention" section, reused verbatim
// here rather than re-derived.
const (
	cloudEventsSubdomain      = "wes"
	cloudEventsBoundedContext = "work-planning"
)

// cloudEventsType builds the reverse-DNS CloudEvents "type" context
// attribute apis/asyncapi.yaml already documents:
// com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>. A pure
// function so it is trivially unit-testable independent of any Publisher.
func cloudEventsType(entity, eventName string) string {
	return fmt.Sprintf("com.warehouse.%s.%s.%s.%s", cloudEventsSubdomain, cloudEventsBoundedContext, entity, eventName)
}

// eventTypeEntity maps each domain event's bare EventName to the
// CloudEvents "type" entity segment apis/asyncapi.yaml already documents
// for it (that message's own `tags` entry) — read straight off the spec,
// never invented.
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
}

// entityFor looks up e's CloudEvents type entity segment in
// eventTypeEntity. An event type with no documented entity (none exist
// today, but this keeps cloudevents mode fail-soft rather than panicking on
// a future undocumented event type) falls back to "unknown".
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

// dataFor builds the event-type-specific "data" payload by dispatching to
// the per-event-type payload builder. Only WorkReleased has a documented
// downstream schema; other event types get a best-effort payload of their
// own fields.
func (p *Publisher) dataFor(ctx context.Context, e shared.DomainEvent) (json.RawMessage, error) {
	switch ev := e.(type) {
	case shared.WorkReleased:
		return p.workReleasedData(ctx, ev)
	case shared.ChargeForecastReceived:
		return pathOnlyData(ev.PathId)
	case shared.ShiftPlanCommitted:
		return pathOnlyData(ev.PathId)
	case shared.WorkUnitCreated:
		return workUnitData(ev.WorkUnitId, ev.PathId)
	case shared.BacklogThresholdBreached:
		return pathOnlyData(ev.PathId)
	case shared.RateDeviationDetected:
		return pathOnlyData(ev.PathId)
	case shared.PathThrottled:
		return pathOnlyData(ev.PathId)
	case shared.LaborReassignmentFlagged:
		return pathOnlyData(ev.PathId)
	case shared.WorkUnitCompleted:
		return workUnitData(ev.WorkUnitId, ev.PathId)
	case shared.PathCapacityChanged:
		return pathCapacityChangedData(ev)
	default:
		return json.Marshal(map[string]any{})
	}
}

// workReleasedData builds the WorkReleased payload: the path/work-unit
// identity plus the CPT, reference and gift-wrap characteristics READ off
// the WorkUnit repo at encode time, and the strictly-additive
// classification hints (ADR-0009/0010).
func (p *Publisher) workReleasedData(ctx context.Context, ev shared.WorkReleased) (json.RawMessage, error) {
	cpt, ref, sku, giftWrap := "", "", "", false
	if unit, err := p.workUnits.FindById(ctx, ev.WorkUnitId); err == nil {
		cpt = unit.CPT().Time().Format(time.RFC3339)
		ref = unit.Reference()
		sku = unit.SKU()
		giftWrap = unit.GiftWrap()
	}

	requiredCapabilities, fragile := p.classificationHints(ctx, sku)

	data := map[string]any{
		"path_id":      ev.PathId.String(),
		"work_unit_id": ev.WorkUnitId,
		"cpt":          cpt,
		"ref":          ref,
	}
	// Strictly additive and backward compatible: only set these two
	// optional fields when there is something to say. An unclassified
	// SKU or an unavailable lookup omits them entirely rather than
	// publishing an empty array / explicit false, so a consumer that
	// already treats "absent" as "no hint" (fulfillment-execution's
	// documented default) sees no difference from before this feature
	// existed.
	if len(requiredCapabilities) > 0 {
		data["required_capabilities"] = requiredCapabilities
	}
	if fragile {
		data["fragile"] = fragile
	}
	// gift_wrap is a caller-stated WorkReleased characteristic, not a
	// classification hint — read straight off the WorkUnit (like
	// cpt/ref), never derived from classificationHints (see ADR-0010).
	// Same omit-when-false discipline as fragile.
	if giftWrap {
		data["gift_wrap"] = giftWrap
	}
	return json.Marshal(data)
}

// pathOnlyData is the best-effort payload for event types that carry only
// their process-path identity.
func pathOnlyData(pathId shared.PathId) (json.RawMessage, error) {
	return json.Marshal(map[string]any{"path_id": pathId.String()})
}

// workUnitData is the best-effort payload for event types that carry a
// work-unit identity alongside their process path.
func workUnitData(workUnitId string, pathId shared.PathId) (json.RawMessage, error) {
	return json.Marshal(map[string]any{"path_id": pathId.String(), "work_unit_id": workUnitId})
}

// pathCapacityChangedData is the PathCapacityChanged payload consumed by
// order-management (ADR-0018).
func pathCapacityChangedData(ev shared.PathCapacityChanged) (json.RawMessage, error) {
	return json.Marshal(map[string]any{
		"path_id":         ev.PathId.String(),
		"cutoff_at":       ev.CutoffAt.Format(time.RFC3339),
		"remaining_units": ev.RemainingUnits,
		"known":           ev.Known,
	})
}

// classificationHints looks up sku's ProductClassification once, at
// publish time, and derives the two optional WorkReleased hints from it:
// "hazmat" appended to requiredCapabilities when the SKU is classified
// Hazmat, and fragile=true when it is classified Fragile. This is the
// concrete implementation of ADR-0009's "read-once-at-release, stamp onto
// WorkReleased" decision — fulfillment-execution's Task then carries these
// hints without ever calling back to inventory-storage.
//
// A missing sku, a nil classifications port, an unclassified SKU (Known
// but no relevant tag, or altogether unknown), or a lookup error are all
// treated identically: no hints. This is deliberately permissive/fail-open
// — unlike inventory-storage's own StowStock placement check, a
// classification-lookup problem here must never block or delay releasing
// work; it can only omit an optional enrichment.
func (p *Publisher) classificationHints(ctx context.Context, sku string) (requiredCapabilities []string, fragile bool) {
	if sku == "" || p.classifications == nil {
		return nil, false
	}

	view, err := p.classifications.GetClassification(ctx, sku)
	if err != nil || !view.Known {
		return nil, false
	}

	if view.HasTag(hazmatTag) {
		requiredCapabilities = append(requiredCapabilities, "hazmat")
	}
	fragile = view.HasTag(fragileTag)
	return requiredCapabilities, fragile
}
