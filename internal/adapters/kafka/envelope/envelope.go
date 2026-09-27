// Package envelope defines the integration-event wire format shared by
// every warehouse-systems service: a fixed envelope with an event-type-
// specific JSON payload. Used by both the outbound and inbound Kafka
// adapters so they agree on the wire format without depending on each
// other.
package envelope

import (
	"encoding/json"
	"time"
)

// Envelope is the identical outer shape published/consumed across all
// warehouse-systems services.
type Envelope struct {
	EventId    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Source     string          `json:"source"`
	Data       json.RawMessage `json:"data"`
}

// Source identifies this service in the "source" field of every envelope
// it publishes.
const Source = "wes-work-planning"

// CloudEvent is the CloudEvents 1.0 structured envelope shape this service
// migrates the integration topic to (ADR-0021), matching
// apis/asyncapi.yaml's already-documented target verbatim. Field-for-field
// mapping against the legacy Envelope above: Id==EventId, Time==OccurredAt,
// Data is byte-identical either way. Subject and SpecVersion have no flat
// equivalent -- SpecVersion in particular is the sole dual-read
// discriminator (its presence/absence is how a consumer tells the two
// shapes apart), so it must never appear on a flat-shaped Envelope.
type CloudEvent struct {
	SpecVersion     string          `json:"specversion"`
	Id              string          `json:"id"`
	Type            string          `json:"type"`
	Source          string          `json:"source"`
	Subject         string          `json:"subject"`
	Time            time.Time       `json:"time"`
	DataContentType string          `json:"datacontenttype"`
	Data            json.RawMessage `json:"data"`
}

// SourceURI is the CloudEvents "source" context attribute this service
// stamps onto every CloudEvents-shaped envelope it publishes -- the URI
// form already used throughout apis/asyncapi.yaml's worked examples.
const SourceURI = "/warehouse/wes-work-planning"

// CloudEventsSpecVersion is the fixed CloudEvents 1.0 "specversion" value.
const CloudEventsSpecVersion = "1.0"

// CloudEventsDataContentType is the fixed "datacontenttype" value: the
// inner `data` payload is always application/json (the outer message as a
// whole is application/cloudevents+json per asyncapi.yaml's
// defaultContentType, but that is a transport/Content-Type concern, not a
// JSON field carried on the envelope itself).
const CloudEventsDataContentType = "application/json"

// Topics this service publishes to / consumes from.
const (
	TopicWorkPlanningEvents    = "warehouse.work-planning.events"
	TopicWorkforceEvents       = "warehouse.workforce.events"
	TopicInventoryEvents       = "warehouse.inventory.events"
	TopicFulfillmentEvents     = "warehouse.fulfillment.events"
	TopicOrderManagementEvents = "warehouse.order-management.events"
)

// Event types this service consumes.
const (
	EventTypeShiftPlanCommitted      = "ShiftPlanCommitted"
	EventTypeStockReserved           = "StockReserved"
	EventTypeReservationRevoked      = "ReservationRevoked"
	EventTypeTaskCompleted           = "TaskCompleted"
	EventTypeOrderAllocated          = "OrderAllocated"
	EventTypeOrderPartiallyAllocated = "OrderPartiallyAllocated"
)
