package ports

import (
	"context"

	"github.com/claudioed/wes-work-planning/internal/domain/traveldistanceview"
)

// TravelDistanceLookup is the outbound port for the synchronous
// cross-context read from facility-layout's travel-graph endpoint
// (GET /distance?from=&to=), used at shift-plan-commit time to enrich a
// PathPlan with the real measured/estimated distance between two coded
// locations, when both are supplied (see ADR-0017 in this repo).
//
// This is a synchronous HTTP read, not a Kafka projection, because there
// is no "distance changed" domain event to consume — a travel distance
// between two static warehouse locations changes only when the physical
// layout itself changes, which is exactly the kind of static fact a
// synchronous read-through is right for. (ProductClassificationLookup used
// the same reasoning under ADR-0009 until product-master started publishing
// ProductClassified; it is a Kafka-fed local copy since ADR-0035.)
type TravelDistanceLookup interface {
	GetDistance(ctx context.Context, from, to string) (traveldistanceview.TravelDistanceView, error)
}
