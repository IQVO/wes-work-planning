package plan

import (
	"math"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// PathPlan is the committed rate x heads x hours split for one process path.
// Invariant: plannedHeads must not exceed installedStations.
type PathPlan struct {
	pathId            shared.PathId
	plannedHeads      shared.StationCount
	installedStations shared.StationCount
	rate              shared.Rate
	hours             float64

	// travelDistanceKnown/travelDistanceM/travelDistanceEstimated are an
	// optional, derived travel-distance hint between this path's two
	// representative locations (e.g. a pick zone's aisle and its pack
	// station), read once from facility-layout at commit time (ADR-0017,
	// Phase B3). Unset (travelDistanceKnown=false) by default so every
	// existing caller and test fixture keeps compiling and behaving
	// unchanged — nothing in this aggregate's own invariants depends on
	// it, exactly like WorkUnit.SKU (ADR-0009).
	travelDistanceKnown     bool
	travelDistanceM         float64
	travelDistanceEstimated bool
}

// NewPathPlan validates and constructs a PathPlan. Enforces
// plannedHeads <= installedStations.
func NewPathPlan(pathId shared.PathId, plannedHeads, installedStations shared.StationCount, rate shared.Rate, hours float64) (PathPlan, error) {
	if plannedHeads.GreaterThan(installedStations) {
		return PathPlan{}, ErrHeadsExceedStations
	}
	if hours <= 0 {
		return PathPlan{}, shared.ErrInvalidHours
	}
	throughput := rate.UnitsPerHour() * float64(plannedHeads.Value()) * hours
	if math.IsInf(throughput, 0) || math.IsNaN(throughput) {
		return PathPlan{}, ErrThroughputNotFinite
	}
	return PathPlan{
		pathId:            pathId,
		plannedHeads:      plannedHeads,
		installedStations: installedStations,
		rate:              rate,
		hours:             hours,
	}, nil
}

func (p PathPlan) PathId() shared.PathId                  { return p.pathId }
func (p PathPlan) PlannedHeads() shared.StationCount      { return p.plannedHeads }
func (p PathPlan) InstalledStations() shared.StationCount { return p.installedStations }
func (p PathPlan) Rate() shared.Rate                      { return p.rate }
func (p PathPlan) Hours() float64                         { return p.hours }

// PlannedThroughput is rate x heads x hours for this path.
func (p PathPlan) PlannedThroughput() float64 {
	return p.rate.UnitsPerHour() * float64(p.plannedHeads.Value()) * p.hours
}

// SetTravelDistance records the optional travel-distance hint after
// construction. A separate setter, rather than a NewPathPlan parameter,
// keeps every existing caller and test fixture compiling unchanged —
// travel distance is additive, not a new invariant (mirrors
// WorkUnit.SetSKU, ADR-0009).
func (p *PathPlan) SetTravelDistance(metresM float64, estimated bool) {
	p.travelDistanceKnown = true
	p.travelDistanceM = metresM
	p.travelDistanceEstimated = estimated
}

// TravelDistanceM is the optional travel-distance hint's length in metres.
// Only meaningful when TravelDistanceKnown reports true — callers must
// check that first, the same way WorkUnit.SKU()'s "" means "not known"
// but TravelDistanceKnown is an explicit bool rather than an empty-string
// sentinel, since 0.0 is itself a valid distance.
func (p PathPlan) TravelDistanceM() float64 { return p.travelDistanceM }

// TravelDistanceEstimated reports whether the travel-distance hint used a
// zone's bay-pitch fallback rather than real aisle centreline geometry
// (mirrors facility-layout's own TravelDistance.Estimated field). Only
// meaningful when TravelDistanceKnown reports true.
func (p PathPlan) TravelDistanceEstimated() bool { return p.travelDistanceEstimated }

// TravelDistanceKnown reports whether a travel-distance hint was recorded
// for this path plan — false when no lookup was attempted, the lookup was
// permissive/unavailable, or the two locations named at commit time were
// unknown or in different zones (facility-layout refuses rather than
// guesses across zones, see ADR-0017 there).
func (p PathPlan) TravelDistanceKnown() bool { return p.travelDistanceKnown }
