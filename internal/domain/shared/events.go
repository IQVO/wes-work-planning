package shared

import "time"

// DomainEvent is a past-tense fact published by an aggregate.
type DomainEvent interface {
	EventName() string
	OccurredAt() time.Time
}

type baseEvent struct {
	name string
	at   time.Time
}

func (b baseEvent) EventName() string     { return b.name }
func (b baseEvent) OccurredAt() time.Time { return b.at }

// WorkUnitCreated is raised when a new work unit is enqueued into a path's
// work pool. Reference and LineNo are the unit's originating reference (the
// order id for order work) and line number, carried as explicit attributes
// (ADR-0041) so a consumer correlates the unit to its source without parsing
// the work-unit id. Both are zero when the unit has no such origin.
type WorkUnitCreated struct {
	baseEvent
	WorkUnitId string
	PathId     PathId
	Reference  string
	LineNo     int
}

func NewWorkUnitCreated(workUnitId string, pathId PathId, at time.Time) WorkUnitCreated {
	return WorkUnitCreated{baseEvent: baseEvent{name: "WorkUnitCreated", at: at}, WorkUnitId: workUnitId, PathId: pathId}
}

// WithOrigin returns a copy of the event carrying the unit's originating
// reference and line number.
func (e WorkUnitCreated) WithOrigin(reference string, lineNo int) WorkUnitCreated {
	e.Reference = reference
	e.LineNo = lineNo
	return e
}

// WorkReleased is raised when the release policy admits a work unit from the
// pool into active work.
type WorkReleased struct {
	baseEvent
	WorkUnitId string
	PathId     PathId
}

func NewWorkReleased(workUnitId string, pathId PathId, at time.Time) WorkReleased {
	return WorkReleased{baseEvent: baseEvent{name: "WorkReleased", at: at}, WorkUnitId: workUnitId, PathId: pathId}
}

// BacklogThresholdBreached is raised when a path's backlog depth crosses its
// alarm threshold.
type BacklogThresholdBreached struct {
	baseEvent
	PathId PathId
}

func NewBacklogThresholdBreached(pathId PathId, at time.Time) BacklogThresholdBreached {
	return BacklogThresholdBreached{baseEvent: baseEvent{name: "BacklogThresholdBreached", at: at}, PathId: pathId}
}

// RateDeviationDetected is raised when a path's actual throughput deviates
// materially from plan.
type RateDeviationDetected struct {
	baseEvent
	PathId PathId
}

func NewRateDeviationDetected(pathId PathId, at time.Time) RateDeviationDetected {
	return RateDeviationDetected{baseEvent: baseEvent{name: "RateDeviationDetected", at: at}, PathId: pathId}
}

// PathThrottled is raised when flow balancing decides to throttle upstream
// release into a path.
type PathThrottled struct {
	baseEvent
	PathId PathId
}

func NewPathThrottled(pathId PathId, at time.Time) PathThrottled {
	return PathThrottled{baseEvent: baseEvent{name: "PathThrottled", at: at}, PathId: pathId}
}

// LaborReassignmentFlagged is raised when flow balancing recommends moving
// labor to relieve a path.
type LaborReassignmentFlagged struct {
	baseEvent
	PathId PathId
}

func NewLaborReassignmentFlagged(pathId PathId, at time.Time) LaborReassignmentFlagged {
	return LaborReassignmentFlagged{baseEvent: baseEvent{name: "LaborReassignmentFlagged", at: at}, PathId: pathId}
}

// WorkUnitCompleted is raised when a released work unit finishes.
type WorkUnitCompleted struct {
	baseEvent
	WorkUnitId string
	PathId     PathId
}

func NewWorkUnitCompleted(workUnitId string, pathId PathId, at time.Time) WorkUnitCompleted {
	return WorkUnitCompleted{baseEvent: baseEvent{name: "WorkUnitCompleted", at: at}, WorkUnitId: workUnitId, PathId: pathId}
}

// PathCapacityChanged is raised when this service reports a process path's
// currently remaining admission capacity for a specific CPT cutoff window
// (see ADR-0018). It PUBLISHES state this service already tracks on its own
// WorkPool aggregate (wipLimit vs. current WIP) — it introduces no new
// domain logic or per-CPT-bucket capacity concept.
//
// CutoffAt identifies which CPT cutoff window this reported figure applies
// to; it is a plain time.Time (this service's own native CPT currency),
// not a reference to process-path-management's site-schedule cptId string,
// which this service has no dependency on (see ADR-0018's correlation-by-
// timestamp discussion). RemainingUnits is meaningful only when Known is
// true.
//
// Known is false when the path's WorkPool is FlowFed (no hard admission
// ceiling — only an alarm threshold, which is not a capacity figure) or
// when its WIP limit is unset/zero (never provisioned for release-fed
// admission control). RemainingUnits is 0 whenever Known is false.
type PathCapacityChanged struct {
	baseEvent
	PathId         PathId
	CutoffAt       time.Time
	RemainingUnits int
	Known          bool
}

func NewPathCapacityChanged(pathId PathId, cutoffAt time.Time, remainingUnits int, known bool, at time.Time) PathCapacityChanged {
	return PathCapacityChanged{
		baseEvent:      baseEvent{name: "PathCapacityChanged", at: at},
		PathId:         pathId,
		CutoffAt:       cutoffAt,
		RemainingUnits: remainingUnits,
		Known:          known,
	}
}
