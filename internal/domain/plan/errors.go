// Package plan holds the ShiftPlan and PathPlan aggregates: the committed
// split of headcount across process paths.
package plan

import "errors"

var (
	ErrHeadsExceedStations = errors.New("planned heads exceeds installed stations")
	ErrNoPathPlans         = errors.New("shift plan requires at least one path plan")
	// ErrThroughputNotFinite guards the aggregate's own derived value:
	// plannedThroughput is rate x heads x hours, and each factor is
	// individually finite and positive, yet their product can still
	// overflow float64 to +/-Inf. JSON cannot carry an infinity, so an
	// overflowing plan has no representable response body — it must be
	// rejected at commit time as a client error, never surface later as
	// an unmarshalable (empty) 201 body.
	ErrThroughputNotFinite = errors.New("planned throughput is not finite: rate x heads x hours overflows")
)
