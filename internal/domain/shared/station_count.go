package shared

import "math"

// StationCount is a non-negative count of staffed or installed stations on a
// process path.
type StationCount struct {
	value int
}

// NewStationCount validates and constructs a StationCount. Negative values
// and values above the int32 range are rejected: the count is persisted as
// a Postgres INTEGER (int4) on the Postgres adapter, and the API contract
// documents `format: int32` — every adapter must enforce that same bound,
// so an out-of-range count is a client error (400) everywhere, never a
// store-specific failure later.
func NewStationCount(value int) (StationCount, error) {
	if value < 0 || value > math.MaxInt32 {
		return StationCount{}, ErrInvalidStationCount
	}
	return StationCount{value: value}, nil
}

func (s StationCount) Value() int {
	return s.value
}

func (s StationCount) LessThan(other StationCount) bool {
	return s.value < other.value
}

func (s StationCount) GreaterThan(other StationCount) bool {
	return s.value > other.value
}
