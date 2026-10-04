package telemetry

// NewInt64CounterForTest exposes the guarded counter constructor to the
// external test package.
var NewInt64CounterForTest = newInt64Counter
