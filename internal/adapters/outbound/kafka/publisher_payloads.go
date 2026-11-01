package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

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
		return workUnitCreatedData(ev)
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
	case shared.PathPlanDriftDetected:
		return pathPlanDriftDetectedData(ev), nil
	default:
		return json.Marshal(map[string]any{})
	}
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

// workUnitCreatedData is the WorkUnitCreated payload on the integration
// stream: the path/work-unit identity plus, strictly additively (ADR-0041),
// the originating `reference` (the order id for order work) and `line_no`.
// Each is omitted when unknown, so a unit with no origin keeps the exact
// payload every consumer already reads. The analytics stream does not carry
// them.
func workUnitCreatedData(ev shared.WorkUnitCreated) (json.RawMessage, error) {
	data := map[string]any{"path_id": ev.PathId.String(), "work_unit_id": ev.WorkUnitId}
	if ev.Reference != "" {
		data["reference"] = ev.Reference
	}
	if ev.LineNo > 0 {
		data["line_no"] = ev.LineNo
	}
	return json.Marshal(data)
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

// pathPlanDriftDetectedData is the PathPlanDriftDetected payload (ADR-0019),
// shared by the integration and analytics encoders so the two can never
// disagree. drift_heads is signed (observed - ours).
func pathPlanDriftDetectedData(ev shared.PathPlanDriftDetected) json.RawMessage {
	return mustMarshal(map[string]any{
		"path_id":                ev.PathId.String(),
		"wes_planned_heads":      ev.WesPlannedHeads,
		"observed_planned_heads": ev.ObservedPlannedHeads,
		"drift_heads":            ev.DriftHeads,
		"observed_at":            ev.ObservedAt.Format(time.RFC3339),
	})
}
