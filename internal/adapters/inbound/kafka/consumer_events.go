package kafka

import (
	"time"
)

// shiftPlanCommittedData is Workforce Management's ShiftPlanCommitted
// payload — a DIFFERENT model from this service's own ShiftPlan aggregate.
type shiftPlanCommittedData struct {
	BuildingId   string  `json:"building_id"`
	ShiftId      string  `json:"shift_id"`
	PathId       string  `json:"path_id"`
	PlannedHeads int     `json:"planned_heads"`
	PlannedRate  float64 `json:"planned_rate"`
	PlannedHours float64 `json:"planned_hours"`
}

// inventoryEventData is shared by StockReserved and ReservationRevoked.
type inventoryEventData struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demand_ref"`
}

// taskCompletedData is fulfillment-execution's TaskCompleted payload (the
// CloudEvents `data` object). TaskType (PICK, PACK, SLAM, REBIN) is optional
// on the wire and read only for logging: it explains why a completion names
// a work unit this context never planned (a PACK task fulfillment-execution
// created itself during rebin consolidation carries the ORDER id).
type taskCompletedData struct {
	TaskId     string `json:"task_id"`
	StationId  string `json:"station_id"`
	WorkUnitId string `json:"work_unit_id"`
	TaskType   string `json:"task_type,omitempty"`
}

// orderAllocatedData is order-management's OrderAllocated /
// OrderPartiallyAllocated payload — both event types share this identical
// shape, since both mean "these lines are ready to enqueue".
type orderAllocatedData struct {
	OrderId     string          `json:"order_id"`
	PromiseDate time.Time       `json:"promise_date"`
	Lines       []orderLineData `json:"lines"`
}

// orderLineData is one allocated-and-released order line within an
// OrderAllocated/OrderPartiallyAllocated payload.
type orderLineData struct {
	LineNo   int    `json:"line_no"`
	SKU      string `json:"sku"`
	PathId   string `json:"path_id"`
	GiftWrap bool   `json:"gift_wrap"`
}

// workDemandReleasedData is network-inventory-planning's
// WorkDemandReleased payload (the CloudEvents `data` object), mirrored
// read-only from the producer's contract: one released transfer demand
// leg, becoming one WorkUnit via ApplyWorkDemandReleased (ADR-0033).
type workDemandReleasedData struct {
	DemandId    string    `json:"demand_id"`
	WorkKind    string    `json:"work_kind"`
	TransferRef string    `json:"transfer_ref"`
	PathId      string    `json:"path_id"`
	SiteId      string    `json:"site_id"`
	CPT         time.Time `json:"cpt"`
	SKU         string    `json:"sku"`
	Quantity    int       `json:"quantity"`
}

// slotPlanApprovedData is slotting-optimization's SlotPlanApproved payload
// (the CloudEvents `data` object), mirrored read-only from the producer's
// apis/asyncapi.yaml (SlotPlanApprovedEvent): only the fields this context
// plans from. `assignments` (the full forward map) is not needed: the moves
// are what changes on the floor (ADR-0040).
type slotPlanApprovedData struct {
	PlanId           string             `json:"plan_id"`
	SiteId           string             `json:"site_id"`
	ApprovedAt       time.Time          `json:"approved_at"`
	SupersedesPlanId string             `json:"supersedes_plan_id,omitempty"`
	Moves            []slotPlanMoveData `json:"moves"`
}

// slotPlanMoveData is one SlotMove: kind Assign | Relocate | Vacate; a
// Vacate omits to_slot and an Assign omits from_slot.
type slotPlanMoveData struct {
	SKU      string `json:"sku"`
	FromSlot string `json:"from_slot,omitempty"`
	ToSlot   string `json:"to_slot,omitempty"`
	Kind     string `json:"kind"`
}
