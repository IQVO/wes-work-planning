// Package http is the inbound REST adapter: chi router, handlers, DTOs, and
// domain-error-to-HTTP-status mapping. Domain structs never leak across this
// boundary.
package http

import "time"

// problemDetails is the RFC 7807 (https://www.rfc-editor.org/rfc/rfc7807)
// Problem Details for HTTP APIs response shape used for every error
// response, served with Content-Type: application/problem+json.
type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
}

// cptBucketDTO uses pointers for its required fields so a bucket that
// omits cpt or quantity is distinguishable from one carrying their zero
// values — a missing required field is a 400, never a silently coerced
// zero CPT / zero quantity (mirrors inventory-storage's cycle-count fix).
type cptBucketDTO struct {
	CPT      *time.Time `json:"cpt"`
	Quantity *int       `json:"quantity"`
	// SKU and PickUnit are the optional pack-aware estimate of ADR-0045:
	// quantity stays EACHES; with a sku, the response also carries the
	// handles (cases for pickUnit CASE), cube and weight the bucket stands
	// for. Absent for a plain bucket.
	SKU      string `json:"sku,omitempty"`
	PickUnit string `json:"pickUnit,omitempty"`
	// Handles / CubeMm3 / WeightG are response-only; CubeMm3 and WeightG are
	// omitted when the pack level has no size on record.
	Handles *int   `json:"handles,omitempty"`
	CubeMm3 *int64 `json:"cubeMm3,omitempty"`
	WeightG *int64 `json:"weightG,omitempty"`
}

type receiveChargeForecastRequestDTO struct {
	Buckets []cptBucketDTO `json:"buckets"`
}

type chargeForecastResponseDTO struct {
	PathId        string         `json:"pathId"`
	TotalQuantity int            `json:"totalQuantity"`
	Buckets       []cptBucketDTO `json:"buckets"`
	ReceivedAt    time.Time      `json:"receivedAt"`
}

// commitShiftPlanRequestDTO uses pointers for plannedHeads and
// installedStations so an omitted field is distinguishable from an
// explicit 0 — both are required, and a missing one is a 400 rather than
// a silently coerced zero count (rateUnitsPerHour and hours need no
// pointer: their zero values already fail domain validation with 400).
type commitShiftPlanRequestDTO struct {
	PlannedHeads      *int    `json:"plannedHeads"`
	InstalledStations *int    `json:"installedStations"`
	RateUnitsPerHour  float64 `json:"rateUnitsPerHour"`
	Hours             float64 `json:"hours"`
	// FromLocationCode/ToLocationCode are optional facility-layout
	// LocationCodes enriching the committed plan with a real
	// travel-distance hint (ADR-0017, Phase B3). Both omitted (the
	// default) skips the enrichment entirely.
	FromLocationCode string `json:"fromLocationCode,omitempty"`
	ToLocationCode   string `json:"toLocationCode,omitempty"`
	// SKU and PickUnit (EACH | CASE, default EACH) are the optional
	// pack-aware handle of ADR-0045: the plan records the load of one handle
	// of that SKU and the response carries the travel estimate.
	SKU      string `json:"sku,omitempty"`
	PickUnit string `json:"pickUnit,omitempty"`
}

type shiftPlanResponseDTO struct {
	PathId            string  `json:"pathId"`
	PlannedHeads      int     `json:"plannedHeads"`
	InstalledStations int     `json:"installedStations"`
	RateUnitsPerHour  float64 `json:"rateUnitsPerHour"`
	Hours             float64 `json:"hours"`
	PlannedThroughput float64 `json:"plannedThroughput"`
	// TravelDistanceM/TravelDistanceEstimated are omitted entirely when
	// no travel-distance hint was recorded (PathPlan.TravelDistanceKnown()
	// == false) — not defaulted to 0/false, so a consumer that already
	// treats "absent" as "no hint" sees no difference from before this
	// feature existed (ADR-0017, Phase B3; same omit-when-unknown
	// discipline as WorkReleased's required_capabilities/fragile, ADR-0009).
	TravelDistanceM         *float64 `json:"travelDistanceM,omitempty"`
	TravelDistanceEstimated *bool    `json:"travelDistanceEstimated,omitempty"`
	// Handle is the optional per-handle load of a pack-aware plan (ADR-0045),
	// omitted when the request named no sku.
	Handle *handleLoadDTO `json:"handle,omitempty"`
}

// handleLoadDTO is the load of ONE handle (a whole case for CASE) and the
// travel estimate derived from it. WeightG / CubeMm3 are omitted when the
// pack level has no size on record; CarryKgM only when the weight AND a travel
// distance are known — never zero-filled.
type handleLoadDTO struct {
	PickUnit string   `json:"pickUnit"`
	WeightG  *int64   `json:"weightG,omitempty"`
	CubeMm3  *int64   `json:"cubeMm3,omitempty"`
	CarryKgM *float64 `json:"carryKgM,omitempty"`
}

// enqueueWorkUnitRequestDTO carries cpt as a pointer so an omitted CPT —
// a required field — is a 400, never a silently coerced zero-value
// timestamp. workUnitId/reference already 400 on empty via domain
// validation; sku and giftWrap are genuinely optional.
type enqueueWorkUnitRequestDTO struct {
	WorkUnitId string     `json:"workUnitId"`
	CPT        *time.Time `json:"cpt"`
	Reference  string     `json:"reference"`
	// SKU is optional: the inventory SKU this order line corresponds to,
	// if known. Threaded through so the released WorkReleased integration
	// event can carry derived hazmat/fragile hints (see ADR-0009). Absent
	// or empty is valid — not every caller knows a SKU.
	SKU string `json:"sku,omitempty"`
	// GiftWrap is optional: whether the requester asked the warehouse to
	// produce a gift package for this work unit, if known at enqueue
	// time. Threaded through so the released WorkReleased integration
	// event carries it directly (see ADR-0010) — unlike SKU, this is
	// never looked up from another service. Absent defaults to false.
	GiftWrap bool `json:"giftWrap,omitempty"`
	// LineNo is optional: the 1-based order line this work unit is for
	// (ADR-0036). A pointer so an explicit 0 or negative value is a 400
	// (invalid-line-no) rather than silently becoming "unknown"; absent or
	// null means unknown.
	LineNo *int `json:"lineNo,omitempty"`
	// Quantity (EACHES), PickUnit (EACH | CASE, default EACH) and FromBin
	// make the unit a pack-aware pick (CASE_PICK / EACH_PICK, ADR-0045); they
	// need sku. A CASE pick needs an active CASE level in the local pack copy
	// and a whole number of cases.
	Quantity *int   `json:"quantity,omitempty"`
	PickUnit string `json:"pickUnit,omitempty"`
	FromBin  string `json:"fromBin,omitempty"`
	// CaseBreak makes the unit a BREAK_CASE (ADR-0045); it needs sku and
	// excludes the pick members.
	CaseBreak *caseBreakDTO `json:"caseBreak,omitempty"`
}

// caseBreakDTO is the sealed-case break of a BREAK_CASE unit.
type caseBreakDTO struct {
	LPN        string `json:"lpn"`
	LooseBin   string `json:"looseBin"`
	PickTaskId string `json:"pickTaskId"`
}

// pickDTO is the pick plan of a CASE_PICK / EACH_PICK unit as the work-unit
// read shows it; quantity is in eaches.
type pickDTO struct {
	Quantity int    `json:"quantity"`
	Unit     string `json:"unit"`
	FromBin  string `json:"fromBin,omitempty"`
}

type workUnitResponseDTO struct {
	Id        string    `json:"id"`
	PathId    string    `json:"pathId"`
	CPT       time.Time `json:"cpt"`
	Reference string    `json:"reference"`
	State     string    `json:"state"`
	GiftWrap  bool      `json:"giftWrap"`
	// SKU is the optional inventory SKU this work unit's order line
	// corresponds to, omitted when not known (see EnqueueWorkUnitRequest.SKU).
	SKU string `json:"sku,omitempty"`
	// LineNo is the optional order line number this work unit is for,
	// omitted when unknown (ADR-0036).
	LineNo int `json:"lineNo,omitempty"`
	// ReleasedAt/CompletedAt are omitted (not null) until the work unit
	// reaches the corresponding lifecycle state.
	ReleasedAt  *time.Time `json:"releasedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	// CancelledAt is set only on a Cancelled unit (ADR-0040: a superseded
	// slot plan's move that was never released).
	CancelledAt *time.Time `json:"cancelledAt,omitempty"`
	// Relocation is the slot-plan move of a RELOCATE unit (ADR-0040),
	// omitted on every other unit.
	Relocation *relocationDTO `json:"relocation,omitempty"`
	// WorkKind is the pack-aware kind (CASE_PICK, EACH_PICK, BREAK_CASE,
	// ADR-0045), omitted on every other unit.
	WorkKind string `json:"workKind,omitempty"`
	// Pick / CaseBreak are the pack-aware members, omitted on every other unit.
	Pick      *pickDTO      `json:"pick,omitempty"`
	CaseBreak *caseBreakDTO `json:"caseBreak,omitempty"`
}

// relocationDTO is one slot-plan move as the work-unit read shows it.
type relocationDTO struct {
	PlanId   string `json:"planId"`
	MoveSeq  int    `json:"moveSeq"`
	Kind     string `json:"kind"`
	SiteId   string `json:"siteId"`
	FromSlot string `json:"fromSlot,omitempty"`
	ToSlot   string `json:"toSlot,omitempty"`
	// BlockedBy is the work unit this move waits on before it can be
	// released; omitted when it waits on nothing.
	BlockedBy string `json:"blockedBy,omitempty"`
}

// configurePoolRequestDTO carries wipLimit as a pointer so an omitted limit —
// a required field — is a 400, never a silently coerced 0. mode is validated
// against the FeedMode enum by the domain (empty/unknown -> 400).
type configurePoolRequestDTO struct {
	Mode     string `json:"mode"`
	WIPLimit *int   `json:"wipLimit"`
}

// poolConfigResponseDTO is the pool as configured, plus the live occupancy
// so a caller lowering a limit can see it is above the new limit (WIP is
// never evicted).
type poolConfigResponseDTO struct {
	PathId       string `json:"pathId"`
	Mode         string `json:"mode"`
	WIPLimit     int    `json:"wipLimit"`
	WIP          int    `json:"wip"`
	BacklogDepth int    `json:"backlogDepth"`
}

type backlogSnapshotResponseDTO struct {
	PathId             string `json:"pathId"`
	BacklogDepth       int    `json:"backlogDepth"`
	WIP                int    `json:"wip"`
	Mode               string `json:"mode"`
	OverAlarmThreshold bool   `json:"overAlarmThreshold"`
	// RemainingCapacityKnown/RemainingCapacityUnits are present only
	// when the request supplied a `cutoffAt` query parameter (ADR-0018).
	// RemainingCapacityUnits is additionally omitted (not present as
	// null) when RemainingCapacityKnown is false — a FlowFed path or a
	// ReleaseFed path with no WIP limit provisioned has no meaningful
	// remaining-capacity figure to report.
	RemainingCapacityKnown *bool `json:"remainingCapacityKnown,omitempty"`
	RemainingCapacityUnits *int  `json:"remainingCapacityUnits,omitempty"`
}

type rebalanceResponseDTO struct {
	PathId       string            `json:"pathId"`
	Action       string            `json:"action"`
	BacklogDepth int               `json:"backlogDepth"`
	WIP          int               `json:"wip"`
	LaborPlan    *laborPlanViewDTO `json:"laborPlan,omitempty"`
}

type laborPlanViewDTO struct {
	PathId       string    `json:"pathId"`
	PlannedHeads int       `json:"plannedHeads"`
	PlannedRate  float64   `json:"plannedRate"`
	PlannedHours float64   `json:"plannedHours"`
	ObservedAt   time.Time `json:"observedAt"`
	// DriftHeads / DriftDetectedAt are the ADR-0019 reconciliation outcome
	// against this service's own committed PathPlan. Present only once a
	// comparison has been computed (omitted, never zeroed, before that);
	// DriftHeads is signed (observed - ours) and 0 means the plans agree;
	// DriftDetectedAt is present only when DriftHeads != 0.
	DriftHeads      *int       `json:"driftHeads,omitempty"`
	DriftDetectedAt *time.Time `json:"driftDetectedAt,omitempty"`
}

type inventoryViewResponseDTO struct {
	SKU            string    `json:"sku"`
	UsableQuantity int       `json:"usableQuantity"`
	ObservedAt     time.Time `json:"observedAt"`
}
