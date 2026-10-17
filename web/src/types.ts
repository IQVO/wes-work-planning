/** Wire types mirroring wes-work-planning's dto.go response shapes
 *  (backlogSnapshotResponseDTO / rebalanceResponseDTO / laborPlanViewDTO /
 *  workUnitResponseDTO) exactly -- kept hand-in-sync with the Go DTOs
 *  rather than code-generated for v1; revisit with openapi-typescript once
 *  the OpenAPI spec is the enforced source of truth (see order-mgmt-mfe's
 *  types.ts for the same convention). */

/** GET /paths/{pathId}/telemetry -- SampleBacklog read model. */
export interface BacklogSnapshot {
  pathId: string;
  backlogDepth: number;
  wip: number;
  mode: string;
  overAlarmThreshold: boolean;
}

/** Nested in RebalanceResponse when a labor-reassignment recommendation is
 *  attached -- committed ShiftPlan snapshot at the moment of the decision. */
export interface LaborPlanView {
  pathId: string;
  plannedHeads: number;
  plannedRate: number;
  plannedHours: number;
  observedAt: string;
}

/** GET /paths/{pathId}/rebalance -- RebalanceDecision (throttle vs
 *  reassign vs none), with the backlog/WIP snapshot it was decided from. */
export interface RebalanceRecommendation {
  pathId: string;
  action: string;
  backlogDepth: number;
  wip: number;
  laborPlan?: LaborPlanView;
}

/** POST /paths/{pathId}/charge body and response, and the pool
 *  configuration PUT -- mirror poolConfigResponseDTO /
 *  configurePoolRequestDTO / receiveChargeForecastRequestDTO /
 *  chargeForecastResponseDTO / cptBucketDTO in
 *  internal/adapters/inbound/http/dto.go exactly (same hand-in-sync
 *  convention as the read types above; see types.ts header note). */

/** FeedMode (internal/domain/release) -- exactly two legal values. */
export type PoolMode = "ReleaseFed" | "FlowFed";

export const POOL_MODES: readonly PoolMode[] = ["ReleaseFed", "FlowFed"];

/** PUT /paths/{pathId}/pool body -- the ConfigurePool command (ADR-0034). */
export interface ConfigurePoolRequest {
  mode: PoolMode;
  wipLimit: number;
}

/** PUT /paths/{pathId}/pool 200 response -- the pool as configured, with its
 *  live occupancy (wip may exceed wipLimit after a limit was lowered). */
export interface PoolConfigResponse {
  pathId: string;
  mode: PoolMode;
  wipLimit: number;
  wip: number;
  backlogDepth: number;
}

/** One CPT bucket: the last moment a parcel can be manifested and still make
 *  its truck, and the quantity due by that moment. */
export interface CPTBucket {
  cpt: string;
  quantity: number;
}

/** POST /paths/{pathId}/charge body -- at least one bucket. */
export interface ReceiveChargeForecastRequest {
  buckets: CPTBucket[];
}

/** POST /paths/{pathId}/charge 201 response. */
export interface ChargeForecastResponse {
  pathId: string;
  totalQuantity: number;
  buckets: CPTBucket[];
  receivedAt: string;
}

/** WorkUnit.State enum values (internal/domain/workunit/work_unit.go) --
 *  matches the domain's State.String() exactly: "Pending" | "Released" |
 *  "Completed". */
export type WorkUnitState = "Pending" | "Released" | "Completed" | (string & {});

/** GET /work-units?reference= (and the POST /paths/{pathId}/work-units
 *  response) -- a single releasable unit of work. */
export interface WorkUnit {
  id: string;
  pathId: string;
  cpt: string;
  reference: string;
  state: WorkUnitState;
  giftWrap: boolean;
  sku?: string;
  releasedAt?: string;
  completedAt?: string;
}
