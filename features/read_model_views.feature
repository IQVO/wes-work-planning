# Scenarios in this file derive from:
#   - docs/docs/ddd/read-models.md — "The four projections" table
#     (LaborPlanObserved keyed by path_id, UsableInventoryObserved keyed by
#     sku) and "Projection staleness is explicit" ("If nothing has been
#     observed yet, the endpoints return 404, not a zero-valued body").
#   - apis/openapi.yaml — "getLaborPlanView" (404 until a
#     ShiftPlanCommitted event has been observed; LaborPlanViewResponse
#     fields incl. observedAt), "getInventoryView"
#     (InventoryViewResponse.usableQuantity), "getWorkUnitsByReference"
#     (array response, "empty (not 404) when the reference has never been
#     seen"; 400 reference-required when the query parameter is missing),
#     and "rebalanceDecision" (laborPlan is "Present only if a labor plan
#     has been observed for this path; ... Additive context, does not
#     affect action").
#   - .claude/rules/integration-events.md — "GET /work-units?reference="
#     "returns every WorkUnit ever enqueued against a caller-supplied
#     reference (order-management's OrderId), array-shaped,
#     side-effect-free".
#   - docs/docs/api/errors.md — "The full catalogue": not-found (404) and
#     reference-required (400) type URIs.
Feature: Read-only projections observed from neighbouring bounded contexts
  As the WES conductor
  I want read-only projections of Workforce's labor plan, Inventory's usable stock, and my own work-unit history
  So that operators and sibling services can see observed state without mutating this context's aggregates

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: The labor plan view is unavailable until a plan has been observed for the path
    When the labor plan view is requested for process path "pick-zone-a"
    Then the request is rejected with status 404
    And the problem detail title is "Resource not found"
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/not-found"

  @bdd
  Scenario: The labor plan view reports the latest plan Workforce Management observed
    Given Workforce Management committed a labor plan of 6 heads at 95.5 units per hour for 8 hours for process path "pick-zone-a"
    When the labor plan view is requested for process path "pick-zone-a"
    Then the request is accepted with status 200
    And the labor plan view reports 6 planned heads at a rate of 95.5 units per hour for 8 hours
    And the labor plan view was observed at "2026-08-21T08:00:00Z"

  @bdd
  Scenario: The rebalance recommendation carries the observed labor plan as additive context
    Given Workforce Management committed a labor plan of 6 heads at 95.5 units per hour for 8 hours for process path "pick-zone-a"
    And a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When the rebalance decision is requested for process path "pick-zone-a"
    Then the request is accepted with status 200
    And the rebalance recommendation action is "NoActionNeeded"
    And the rebalance recommendation includes the observed labor plan with 6 planned heads

  @bdd
  Scenario: The inventory view reports the net usable quantity observed for a SKU
    Given inventory changes of +500 and -255 units are observed for SKU "sku-88213"
    When the inventory view is requested for SKU "sku-88213"
    Then the request is accepted with status 200
    And the inventory view reports a usable quantity of 245 units
    And the inventory view was observed at "2026-08-21T08:00:00Z"

  @bdd
  Scenario: Work units are looked up by external reference as an array
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-77213-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-2" with CPT "2026-08-21T16:00:00Z" and reference "order-77213-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-3" with CPT "2026-08-21T12:00:00Z" and reference "order-77213-line-2" is enqueued to process path "pack-station-1"
    When work units are looked up by reference "order-77213-line-1"
    Then the request is accepted with status 200
    And the work unit lookup returns 2 work units
    And every returned work unit carries reference "order-77213-line-1"
    When work units are looked up by reference "order-99999-line-1"
    Then the work unit lookup returns 0 work units

  @bdd
  Scenario: Looking up work units without a reference is rejected
    When work units are looked up without a reference
    Then the request is rejected with status 400
    And the problem detail title is "Reference query parameter is required"
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/reference-required"
