# Documentation-derived rules of the events this service consumes.
#
# Sources:
#   - warehouse-docs docs/contexts/wes-work-planning/use-cases.md "Inbound-event
#     use cases" table (every row's Effect and Invariants/failure columns) and
#     aggregate-design-canvas.md WorkUnit "Corrective policies";
#   - docs/docs/adr/0028 (the processed-event mark commits with the effect, a
#     failure rolls it back), 0031 (OrderAllocated), 0033 (WorkDemandReleased),
#     0036 (line_no), 0043 (TaskCompleted for an already completed unit);
#   - troubleshooting.md "Kafka consumption" rows.
# The events are applied through the use cases the Kafka consumers call; every
# outcome is read back through the REST API.
Feature: Consumed-event rules from the documentation
  As the WES conductor
  I want every consumed fact mapped, deduplicated and refused exactly as documented
  So that a redelivery, a replay under a new id or a poison message never corrupts the pools

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: A TaskCompleted for a unit that is already completed is skipped, not retried
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    And fulfillment-execution publishes TaskCompleted event "evt-1" for WorkUnit "wu-1"
    And the TaskCompleted event is applied
    When fulfillment-execution publishes TaskCompleted event "evt-2" for WorkUnit "wu-1"
    Then the TaskCompleted event is skipped because the work unit was already completed
    And 1 "WorkUnitCompleted" event was published
    When fulfillment-execution publishes TaskCompleted event "evt-2" for WorkUnit "wu-1"
    Then the TaskCompleted event is ignored as a redelivery

  @bdd
  Scenario: A TaskCompleted that failed leaves no processed mark, so its retry is applied
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And fulfillment-execution publishes TaskCompleted event "evt-7" for WorkUnit "wu-1"
    And the TaskCompleted event fails so that it can be retried
    And work is released from process path "pick-zone-a"
    When fulfillment-execution publishes TaskCompleted event "evt-7" for WorkUnit "wu-1"
    Then the TaskCompleted event is applied
    When the WorkUnit "wu-1" is requested by its id
    Then the WorkUnit in the response is in state "Completed"
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 0 and WIP 0

  @bdd
  Scenario: An allocated order line becomes a work unit carrying its line, SKU, gift wrap and the promise date as CPT
    When order-management publishes OrderAllocated event "evt-1" for order "order-500" promised for "2026-08-22T12:00:00Z" with lines:
      | line_no | sku    | path_id     | gift_wrap |
      | 1       | SKU-A  | pick-zone-a | false     |
      | 2       | SKU-B  | pick-zone-a | true      |
    Then the OrderAllocated event is applied
    When the WorkUnit "order-500-line-2" is requested by its id
    Then the WorkUnit in the response has reference "order-500"
    And the WorkUnit in the response has pathId "pick-zone-a"
    And the WorkUnit in the response has cpt "2026-08-22T12:00:00Z"
    And the WorkUnit in the response has sku "SKU-B"
    And the WorkUnit in the response has line number 2
    And the WorkUnit in the response requests gift wrap
    And the WorkUnit in the response is in state "Pending"
    And 2 "WorkUnitCreated" events were published
    When the WorkUnit "order-500-line-1" is requested by its id
    Then the WorkUnit in the response does not request gift wrap

  @bdd
  Scenario Outline: An in-range line number is stored on the work unit
    When order-management publishes OrderAllocated event "evt-1" for order "order-600" promised for "2026-08-22T12:00:00Z" with the single line <line> of SKU "SKU-A" on process path "pick-zone-a"
    Then the OrderAllocated event is applied
    When the WorkUnit "order-600-line-<line>" is requested by its id
    Then the WorkUnit in the response has line number <line>

    Examples:
      | line       |
      | 1          |
      | 2147483647 |

  @bdd
  Scenario Outline: An out-of-range line number is stored as unknown and the order is never rejected
    When order-management publishes OrderAllocated event "evt-1" for order "order-601" promised for "2026-08-22T12:00:00Z" with the single line <line> of SKU "SKU-A" on process path "pick-zone-a"
    Then the OrderAllocated event is applied
    When the WorkUnit "order-601-line-<line>" is requested by its id
    Then the request is accepted with status 200
    And the WorkUnit in the response has no lineNo

    Examples:
      | line       |
      | 0          |
      | -3         |
      | 2147483648 |

  @bdd
  Scenario: A later partial allocation adds only the lines that are not yet in the pool
    Given order-management publishes OrderAllocated event "evt-1" for order "order-700" promised for "2026-08-22T12:00:00Z" with the single line 1 of SKU "SKU-A" on process path "pick-zone-a"
    And the OrderAllocated event is applied
    When order-management publishes OrderAllocated event "evt-2" for order "order-700" promised for "2026-08-22T12:00:00Z" with lines:
      | line_no | sku   | path_id     | gift_wrap |
      | 1       | SKU-A | pick-zone-a | false     |
      | 2       | SKU-B | pick-zone-a | false     |
    Then the OrderAllocated event is applied
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 2 and WIP 0
    And 2 "WorkUnitCreated" events were published

  @bdd
  Scenario: An order with one line on an unknown path enqueues nothing and leaves no mark for the corrected replay
    When order-management publishes OrderAllocated event "evt-1" for order "order-800" promised for "2026-08-22T12:00:00Z" with lines:
      | line_no | sku   | path_id      | gift_wrap |
      | 1       | SKU-A | pick-zone-a  | false     |
      | 2       | SKU-B | unknown-zone | false     |
    Then the OrderAllocated event is rejected for an unrecognized process path
    When work units are looked up by reference "order-800"
    Then the work unit lookup returns 0 work units
    When order-management publishes OrderAllocated event "evt-1" for order "order-800" promised for "2026-08-22T12:00:00Z" with lines:
      | line_no | sku   | path_id     | gift_wrap |
      | 1       | SKU-A | pick-zone-a | false     |
      | 2       | SKU-B | pack-zone-a | false     |
    Then the OrderAllocated event is applied
    When work units are looked up by reference "order-800"
    Then the work unit lookup returns 2 work units

  @bdd
  Scenario: A transfer work demand becomes one unit whose id and reference are the demand id, and a replay under a new event id is a no-op
    When network-inventory-planning publishes WorkDemandReleased event "evt-9" for demand "dem-77" of kind "TRANSFER_DISPATCH" on process path "pack-zone-a" for 40 units of SKU "SKU-T" due at "2026-08-22T09:30:00Z"
    Then the WorkDemandReleased event is applied
    When the WorkUnit "dem-77" is requested by its id
    Then the WorkUnit in the response has reference "dem-77"
    And the WorkUnit in the response has pathId "pack-zone-a"
    And the WorkUnit in the response has cpt "2026-08-22T09:30:00Z"
    And the WorkUnit in the response is in state "Pending"
    When network-inventory-planning publishes WorkDemandReleased event "evt-10" for demand "dem-77" of kind "TRANSFER_DISPATCH" on process path "pack-zone-a" for 40 units of SKU "SKU-T" due at "2026-08-22T09:30:00Z"
    Then the WorkDemandReleased event is applied
    And the Work Pool telemetry for process path "pack-zone-a" reports backlog depth 1 and WIP 0
    And 1 "WorkUnitCreated" event was published

  @bdd
  Scenario Outline: Only a transfer leg may arrive as a work demand
    When network-inventory-planning publishes WorkDemandReleased event "evt-9" for demand "dem-77" of kind "<kind>" on process path "pick-zone-a" for 40 units of SKU "SKU-T" due at "2026-08-22T09:30:00Z"
    Then the WorkDemandReleased event is rejected for an unknown work kind
    When work units are looked up by reference "dem-77"
    Then the work unit lookup returns 0 work units

    Examples:
      | kind           |
      | RELOCATE       |
      | REPLENISH      |
      | transfer_pick  |
      |                |

  @bdd
  Scenario: An unseen SKU has no inventory view, a revoked reservation nets to zero and SKUs stay separate
    When the inventory view is requested for SKU "sku-never-seen"
    Then the request is rejected with status 404
    And the problem type slug is "not-found"
    Given inventory event "inv-1" changes SKU "SKU-A" by -5 units
    And inventory event "inv-2" changes SKU "SKU-A" by 5 units
    And inventory event "inv-3" changes SKU "SKU-B" by -2 units
    When the inventory view is requested for SKU "SKU-A"
    Then the request is accepted with status 200
    And the inventory view reports a usable quantity of 0 units
    When the inventory view is requested for SKU "SKU-B"
    Then the inventory view reports a usable quantity of -2 units
