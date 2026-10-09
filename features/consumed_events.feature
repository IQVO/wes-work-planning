# Behaviour driven by the Kafka events this service consumes.
#
# Derived from:
#   - docs/docs/adr/0028-processed-event-mark-atomic-with-handling.md: a
#     redelivery (same event id) is a no-op, never a second application
#   - docs/docs/adr/0031-order-allocated-choreography.md: an OrderAllocated
#     event enqueues one work unit per line on the path named by the line
#   - docs/docs/adr/0033-transfer-work-demand-choreography.md: a
#     WorkDemandReleased event enqueues the transfer's work on its path
#   - docs/docs/adr/0012-process-path-catalogue-validation.md and 0030: a
#     path outside the catalogue is rejected, never invented
#   - fulfillment-execution's TaskCompleted completes the work unit it
#     names; an unknown unit is acknowledged without effect, a unit that was
#     never released fails so the message is retried
#   - ADR-0006 / 0019: Workforce Management's labor plan and Inventory's
#     usable-stock changes feed read models, idempotent per event id
#
# The events are applied through the same use cases the Kafka consumers
# call; every outcome is read back through the REST API.

Feature: Reacting to events from sibling contexts
  As the WES conductor
  I want upstream facts applied exactly once and bad ones refused loudly
  So that redeliveries never double-enqueue work and unknown paths never sneak in

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: An allocated order becomes one work unit per line
    When order-management publishes OrderAllocated event "evt-1" for order "order-500" promised for "2026-08-22T12:00:00Z" with lines:
      | line_no | sku   | path_id     | gift_wrap |
      | 1       | SKU-A | pick-zone-a | false     |
      | 2       | SKU-B | pick-zone-a | true      |
    Then the OrderAllocated event is applied
    When work units are looked up by reference "order-500"
    Then the work unit lookup returns 2 work units
    And every returned work unit carries reference "order-500"
    And every returned work unit is in state "Pending"
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 2 and WIP 0

  @bdd
  Scenario: The lines of an order can go to different paths
    When order-management publishes OrderAllocated event "evt-1" for order "order-501" promised for "2026-08-22T12:00:00Z" with lines:
      | line_no | sku   | path_id     | gift_wrap |
      | 1       | SKU-A | pick-zone-a | false     |
      | 2       | SKU-B | pack-zone-a | false     |
    Then the OrderAllocated event is applied
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 1 and WIP 0
    And the Work Pool telemetry for process path "pack-zone-a" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: A redelivered OrderAllocated enqueues nothing more
    Given order-management publishes OrderAllocated event "evt-1" for order "order-502" promised for "2026-08-22T12:00:00Z" with lines:
      | line_no | sku   | path_id     | gift_wrap |
      | 1       | SKU-A | pick-zone-a | false     |
    And the OrderAllocated event is applied
    When order-management publishes OrderAllocated event "evt-1" for order "order-502" promised for "2026-08-22T12:00:00Z" with lines:
      | line_no | sku   | path_id     | gift_wrap |
      | 1       | SKU-A | pick-zone-a | false     |
    Then the OrderAllocated event is ignored as a redelivery
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: An order allocated to a path outside the catalogue is rejected
    When order-management publishes OrderAllocated event "evt-1" for order "order-503" promised for "2026-08-22T12:00:00Z" with lines:
      | line_no | sku   | path_id      | gift_wrap |
      | 1       | SKU-A | unknown-zone | false     |
    Then the OrderAllocated event is rejected for an unrecognized process path

  @bdd
  Scenario: Transfer work demand becomes work on its path
    When network-inventory-planning publishes WorkDemandReleased event "evt-9" for demand "dem-1" of kind "TRANSFER_PICK" on process path "pick-zone-a" for 12 units of SKU "SKU-T" due at "2026-08-22T12:00:00Z"
    Then the WorkDemandReleased event is applied
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 1 and WIP 0

  @bdd
  Scenario Outline: Every transfer work kind is accepted
    When network-inventory-planning publishes WorkDemandReleased event "evt-9" for demand "dem-1" of kind "<kind>" on process path "pick-zone-a" for 12 units of SKU "SKU-T" due at "2026-08-22T12:00:00Z"
    Then the WorkDemandReleased event is applied

    Examples:
      | kind              |
      | TRANSFER_PICK     |
      | TRANSFER_DISPATCH |
      | TRANSFER_ARRIVAL  |

  @bdd
  Scenario: A redelivered WorkDemandReleased enqueues nothing more
    Given network-inventory-planning publishes WorkDemandReleased event "evt-9" for demand "dem-1" of kind "TRANSFER_PICK" on process path "pick-zone-a" for 12 units of SKU "SKU-T" due at "2026-08-22T12:00:00Z"
    And the WorkDemandReleased event is applied
    When network-inventory-planning publishes WorkDemandReleased event "evt-9" for demand "dem-1" of kind "TRANSFER_PICK" on process path "pick-zone-a" for 12 units of SKU "SKU-T" due at "2026-08-22T12:00:00Z"
    Then the WorkDemandReleased event is ignored as a redelivery
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: Transfer work demand for a path outside the catalogue is rejected
    When network-inventory-planning publishes WorkDemandReleased event "evt-9" for demand "dem-1" of kind "TRANSFER_PICK" on process path "unknown-zone" for 12 units of SKU "SKU-T" due at "2026-08-22T12:00:00Z"
    Then the WorkDemandReleased event is rejected for an unrecognized process path

  @bdd
  Scenario: Transfer work demand of an unknown kind is rejected
    When network-inventory-planning publishes WorkDemandReleased event "evt-9" for demand "dem-1" of kind "Teleport" on process path "pick-zone-a" for 12 units of SKU "SKU-T" due at "2026-08-22T12:00:00Z"
    Then the WorkDemandReleased event is rejected for an unknown work kind

  @bdd
  Scenario: A TaskCompleted event completes the work unit it names
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    When fulfillment-execution publishes TaskCompleted event "evt-7" for WorkUnit "wu-1"
    Then the TaskCompleted event is applied
    When the WorkUnit "wu-1" is requested by its id
    Then the WorkUnit in the response is in state "Completed"
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 0 and WIP 0

  @bdd
  Scenario: A redelivered TaskCompleted changes nothing
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    And fulfillment-execution publishes TaskCompleted event "evt-7" for WorkUnit "wu-1"
    And the TaskCompleted event is applied
    When fulfillment-execution publishes TaskCompleted event "evt-7" for WorkUnit "wu-1"
    Then the TaskCompleted event is ignored as a redelivery

  @bdd
  Scenario: A TaskCompleted for a work unit this service never held is acknowledged without effect
    When fulfillment-execution publishes TaskCompleted event "evt-7" for WorkUnit "wu-ghost"
    Then the TaskCompleted event is acknowledged without effect because the work unit is unknown

  @bdd
  Scenario: A TaskCompleted for a unit that was never released fails so it can be retried
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When fulfillment-execution publishes TaskCompleted event "evt-7" for WorkUnit "wu-1"
    Then the TaskCompleted event fails so that it can be retried
    When the WorkUnit "wu-1" is requested by its id
    Then the WorkUnit in the response is in state "Pending"

  @bdd
  Scenario: An inventory change event is applied once however often it is delivered
    Given inventory event "inv-1" changes SKU "SKU-A" by 5 units
    And inventory event "inv-1" changes SKU "SKU-A" by 5 units
    And inventory event "inv-2" changes SKU "SKU-A" by -2 units
    When the inventory view is requested for SKU "SKU-A"
    Then the request is accepted with status 200
    And the inventory view reports a usable quantity of 3 units

  @bdd
  Scenario: A labor plan event is applied once however often it is delivered
    Given Workforce Management publishes labor plan event "lp-1" of 6 heads at 90.5 units per hour for 8 hours for process path "pick-zone-a"
    And Workforce Management publishes labor plan event "lp-1" of 9 heads at 90.5 units per hour for 8 hours for process path "pick-zone-a"
    When the labor plan view is requested for process path "pick-zone-a"
    Then the request is accepted with status 200
    And the labor plan view reports 6 planned heads at a rate of 90.5 units per hour for 8 hours
