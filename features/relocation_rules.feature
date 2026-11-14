# Documentation-derived rules of slot-plan relocation work.
#
# Sources:
#   - warehouse-docs docs/contexts/wes-work-planning/use-cases.md, inbound
#     table row `ApplySlotPlanApproved` and `ReleaseNextWork` /
#     `RecordCompletion` invariants (blocked_by admitted on completion,
#     Cancelled leftovers skipped);
#   - docs/docs/adr/0040-slot-plan-relocation-work.md decisions 3 (id, CPT
#     horizon, intake), 4 (sequencing), 5 (supersession per site), 7 (fail
#     loud on a broken contract);
#   - docs/docs/api/errors.md and apis/openapi.yaml (completeWorkUnit 409
#     `work-unit-cancelled`).
# The SlotPlanApproved steps drive ApplySlotPlanApproved, the use case the
# warehouse.slotting-optimization.events consumer calls; every outcome is
# read back through the real REST surface.
Feature: Slot-plan relocation rules from the documentation
  As the WES planner
  I want the sequencing, supersession and fail-loud rules of approved slot plans pinned
  So that the floor never fills an occupied slot and a broken plan never leaves half its work behind

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: Each move is a RELOCATE unit due 24 hours after approval and shares its plan's reference
    When slotting-optimization approves slot plan "plan-7" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind     | from_slot | to_slot |
      | SKU-1 | Relocate | S1        | S2      |
      | SKU-2 | Relocate | S3        | S4      |
      | SKU-3 | Relocate | S5        | S6      |
    Then 3 "WorkUnitCreated" events were published
    When work units are looked up by reference "plan-7"
    Then the work unit lookup returns 3 work units
    When the WorkUnit "plan-7-move-2" is requested by its id
    Then the WorkUnit in the response has cpt "2026-08-22T07:00:00Z"
    And the WorkUnit in the response relocates SKU "SKU-3" from "S5" to "S6" as move 2 of plan "plan-7"
    And the WorkUnit in the response belongs to site "SITE-1"
    And the WorkUnit in the response waits on nothing

  @bdd
  Scenario Outline: A move is planned with the slots its kind needs
    When slotting-optimization approves slot plan "plan-K" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | <kind> | <from>    | <to>    |
    Then the RELOCATE WorkUnit "plan-K-move-0" is in state "Pending"

    Examples:
      | kind     | from | to |
      | Relocate | S1   | S2 |
      | Assign   |      | S2 |
      | Vacate   | S1   |    |

  @bdd
  Scenario Outline: A move that lacks what its kind needs refuses the whole plan
    When slotting-optimization approves slot plan "plan-X" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-9 | Assign |           | S9      |
      | <sku> | <kind> | <from>    | <to>    |
    Then the slot plan is refused as poison
    And 0 "WorkUnitCreated" events were published
    When work units are looked up by reference "plan-X"
    Then the work unit lookup returns 0 work units

    Examples:
      | sku   | kind     | from | to | why                                  |
      | SKU-1 | Relocate | S1   |    | a Relocate needs to_slot             |
      | SKU-1 | Relocate |      | S2 | a Relocate needs from_slot           |
      | SKU-1 | Assign   | S1   |    | an Assign needs to_slot              |
      | SKU-1 | Vacate   |      | S2 | a Vacate needs from_slot             |
      | SKU-1 | Relocate | S1   | S1 | a Relocate between equal slots       |
      | SKU-1 | relocate | S1   | S2 | the kind keeps the slotting spelling |
      |       | Assign   |      | S2 | every move names a SKU               |

  @bdd
  Scenario: A refused plan leaves no trace, so the corrected plan is planned in full
    When slotting-optimization approves slot plan "plan-X" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind     | from_slot | to_slot |
      | SKU-1 | Assign   |           | S1      |
      | SKU-2 | Relocate | S2        | S2      |
    Then the slot plan is refused as poison
    When slotting-optimization approves slot plan "plan-X" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind     | from_slot | to_slot |
      | SKU-1 | Assign   |           | S1      |
      | SKU-2 | Relocate | S2        | S3      |
    And work units are looked up by reference "plan-X"
    Then the work unit lookup returns 2 work units
    And the RELOCATE WorkUnit "plan-X-move-1" is in state "Pending"

  @bdd
  Scenario: A RELOCATE path missing from the catalogue refuses the plan until it is declared and replayed
    Given the process-path catalogue does not declare RELOCATE
    When slotting-optimization approves slot plan "plan-C" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    Then the slot plan is refused as poison
    When work units are looked up by reference "plan-C"
    Then the work unit lookup returns 0 work units
    When the process-path catalogue declares RELOCATE without an intake
    And the SlotPlanApproved event is redelivered
    And work units are looked up by reference "plan-C"
    Then the work unit lookup returns 1 work units
    And the RELOCATE WorkUnit "plan-C-move-0" is in state "Pending"

  @bdd
  Scenario: A RELOCATE intake that excludes SLOT_RELOCATION refuses the plan
    Given the process-path catalogue declares RELOCATE with the intakes "CUSTOMER_ORDER", "STOCK_TRANSFER"
    When slotting-optimization approves slot plan "plan-C" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    Then the slot plan is refused as poison
    When work units are looked up by reference "plan-C"
    Then the work unit lookup returns 0 work units

  @bdd
  Scenario: A RELOCATE intake that lists SLOT_RELOCATION among others admits the plan
    Given the process-path catalogue declares RELOCATE with the intakes "CUSTOMER_ORDER", "SLOT_RELOCATION"
    When slotting-optimization approves slot plan "plan-C" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    Then the RELOCATE WorkUnit "plan-C-move-0" is in state "Pending"

  @bdd
  Scenario: The same plan arriving again under a new event id plans nothing more
    Given slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    When slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    And work units are looked up by reference "plan-A"
    Then the work unit lookup returns 1 work units
    And 1 "WorkUnitCreated" event was published

  @bdd
  Scenario: Two plans approved at the same instant order by plan id, whichever order they arrive in
    Given slotting-optimization approves slot plan "plan-B" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-2 | Assign |           | S2      |
    And slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    And slotting-optimization approves slot plan "plan-C" for site "SITE-2" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-3 | Assign |           | S3      |
    And slotting-optimization approves slot plan "plan-D" for site "SITE-2" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-4 | Assign |           | S4      |
    When work units are looked up by reference "plan-A"
    Then the work unit lookup returns 0 work units
    And the RELOCATE WorkUnit "plan-B-move-0" is in state "Pending"
    And the RELOCATE WorkUnit "plan-C-move-0" is in state "Cancelled"
    And the RELOCATE WorkUnit "plan-D-move-0" is in state "Pending"

  @bdd
  Scenario: A newer plan of another site never cancels this site's moves
    Given slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    When slotting-optimization approves slot plan "plan-Z" for site "SITE-2" at "2026-08-21T09:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-9 | Assign |           | S9      |
    Then the RELOCATE WorkUnit "plan-A-move-0" is in state "Pending"
    And the RELOCATE WorkUnit "plan-Z-move-0" is in state "Pending"

  @bdd
  Scenario: A swap is broken at its lowest move, which runs first
    When slotting-optimization approves slot plan "plan-S" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind     | from_slot | to_slot |
      | SKU-1 | Relocate | S1        | S2      |
      | SKU-2 | Relocate | S2        | S1      |
    And the WorkUnit "plan-S-move-0" is requested by its id
    Then the WorkUnit in the response waits on nothing
    When the WorkUnit "plan-S-move-1" is requested by its id
    Then the WorkUnit in the response waits on "plan-S-move-0"
    When work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-S-move-0"
    When work is released from process path "RELOCATE"
    Then the request is rejected with status 409
    When the WorkUnit "plan-S-move-0" is recorded as completed
    And work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-S-move-1"

  @bdd
  Scenario: When several moves vacate one slot the lowest move_seq is the one waited on
    When slotting-optimization approves slot plan "plan-V" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind     | from_slot | to_slot |
      | SKU-1 | Vacate   | S3        |         |
      | SKU-2 | Relocate | S3        | S8      |
      | SKU-3 | Assign   |           | S3      |
    And the WorkUnit "plan-V-move-2" is requested by its id
    Then the WorkUnit in the response waits on "plan-V-move-0"
    When work is released from process path "RELOCATE"
    And work is released from process path "RELOCATE"
    And the WorkUnit "plan-V-move-0" is recorded as completed
    And work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-V-move-2"

  @bdd
  Scenario: A blocked move stays out of the backlog and only its own blocker admits it
    When slotting-optimization approves slot plan "plan-W" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Vacate | S3        |         |
      | SKU-2 | Assign |           | S3      |
      | SKU-3 | Vacate | S5        |         |
      | SKU-4 | Assign |           | S5      |
    Then the Work Pool telemetry for process path "RELOCATE" reports backlog depth 2 and WIP 0
    When work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-W-move-0"
    When work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-W-move-2"
    When the WorkUnit "plan-W-move-0" is recorded as completed
    Then the Work Pool telemetry for process path "RELOCATE" reports backlog depth 1 and WIP 1
    When work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-W-move-1"
    When work is released from process path "RELOCATE"
    Then the request is rejected with status 409
    And the problem type slug is "work-pool-empty"
    When the WorkUnit "plan-W-move-2" is recorded as completed
    And work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-W-move-3"

  @bdd
  Scenario: A TaskCompleted from the floor admits the move that waited on it
    Given slotting-optimization approves slot plan "plan-T" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Vacate | S3        |         |
      | SKU-2 | Assign |           | S3      |
    And work is released from process path "RELOCATE"
    And the released WorkUnit is "plan-T-move-0"
    When fulfillment-execution publishes TaskCompleted event "evt-task-1" for WorkUnit "plan-T-move-0"
    Then the TaskCompleted event is applied
    When the WorkUnit "plan-T-move-1" is requested by its id
    Then the WorkUnit in the response waits on nothing
    When work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-T-move-1"

  @bdd
  Scenario: A superseding plan cancels a blocked move that never had a pool entry
    Given slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Vacate | S3        |         |
      | SKU-2 | Assign |           | S3      |
    When slotting-optimization approves slot plan "plan-B" for site "SITE-1" at "2026-08-21T07:30:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-7 | Assign |           | S7      |
    Then the RELOCATE WorkUnit "plan-A-move-0" is in state "Cancelled"
    And the RELOCATE WorkUnit "plan-A-move-1" is in state "Cancelled"
    And the Work Pool telemetry for process path "RELOCATE" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: A cancelled move is withdrawn from the pool, so release never hands it out
    Given slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    And slotting-optimization approves slot plan "plan-B" for site "SITE-1" at "2026-08-21T07:30:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-7 | Assign |           | S7      |
    When work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-B-move-0"
    When work is released from process path "RELOCATE"
    Then the request is rejected with status 409
    And the problem type slug is "work-pool-empty"
    When the WorkUnit "plan-A-move-0" is requested by its id
    Then the WorkUnit in the response was cancelled at "2026-08-21T08:00:00Z"

  @bdd
  Scenario: Released work of a superseded plan runs to completion and its cancelled waiter stays cancelled
    Given slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Vacate | S3        |         |
      | SKU-2 | Assign |           | S3      |
    And work is released from process path "RELOCATE"
    And the released WorkUnit is "plan-A-move-0"
    And slotting-optimization approves slot plan "plan-B" for site "SITE-1" at "2026-08-21T07:30:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-7 | Assign |           | S7      |
    When the WorkUnit "plan-A-move-0" is recorded as completed
    Then the request is accepted with status 200
    And the WorkUnit in the response is in state "Completed"
    And the RELOCATE WorkUnit "plan-A-move-1" is in state "Cancelled"
    When work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-B-move-0"

  # openapi completeWorkUnit 409: "was cancelled (`work-unit-cancelled`)" and
  # ADR-0040 "Cancelled is terminal: Release and Complete refuse it". Today
  # Complete on a Cancelled unit answers `work-unit-not-released`.
  @bdd @known-bug
  Scenario: Completing a cancelled move is refused as work-unit-cancelled
    Given slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    And slotting-optimization approves slot plan "plan-B" for site "SITE-1" at "2026-08-21T07:30:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-7 | Assign |           | S7      |
    When the WorkUnit "plan-A-move-0" is recorded as completed
    Then the request is rejected with status 409
    And the problem type slug is "work-unit-cancelled"
    And the RELOCATE WorkUnit "plan-A-move-0" is in state "Cancelled"
