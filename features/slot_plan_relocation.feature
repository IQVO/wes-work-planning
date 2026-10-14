# Source:
#   - docs/docs/adr/0040-slot-plan-relocation-work.md — "One RELOCATE work
#     unit per move", "Sequencing: vacate before fill", "Supersession per
#     site" and "Idempotency".
#   - apis/openapi.yaml — WorkUnitResponse.relocation (RelocationMove) and
#     the Cancelled WorkUnitState.
# The SlotPlanApproved steps drive ApplySlotPlanApproved, the same use case
# the warehouse.slotting-optimization.events consumer calls; everything
# else is the real REST surface.
Feature: Approved slot plans become sequenced RELOCATE work
  As the WES planner
  I want every move of an approved slot plan planned as RELOCATE floor work
  So that the floor relocates stock in an order that never fills an occupied slot

  @bdd
  Scenario: Each move becomes one RELOCATE work unit and a fill waits for its vacate
    Given slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind     | from_slot | to_slot |
      | SKU-1 | Relocate | S2        | S3      |
      | SKU-2 | Vacate   | S3        |         |
    When the WorkUnit "plan-A-move-0" is requested by its id
    Then the WorkUnit in the response is in state "Pending"
    And the WorkUnit in the response relocates SKU "SKU-1" from "S2" to "S3" as move 0 of plan "plan-A"
    And the WorkUnit in the response waits on "plan-A-move-1"
    When work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-A-move-1"
    When work is released from process path "RELOCATE"
    Then the request is rejected with status 409
    When the WorkUnit "plan-A-move-1" is recorded as completed
    And work is released from process path "RELOCATE"
    Then the released WorkUnit is "plan-A-move-0"

  @bdd
  Scenario: A newer plan for the site cancels the older plan's unreleased moves
    Given slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Vacate | S1        |         |
      | SKU-2 | Assign |           | S9      |
    And work is released from process path "RELOCATE"
    When slotting-optimization approves slot plan "plan-B" for site "SITE-1" at "2026-08-21T07:30:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-7 | Assign |           | S7      |
    Then the RELOCATE WorkUnit "plan-A-move-0" is in state "Released"
    And the RELOCATE WorkUnit "plan-A-move-1" is in state "Cancelled"
    And the RELOCATE WorkUnit "plan-B-move-0" is in state "Pending"

  @bdd
  Scenario: An older plan arriving late plans nothing
    Given slotting-optimization approves slot plan "plan-B" for site "SITE-1" at "2026-08-21T07:30:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-7 | Assign |           | S7      |
    When slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Vacate | S1        |         |
    And work units are looked up by reference "plan-A"
    Then the work unit lookup returns 0 work units
    And the RELOCATE WorkUnit "plan-B-move-0" is in state "Pending"

  @bdd
  Scenario: A redelivered SlotPlanApproved plans nothing more
    Given slotting-optimization approves slot plan "plan-A" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-1 | Assign |           | S1      |
    When the SlotPlanApproved event is redelivered
    And work units are looked up by reference "plan-A"
    Then the work unit lookup returns 1 work units

  @bdd
  Scenario: A plan with an unknown move kind is refused and plans nothing
    When slotting-optimization approves slot plan "plan-X" for site "SITE-1" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind | from_slot | to_slot |
      | SKU-1 | Swap | S1        | S2      |
    Then the slot plan is refused as poison
    When work units are looked up by reference "plan-X"
    Then the work unit lookup returns 0 work units
