# Documentation-derived rules of pick-face replenishment work.
#
# Sources:
#   - warehouse-docs docs/contexts/wes-work-planning/business-context.md
#     "Pick-face replenishment is work too" and use-cases.md inbound table
#     rows ApplyReplenishmentRequested / ApplyReplenishmentCancelled;
#   - docs/docs/adr/0042-pick-face-replenishment-work.md decisions 3 (one
#     REPLENISH unit per request id, id replenish-<request_id>), 4 (value
#     invariants), 5 (priority is the CPT: NORMAL +8h, URGENT +1h), 6 (the
#     optional replenishment object on WorkReleased, never relocation or
#     transfer fields), 7 (cancellation only while Pending, no event);
#   - domain-events.md consumed table: ReplenishmentRequested /
#     ReplenishmentCancelled on warehouse.pick-face-replenishment.events.
# The ReplenishmentRequested/Cancelled steps drive the same use cases the
# replenishment consumer calls; every outcome is read back through the real
# REST surface or the real outbound Kafka publisher's wire form.
Feature: Pick-face replenishment rules from the documentation
  As the WES planner
  I want every replenishment request planned, prioritized and cancelled exactly as documented
  So that a forward pick face is refilled before it starves and no cancelled refill is still worked

  Background:
    Given the WES Work Planning service is running
    And the process-path catalogue declares REPLENISH

  @bdd
  Scenario: A request becomes one REPLENISH unit due eight hours later under the request id
    When pick-face-replenishment publishes ReplenishmentRequested event "evt-r1" for request "req-100" of 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "NORMAL" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    Then the ReplenishmentRequested event is applied
    And 1 "WorkUnitCreated" event was published
    When the WorkUnit "replenish-req-100" is requested by its id
    Then the WorkUnit in the response has id "replenish-req-100"
    And the WorkUnit in the response has pathId "REPLENISH"
    And the WorkUnit in the response has cpt "2026-08-21T17:00:00Z"
    And the WorkUnit in the response has reference "req-100"
    And the WorkUnit in the response is in state "Pending"
    And the Work Pool telemetry for process path "REPLENISH" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: An URGENT request is due one hour out and is released ahead of an older NORMAL request
    When pick-face-replenishment publishes ReplenishmentRequested event "evt-r1" for request "req-norm" of 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "NORMAL" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    And the ReplenishmentRequested event is applied
    And pick-face-replenishment publishes ReplenishmentRequested event "evt-r2" for request "req-urg" of 3 units of SKU "SKU-U" from bin "RSV-B02" to bin "FWD-B01" with priority "URGENT" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    And the ReplenishmentRequested event is applied
    When the WorkUnit "replenish-req-urg" is requested by its id
    Then the WorkUnit in the response has cpt "2026-08-21T10:00:00Z"
    When work is released from process path "REPLENISH"
    Then the released WorkUnit is "replenish-req-urg"
    When work is released from process path "REPLENISH"
    Then the released WorkUnit is "replenish-req-norm"

  @bdd
  Scenario: Released REPLENISH work carries the replenishment object and nothing it must not
    When pick-face-replenishment publishes ReplenishmentRequested event "evt-r1" for request "req-100" of 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "URGENT" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    And the ReplenishmentRequested event is applied
    And work is released from process path "REPLENISH"
    Then the released WorkUnit is "replenish-req-100"
    And the "WorkReleased" wire data replenishes request "req-100" with 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "URGENT"
    And the "WorkReleased" wire data field "site_id" is "SITE-1"
    And the "WorkReleased" wire data omits "relocation"
    And the "WorkReleased" wire data omits "work_kind"
    And the "WorkReleased" wire data omits "transfer_ref"
    And the "WorkReleased" wire data omits "quantity"

  @bdd
  Scenario Outline: A broken request is deterministic poison and plans nothing
    When pick-face-replenishment publishes ReplenishmentRequested event "evt-broken" for request "<request>" of <qty> units of SKU "<sku>" from bin "<from>" to bin "<to>" with priority "<priority>" at site "<site>" requested at "<requested_at>"
    Then the ReplenishmentRequested event is refused as poison
    And 0 "WorkUnitCreated" events were published
    When work units are looked up by reference "req-broken"
    Then the work unit lookup returns 0 work units

    Examples:
      | request  | sku   | from    | to      | qty | priority | site   | requested_at          | why                     |
      |          | SKU-R | RSV-A07 | FWD-A01 | 6   | NORMAL   | SITE-1 | 2026-08-21T09:00:00Z  | no request id           |
      | req-b    |       | RSV-A07 | FWD-A01 | 6   | NORMAL   | SITE-1 | 2026-08-21T09:00:00Z  | no sku                  |
      | req-b    | SKU-R |         | FWD-A01 | 6   | NORMAL   | SITE-1 | 2026-08-21T09:00:00Z  | no source bin           |
      | req-b    | SKU-R | RSV-A07 |         | 6   | NORMAL   | SITE-1 | 2026-08-21T09:00:00Z  | no forward bin          |
      | req-b    | SKU-R | RSV-A07 | RSV-A07 | 6   | NORMAL   | SITE-1 | 2026-08-21T09:00:00Z  | the bins do not differ  |
      | req-b    | SKU-R | RSV-A07 | FWD-A01 | 0   | NORMAL   | SITE-1 | 2026-08-21T09:00:00Z  | zero quantity           |
      | req-b    | SKU-R | RSV-A07 | FWD-A01 | -3  | NORMAL   | SITE-1 | 2026-08-21T09:00:00Z  | negative quantity       |
      | req-b    | SKU-R | RSV-A07 | FWD-A01 | 6   | WHENEVER | SITE-1 | 2026-08-21T09:00:00Z  | unknown priority        |
      | req-b    | SKU-R | RSV-A07 | FWD-A01 | 6   | NORMAL   |        | 2026-08-21T09:00:00Z  | no site id              |
      | req-b    | SKU-R | RSV-A07 | FWD-A01 | 6   | NORMAL   | SITE-1 |                       | no requested at       |

  @bdd
  Scenario: A REPLENISH path missing from the catalogue refuses the request until it is declared and replayed
    Given the process-path catalogue does not declare REPLENISH
    When pick-face-replenishment publishes ReplenishmentRequested event "evt-r1" for request "req-100" of 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "NORMAL" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    Then the ReplenishmentRequested event is refused as poison
    And the process-path catalogue declares REPLENISH
    When pick-face-replenishment publishes ReplenishmentRequested event "evt-r1" for request "req-100" of 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "NORMAL" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    Then the ReplenishmentRequested event is applied
    And the Work Pool telemetry for process path "REPLENISH" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: A different event for an already-planned request plans nothing more, and a redelivery of the first event is a no-op
    Given pick-face-replenishment publishes ReplenishmentRequested event "evt-r1" for request "req-200" of 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "NORMAL" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    And the ReplenishmentRequested event is applied
    When pick-face-replenishment publishes ReplenishmentRequested event "evt-r2" for request "req-200" of 9 units of SKU "SKU-R" from bin "RSV-C03" to bin "FWD-A02" with priority "URGENT" at site "SITE-1" requested at "2026-08-21T10:00:00Z"
    Then the ReplenishmentRequested event is a no-op because the request is already planned
    When pick-face-replenishment publishes ReplenishmentRequested event "evt-r1" for request "req-200" of 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "NORMAL" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    Then the ReplenishmentRequested event is ignored as a redelivery
    And 1 "WorkUnitCreated" event was published
    And the Work Pool telemetry for process path "REPLENISH" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: A cancellation while the unit is still Pending cancels it, withdraws it and publishes nothing
    Given pick-face-replenishment publishes ReplenishmentRequested event "evt-r1" for request "req-300" of 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "NORMAL" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    And the ReplenishmentRequested event is applied
    When pick-face-replenishment publishes ReplenishmentCancelled event "evt-c1" for request "req-300"
    Then the ReplenishmentCancelled event cancels the pending unit
    And the WorkUnit "replenish-req-300" is requested by its id
    And the WorkUnit in the response is in state "Cancelled"
    And the WorkUnit in the response was cancelled at "2026-08-21T08:00:00Z"
    And the Work Pool telemetry for process path "REPLENISH" reports backlog depth 0 and WIP 0
    When work is released from process path "REPLENISH"
    Then the request is rejected with status 409
    And the problem type slug is "work-pool-empty"
    When pick-face-replenishment publishes ReplenishmentCancelled event "evt-c1" for request "req-300"
    Then the ReplenishmentCancelled event is ignored as a redelivery
    And 1 "WorkUnitCreated" event was published

  @bdd
  Scenario: A cancellation after the unit was released leaves it running to completion
    Given pick-face-replenishment publishes ReplenishmentRequested event "evt-r1" for request "req-400" of 6 units of SKU "SKU-R" from bin "RSV-A07" to bin "FWD-A01" with priority "NORMAL" at site "SITE-1" requested at "2026-08-21T09:00:00Z"
    And the ReplenishmentRequested event is applied
    And work is released from process path "REPLENISH"
    And the released WorkUnit is "replenish-req-400"
    When pick-face-replenishment publishes ReplenishmentCancelled event "evt-c1" for request "req-400"
    Then the ReplenishmentCancelled event leaves the unit running
    When the WorkUnit "replenish-req-400" is recorded as completed
    Then the request is accepted with status 200
    And the WorkUnit in the response is in state "Completed"

  @bdd
  Scenario: A cancellation for a request this context never planned is a no-op
    When pick-face-replenishment publishes ReplenishmentCancelled event "evt-c9" for request "req-ghost"
    Then the ReplenishmentCancelled event is a no-op for a request this context never planned
    When the WorkUnit "replenish-req-ghost" is requested by its id
    Then the request is rejected with status 404

  @bdd
  Scenario: A cancellation with a blank request id is refused as deterministic poison
    When pick-face-replenishment publishes ReplenishmentCancelled event "evt-c0" with a blank request id
    Then the ReplenishmentCancelled event is refused as poison
