# EnqueueWorkUnit scenarios beyond the happy path.
#
# Derived from:
#   - apis/openapi.yaml, POST /paths/{pathId}/work-units: 201 with a
#     Location of /work-units/{id} and the unit in state Pending; 409
#     work-unit-already-enqueued for a repeated workUnitId; 400 for a
#     malformed body, a failed validation or an unknown pathId
#   - docs/docs/adr/0010-gift-wrap-as-a-work-released-characteristic.md and
#     0036-work-unit-line-no-on-work-released.md: sku, giftWrap and lineNo
#     are optional characteristics carried on the unit
#   - docs/docs/adr/0012-process-path-catalogue-validation.md: a write that
#     can seed an aggregate rejects a path outside the catalogue

Feature: Enqueueing work units onto a process path
  As the WES conductor
  I want every unit of work enqueued exactly once with the characteristics it was given
  So that release hands out complete, unambiguous work

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: An enqueued unit is Pending and addressable
    When a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-77213-line-1" is submitted to process path "pick-zone-a"
    Then the request is accepted with status 201
    And the response Location is "/work-units/wu-1"
    And the WorkUnit in the response has id "wu-1"
    And the WorkUnit in the response has pathId "pick-zone-a"
    And the WorkUnit in the response has cpt "2026-08-21T12:00:00Z"
    And the WorkUnit in the response has reference "order-77213-line-1"
    And the WorkUnit in the response is in state "Pending"

  @bdd
  Scenario: Optional characteristics are omitted when none were given
    When a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-77213-line-1" is submitted to process path "pick-zone-a"
    Then the WorkUnit in the response has no sku
    And the WorkUnit in the response has no lineNo
    And the WorkUnit in the response has no releasedAt
    And the WorkUnit in the response has no completedAt
    And the WorkUnit in the response does not request gift wrap

  @bdd
  Scenario: Characteristics given at enqueue are kept on the unit
    When a WorkUnit "wu-1" is submitted to process path "pick-zone-a" with SKU "SKU-9", gift wrap requested and line number 3
    Then the request is accepted with status 201
    And the WorkUnit in the response has sku "SKU-9"
    And the WorkUnit in the response requests gift wrap
    And the WorkUnit in the response has line number 3

  @bdd
  Scenario: Release and completion stamp the unit with the clock
    Given a WorkUnit "wu-1" is submitted to process path "pick-zone-a" with SKU "SKU-9", gift wrap requested and line number 3
    When work is released from process path "pick-zone-a"
    Then the request is accepted with status 200
    And the released WorkUnit is in state "Released"
    And the WorkUnit in the response has releasedAt "2026-08-21T08:00:00Z"
    And the WorkUnit in the response has sku "SKU-9"
    And the WorkUnit in the response requests gift wrap
    And the WorkUnit in the response has line number 3
    When the WorkUnit "wu-1" is recorded as completed
    Then the WorkUnit in the response is in state "Completed"
    And the WorkUnit in the response has completedAt "2026-08-21T08:00:00Z"

  @bdd
  Scenario: A work unit id can be enqueued only once per pool
    Given a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When a WorkUnit "wu-1" with CPT "2026-08-21T14:00:00Z" and reference "order-line-2" is submitted to process path "pick-zone-a"
    Then the request is rejected with status 409
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/work-unit-already-enqueued"
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: Pools are per path, so the same id may be enqueued on two paths
    Given a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is submitted to process path "pack-zone-a"
    Then the request is accepted with status 201
    And the Work Pool telemetry for process path "pack-zone-a" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: The first enqueue provisions a release-fed pool
    Given a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When the Work Pool telemetry for process path "pick-zone-a" is sampled
    Then the Work Pool telemetry for process path "pick-zone-a" reports feed mode "ReleaseFed"

  @bdd
  Scenario: An enqueue is announced as a created work unit
    When a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is submitted to process path "pick-zone-a"
    Then 1 "WorkUnitCreated" event was published

  @bdd
  Scenario: A rejected enqueue announces nothing
    Given a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is submitted to process path "pick-zone-a"
    Then the request is rejected with status 409
    And 1 "WorkUnitCreated" event was published

  @bdd
  Scenario: A path outside the catalogue cannot receive work
    When a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is submitted to process path "unknown-zone"
    Then the request is rejected with status 400

  @bdd
  Scenario Outline: An enqueue request that fails validation is rejected
    When an enqueue request with the body <body> is submitted to process path "pick-zone-a"
    Then the request is rejected with status 400

    Examples:
      | body                                                                  |
      | {"cpt": "2026-08-21T12:00:00Z", "reference": "order-line-1"}          |
      | {"workUnitId": "wu-1", "reference": "order-line-1"}                   |
      | {"workUnitId": "wu-1", "cpt": "tomorrow", "reference": "order-line-1"} |
      | {"workUnitId": "wu-1", "cpt": "2026-08-21T12:00:00Z"}                 |
      | {"workUnitId": "wu-1", "cpt": "2026-08-21T12:00:00Z", "reference":    |
      | []                                                                    |

  @bdd
  Scenario: A negative line number is rejected
    When a WorkUnit "wu-1" is submitted to process path "pick-zone-a" with line number -1
    Then the request is rejected with status 400
