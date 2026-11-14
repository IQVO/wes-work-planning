# Documentation-derived rules of the Work Pool: release order and its error
# rows, flow-fed pools, the alarm threshold boundary, telemetry parameters
# and the flow-balancing decision matrix.
#
# Sources:
#   - warehouse-docs docs/contexts/wes-work-planning/aggregate-design-canvas.md
#     WorkPool invariants W1-W7 and the corrective policies, and use-cases.md
#     ReleaseNextWork / SampleBacklog / RebalanceDecision;
#   - troubleshooting.md "HTTP problem types" rows (404 not-found on
#     work-releases/telemetry/rebalance-decisions, 409 work-pool-empty,
#     400 malformed-request-body for an empty or invalid cutoffAt);
#   - docs/docs/adr/0003-flow-balancing-as-domain-service.md and
#     0018-path-capacity-changed.md.
Feature: Work Pool rules from the documentation
  As the WES conductor
  I want release, telemetry and flow balancing to follow the documented pool rules at their boundaries
  So that a path is neither starved nor flooded and every signal is raised exactly when the documentation says

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: Work with the same CPT is released in the order it arrived
    Given a WorkUnit "wu-b" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-a" with CPT "2026-08-21T12:00:00Z" and reference "order-line-2" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-c" with CPT "2026-08-21T12:00:00Z" and reference "order-line-3" is enqueued to process path "pick-zone-a"
    When work is released from process path "pick-zone-a"
    Then the released WorkUnit is "wu-b"
    When work is released from process path "pick-zone-a"
    Then the released WorkUnit is "wu-a"
    When work is released from process path "pick-zone-a"
    Then the released WorkUnit is "wu-c"

  @bdd
  Scenario Outline: A missing pool or work unit answers not found and announces nothing
    When a <method> request is sent to "<path>"
    Then the request is rejected with status 404
    And the problem type slug is "not-found"
    And 0 "<event>" events were published

    Examples:
      | method | path                                                      | event               |
      | POST   | /paths/pick-zone-a/work-releases                          | WorkReleased        |
      | POST   | /paths/pick-zone-a/rebalance-decisions                    | PathThrottled       |
      | GET    | /paths/pick-zone-a/telemetry                              | BacklogThresholdBreached |
      | GET    | /paths/pick-zone-a/telemetry?cutoffAt=2026-08-21T18:00:00Z | PathCapacityChanged |
      | POST   | /work-units/wu-ghost/completions                          | WorkUnitCompleted   |

  @bdd
  Scenario: A configured pool with nothing pending is empty, which is not the same as having no pool
    Given the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 5
    When work is released from process path "pick-zone-a"
    Then the request is rejected with status 409
    And the problem type slug is "work-pool-empty"
    And 0 "WorkReleased" events were published

  @bdd
  Scenario: A release is announced once for the unit it handed out and a refused release announces nothing
    Given a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When work is released from process path "pick-zone-a"
    Then 1 "WorkReleased" event was published
    And the last "WorkReleased" event is for WorkUnit "wu-1" on process path "pick-zone-a"
    When work is released from process path "pick-zone-a"
    Then the request is rejected with status 409
    And 1 "WorkReleased" event was published

  @bdd
  Scenario: A flow-fed pool does not enforce its WIP limit when releasing
    Given the pool for process path "pick-zone-a" is configured as "FlowFed" with a WIP limit of 2
    And 3 WorkUnits are enqueued to process path "pick-zone-a"
    When 3 WorkUnits are released from process path "pick-zone-a"
    Then the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 0 and WIP 3

  @bdd
  Scenario: Completing a unit announces it once, frees exactly its slot and a second completion announces nothing
    Given a release-fed Work Pool with a WIP limit of 2 is provisioned for process path "pick-zone-a"
    And 2 WorkUnits are enqueued to process path "pick-zone-a"
    And 2 WorkUnits are released from process path "pick-zone-a"
    When the WorkUnit "wu-0000" is recorded as completed
    Then 1 "WorkUnitCompleted" event was published
    And the last "WorkUnitCompleted" event is for WorkUnit "wu-0000" on process path "pick-zone-a"
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 0 and WIP 1
    When the WorkUnit "wu-0000" is recorded as completed
    Then the request is rejected with status 409
    And the problem type slug is "work-unit-already-completed"
    And 1 "WorkUnitCompleted" event was published
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 0 and WIP 1

  @bdd
  Scenario: A unit that was never released cannot complete and the pool is untouched
    Given a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When the WorkUnit "wu-1" is recorded as completed
    Then the request is rejected with status 409
    And the problem type slug is "work-unit-not-released"
    And 0 "WorkUnitCompleted" events were published
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 1 and WIP 0

  @bdd
  Scenario: A backlog exactly at the alarm threshold raises nothing and one more unit breaches it
    Given 1000 WorkUnits are enqueued to process path "pick-zone-a"
    When the Work Pool telemetry for process path "pick-zone-a" is sampled
    Then the request is accepted with status 200
    And 0 "BacklogThresholdBreached" events were published
    When a WorkUnit "wu-extra" with CPT "2026-08-21T12:00:00Z" and reference "order-line-extra" is enqueued to process path "pick-zone-a"
    And the Work Pool telemetry for process path "pick-zone-a" is sampled
    Then the telemetry reports being over its alarm threshold
    And 1 "BacklogThresholdBreached" event was published

  @bdd
  Scenario: Sampling raises PathCapacityChanged only when a cutoff is supplied
    Given a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When the Work Pool telemetry for process path "pick-zone-a" is sampled
    Then 0 "PathCapacityChanged" events were published
    When the Work Pool telemetry for process path "pick-zone-a" is sampled with a CPT cutoff of "2026-08-21T18:00:00Z"
    Then 1 "PathCapacityChanged" event was published
    And the PathCapacityChanged event reports a cutoff of "2026-08-21T18:00:00Z" with 1000 remaining units

  @bdd
  Scenario: A flow-fed pool reports unknown remaining capacity for a cutoff rather than a number
    Given the pool for process path "pick-zone-a" is configured as "FlowFed" with a WIP limit of 5
    When the Work Pool telemetry for process path "pick-zone-a" is sampled with a CPT cutoff of "2026-08-21T18:00:00Z"
    Then the telemetry reports that remaining admission capacity is not known
    And the PathCapacityChanged event reports a cutoff of "2026-08-21T18:00:00Z" with unknown remaining capacity

  @bdd
  Scenario Outline: A cutoffAt that is empty or not an RFC 3339 date-time is refused
    Given a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When a GET request is sent to "/paths/pick-zone-a/telemetry?cutoffAt=<value>"
    Then the request is rejected with status 400
    And the problem type slug is "malformed-request-body"
    And 0 "PathCapacityChanged" events were published

    Examples:
      | value      |
      |            |
      | tomorrow   |
      | 2026-08-21 |

  @bdd
  Scenario Outline: A rebalance decision follows the feed mode, the WIP limit and the backlog
    Given the pool for process path "pick-zone-a" is configured as "<mode>" with a WIP limit of <limit>
    And <enqueued> WorkUnits are enqueued to process path "pick-zone-a"
    And <released> WorkUnits are released from process path "pick-zone-a"
    When the rebalance decision is requested for process path "pick-zone-a"
    Then the request is accepted with status 201
    And the rebalance recommendation action is "<action>"
    And <flagged> "LaborReassignmentFlagged" events were published
    And 0 "PathThrottled" events were published

    Examples:
      | mode       | limit | enqueued | released | action         | flagged |
      | ReleaseFed | 2     | 3        | 2        | ReassignLabor  | 1       |
      | ReleaseFed | 2     | 2        | 2        | NoActionNeeded | 0       |
      | ReleaseFed | 2     | 3        | 1        | NoActionNeeded | 0       |
      | FlowFed    | 2     | 3        | 2        | NoActionNeeded | 0       |

  @bdd
  Scenario Outline: A flow-fed path is told to throttle only when its backlog exceeds the alarm threshold
    Given the pool for process path "pick-zone-a" is configured as "FlowFed" with a WIP limit of 50
    And <backlog> WorkUnits are enqueued to process path "pick-zone-a"
    When the rebalance decision is requested for process path "pick-zone-a"
    Then the request is accepted with status 201
    And the rebalance recommendation action is "<action>"
    And the rebalance recommendation reports backlog depth <backlog> and WIP 0
    And <throttled> "PathThrottled" events were published
    And 0 "LaborReassignmentFlagged" events were published

    Examples:
      | backlog | action           | throttled |
      | 1000    | NoActionNeeded   | 0         |
      | 1001    | ThrottleUpstream | 1         |

  @bdd
  Scenario: A release-fed path over the alarm threshold but below its WIP limit needs no rebalancing
    Given the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 2000
    And 1001 WorkUnits are enqueued to process path "pick-zone-a"
    When the rebalance decision is requested for process path "pick-zone-a"
    Then the rebalance recommendation action is "NoActionNeeded"
    And 0 "PathThrottled" events were published
    And 0 "LaborReassignmentFlagged" events were published

  @bdd
  Scenario Outline: A WIP limit up to the Postgres INTEGER maximum is accepted and above it is refused
    When the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of <limit>
    Then the request is <outcome> with status <status>

    Examples:
      | limit      | outcome  | status |
      | 1          | accepted | 200    |
      | 2147483647 | accepted | 200    |
      | 2147483648 | rejected | 400    |
