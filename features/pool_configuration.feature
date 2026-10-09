# ConfigurePool command scenarios.
#
# Derived from:
#   - docs/docs/adr/0034-configure-pool-command.md: PUT /paths/{pathId}/pool
#     sets a pool's feed mode and WIP limit and creates the pool when none
#     exists; mode must be ReleaseFed|FlowFed (400 unknown-feed-mode), the
#     WIP limit a positive integer (400 invalid-wip-limit), pathId is
#     validated against the process-path catalogue (ADR-0012); a rejected
#     command changes nothing and creates no pool; lowering a limit below
#     the current WIP never evicts work -- releases answer 409
#     wip-limit-reached until WIP drops strictly below the new limit and
#     remaining capacity reports 0; raising takes effect on the next
#     release; an identical repeat is idempotent
#   - apis/openapi.yaml, PUT /paths/{pathId}/pool (200 pool with live wip and
#     backlogDepth; 400; 409)

Feature: Configuring a path's Work Pool
  As an operator
  I want to set how a path's pool is fed and how much work may be in progress
  So that I can change the admission ceiling at runtime without a deploy

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: Configuring a path creates its pool
    When the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 50
    Then the request is accepted with status 200
    And the pool response reports mode "ReleaseFed" with a WIP limit of 50, WIP 0 and backlog depth 0

  @bdd
  Scenario: A flow-fed pool is reachable and shows in the telemetry
    When the pool for process path "pick-zone-a" is configured as "FlowFed" with a WIP limit of 20
    Then the request is accepted with status 200
    And the pool response reports mode "FlowFed" with a WIP limit of 20, WIP 0 and backlog depth 0
    When the Work Pool telemetry for process path "pick-zone-a" is sampled
    Then the Work Pool telemetry for process path "pick-zone-a" reports feed mode "FlowFed"

  @bdd
  Scenario: The response carries the live WIP and backlog of an existing pool
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-2" with CPT "2026-08-21T12:00:00Z" and reference "order-line-2" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-3" with CPT "2026-08-21T14:00:00Z" and reference "order-line-3" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    When the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 10
    Then the pool response reports mode "ReleaseFed" with a WIP limit of 10, WIP 1 and backlog depth 2

  @bdd
  Scenario: Repeating the same configuration is idempotent
    Given the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 7
    When the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 7
    Then the request is accepted with status 200
    And the pool response reports mode "ReleaseFed" with a WIP limit of 7, WIP 0 and backlog depth 0

  @bdd
  Scenario: Switching the mode of an existing pool keeps its work
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When the pool for process path "pick-zone-a" is configured as "FlowFed" with a WIP limit of 5
    Then the pool response reports mode "FlowFed" with a WIP limit of 5, WIP 0 and backlog depth 1
    When work is released from process path "pick-zone-a"
    Then the released WorkUnit is "wu-1"

  @bdd
  Scenario: Lowering the limit below the current WIP never evicts work
    Given the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 5
    And a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-2" with CPT "2026-08-21T12:00:00Z" and reference "order-line-2" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-3" with CPT "2026-08-21T14:00:00Z" and reference "order-line-3" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    When the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 1
    Then the request is accepted with status 200
    And the pool response reports mode "ReleaseFed" with a WIP limit of 1, WIP 2 and backlog depth 1

  @bdd
  Scenario: A saturated pool pauses releases until completions bring WIP strictly below the limit
    Given the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 5
    And a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-2" with CPT "2026-08-21T12:00:00Z" and reference "order-line-2" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-3" with CPT "2026-08-21T14:00:00Z" and reference "order-line-3" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    And the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 1
    When work is released from process path "pick-zone-a"
    Then the request is rejected with status 409
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/wip-limit-reached"
    When the WorkUnit "wu-1" is recorded as completed
    And work is released from process path "pick-zone-a"
    Then the request is rejected with status 409
    When the WorkUnit "wu-2" is recorded as completed
    And work is released from process path "pick-zone-a"
    Then the request is accepted with status 200
    And the released WorkUnit is "wu-3"

  @bdd
  Scenario: A saturated pool reports no remaining admission capacity
    Given the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 5
    And a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-2" with CPT "2026-08-21T12:00:00Z" and reference "order-line-2" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    And the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 1
    When the Work Pool telemetry for process path "pick-zone-a" is sampled with a CPT cutoff of "2026-08-21T18:00:00Z"
    Then the telemetry reports remaining admission capacity of 0 units

  @bdd
  Scenario: Raising the limit takes effect on the next release
    Given the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 1
    And a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-2" with CPT "2026-08-21T12:00:00Z" and reference "order-line-2" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    When work is released from process path "pick-zone-a"
    Then the request is rejected with status 409
    When the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 2
    And work is released from process path "pick-zone-a"
    Then the request is accepted with status 200
    And the released WorkUnit is "wu-2"

  @bdd
  Scenario Outline: A WIP limit must be a positive integer
    When the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of <limit>
    Then the request is rejected with status 400
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/invalid-wip-limit"

    Examples:
      | limit |
      | 0     |
      | -1    |
      | -100  |

  @bdd
  Scenario: An unknown feed mode is rejected
    When the pool for process path "pick-zone-a" is configured as "Hybrid" with a WIP limit of 10
    Then the request is rejected with status 400
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/unknown-feed-mode"

  @bdd
  Scenario: A rejected configuration creates no pool
    When the pool for process path "pick-zone-a" is configured as "Hybrid" with a WIP limit of 10
    Then the request is rejected with status 400
    When the Work Pool telemetry for process path "pick-zone-a" is sampled
    Then the request is rejected with status 404

  @bdd
  Scenario: A path outside the process-path catalogue cannot be configured
    When the pool for process path "unknown-zone" is configured as "ReleaseFed" with a WIP limit of 10
    Then the request is rejected with status 400

  @bdd
  Scenario: A malformed configuration body is rejected
    When the pool for process path "pick-zone-a" is configured with the body {"mode": "ReleaseFed", "wipLimit":
    Then the request is rejected with status 400

  @bdd
  Scenario: Configuring a pool announces nothing
    When the pool for process path "pick-zone-a" is configured as "ReleaseFed" with a WIP limit of 10
    Then 0 "PathCapacityChanged" events were published
