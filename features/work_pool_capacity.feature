# Scenarios in this file derive from:
#   - docs/docs/adr/0018-path-capacity-changed.md — "Decision" §3 (trigger):
#     `GET /paths/{pathId}/telemetry` "gains an optional cutoffAt query
#     parameter (RFC3339). Supplying it makes the response additionally
#     report remainingCapacityKnown/remainingCapacityUnits ... Omitting it
#     (the default) leaves every existing caller's behavior byte-for-byte
#     unchanged"; §2 ("The ReleaseFed/FlowFed Known rule"):
#     RemainingUnits = wipLimit - WIP() for a ReleaseFed pool.
#   - apis/openapi.yaml — "sampleBacklog" cutoffAt parameter and
#     BacklogSnapshotResponse (remainingCapacityKnown /
#     remainingCapacityUnits present only when cutoffAt was supplied);
#     "releaseNextWork" 409 response ("a release-fed pool is already at
#     its WIP limit", type wip-limit-reached); PoolMode schema
#     ("ReleaseFed pools ... enforce a hard WIP limit").
#   - .claude/rules/domain-model.md — WorkPool invariant: "WIP limit is an
#     enforceable invariant on release-fed pools".
#   - docs/docs/api/errors.md — "The full catalogue", 409 table:
#     wip-limit-reached ("WIP-limit backpressure").
Feature: Release-fed Work Pool admission capacity and WIP backpressure
  As the WES conductor
  I want a release-fed pool's remaining admission capacity reported per CPT cutoff and its WIP limit enforced
  So that upstream promise calculations rest on a real ceiling and a saturated path pushes back instead of over-admitting

  Background:
    Given the WES Work Planning service is running

  # The WIP limit is provisioned on the pool directly (the REST surface
  # auto-provisions pools with the fleet default of 1000), mirroring how
  # installedStations is seeded for ShiftPlan scenarios.
  @bdd
  Scenario: Sampling telemetry with a CPT cutoff reports remaining admission capacity
    Given a release-fed Work Pool with a WIP limit of 2 is provisioned for process path "pick-zone-a"
    And a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-2" with CPT "2026-08-21T16:00:00Z" and reference "order-line-2" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    And the released WorkUnit is "wu-1"
    When the Work Pool telemetry for process path "pick-zone-a" is sampled with a CPT cutoff of "2026-08-21T18:00:00Z"
    Then the request is accepted with status 200
    And the telemetry reports remaining admission capacity of 1 units
    And the Work Pool telemetry for process path "pick-zone-a" does not report remaining capacity

  @bdd
  Scenario: Releasing past a release-fed pool's WIP limit is rejected
    Given a release-fed Work Pool with a WIP limit of 2 is provisioned for process path "pick-zone-a"
    And a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-2" with CPT "2026-08-21T12:00:00Z" and reference "order-line-2" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-3" with CPT "2026-08-21T16:00:00Z" and reference "order-line-3" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    And the released WorkUnit is "wu-1"
    And work is released from process path "pick-zone-a"
    And the released WorkUnit is "wu-2"
    When work is released from process path "pick-zone-a"
    Then the request is rejected with status 409
    And the problem detail title is "Release-fed pool WIP limit reached"
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/wip-limit-reached"
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 1 and WIP 2
