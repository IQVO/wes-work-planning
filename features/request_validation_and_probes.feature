# The rebalance read without a labor plan, and the liveness/readiness probes.
#
# Derived from:
#   - POST /paths/{pathId}/rebalance-decisions: the labor plan is part of the answer
#     only when Workforce Management reported one
#   - apis/openapi.yaml /healthz and /readyz (ADR-0023 graceful shutdown):
#     liveness never flips; readiness flips to not-ready first on shutdown
# The shift plan and charge forecast validation moved to warehouse-planning
# with their aggregates (ADR-0049); features/planning_moved.feature pins the
# 410 both routes answer now.

Feature: Rebalancing without an invented labor plan, and reporting health
  As the WES conductor
  I want a rebalance answer to carry only the labor plan Workforce Management reported
  So that flow balancing is never skewed by a fabricated plan and a draining pod leaves rotation cleanly

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: A rebalance decision does not invent a labor plan
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When the rebalance decision is requested for process path "pick-zone-a"
    Then the request is accepted with status 201
    And the rebalance recommendation carries no labor plan

  @bdd
  Scenario: A rebalance decision includes the labor plan Workforce Management reported
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And Workforce Management committed a labor plan of 6 heads at 90 units per hour for 8 hours for process path "pick-zone-a"
    When the rebalance decision is requested for process path "pick-zone-a"
    Then the rebalance recommendation includes the observed labor plan with 6 planned heads

  @bdd
  Scenario: Liveness reports ok
    When the liveness probe is requested
    Then the request is accepted with status 200
    And the probe reports status "ok"

  @bdd
  Scenario: Readiness reports ready while serving
    When the readiness probe is requested
    Then the request is accepted with status 200

  @bdd
  Scenario: Readiness flips to not ready on graceful shutdown while liveness stays up
    When the service begins graceful shutdown
    And the readiness probe is requested
    Then the request is rejected with status 503
    When the liveness probe is requested
    Then the request is accepted with status 200
    And the probe reports status "ok"
