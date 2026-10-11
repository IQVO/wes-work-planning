# Request validation of the shift plan and charge forecast, the rebalance
# read without a labor plan, and the liveness/readiness probes.
#
# Derived from:
#   - apis/openapi.yaml, POST /paths/{pathId}/plan 400: "Malformed body, a
#     missing required field (plannedHeads, installedStations,
#     rateUnitsPerHour, hours -- zero-valued defaults are never silently
#     accepted), an invalid field value (e.g. non-positive rate or hours),
#     the plannedHeads <= installedStations invariant was violated, or the
#     computed plannedThroughput (rate x heads x hours) is not representable
#     as a finite number."; 201 carries a Location of /paths/{id}/plan
#   - apis/openapi.yaml, POST /paths/{pathId}/charge: 201 with the bucket
#     total and a Location of /paths/{id}/charge; 400 for an invalid body
#   - docs/docs/adr/0017-travel-distance-lookup-on-commit-shift-plan.md: a
#     travel distance hint is only present when one was requested
#   - GET /paths/{pathId}/rebalance: the labor plan is part of the answer
#     only when Workforce Management reported one
#   - apis/openapi.yaml /healthz and /readyz (ADR-0023 graceful shutdown):
#     liveness never flips; readiness flips to not-ready first on shutdown

Feature: Rejecting invalid plans and forecasts, and reporting health
  As the WES conductor
  I want malformed commitments refused without side effects
  So that a bad plan never skews flow balancing and a draining pod leaves rotation cleanly

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: A valid shift plan is committed with its throughput
    Given process path "pick-zone-a" has 8 installed stations
    When a ShiftPlan is committed for process path "pick-zone-a" with 6 planned heads at a rate of 95.5 units per hour for 8 hours
    Then the request is accepted with status 201
    And the response Location is "/paths/pick-zone-a/plan"
    And the committed PathPlan reports 6 planned heads against 8 installed stations
    And the committed PathPlan reports a planned throughput of 4584 units
    And the committed PathPlan reports no travel distance hint

  @bdd
  Scenario Outline: A shift plan with an invalid value is rejected
    When a ShiftPlan is committed for process path "pick-zone-a" with <heads> planned heads and <stations> installed stations at a rate of <rate> units per hour for <hours> hours
    Then the request is rejected with status 400

    Examples:
      | heads | stations | rate  | hours |
      | -2    | 8        | 95.5  | 8     |
      | 6     | 0        | 95.5  | 8     |
      | 6     | 8        | 0     | 8     |
      | 6     | 8        | -10   | 8     |
      | 6     | 8        | 95.5  | 0     |
      | 6     | 8        | 95.5  | -1    |
      | 9     | 8        | 95.5  | 8     |

  @bdd
  Scenario: A throughput that overflows is rejected rather than committed as infinity
    When a ShiftPlan is committed for process path "pick-zone-a" with 4 planned heads and 8 installed stations at a rate of 1e308 units per hour for 8 hours
    Then the request is rejected with status 400

  @bdd
  Scenario Outline: A shift plan that omits a required field is rejected
    When a ShiftPlan is committed for process path "pick-zone-a" without the "<field>" field
    Then the request is rejected with status 400

    Examples:
      | field             |
      | plannedHeads      |
      | installedStations |
      | rateUnitsPerHour  |
      | hours             |

  @bdd
  Scenario: Heads exceeding the installed stations fail the whole commit
    When a ShiftPlan is committed for process path "pick-zone-a" with 9 planned heads and 8 installed stations at a rate of 95.5 units per hour for 8 hours
    Then the request is rejected with status 400
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/heads-exceed-installed-stations"
    And 0 "ShiftPlanCommitted" events were published

  @bdd
  Scenario: A committed shift plan is announced once
    When a ShiftPlan is committed for process path "pick-zone-a" with 6 planned heads and 8 installed stations at a rate of 95.5 units per hour for 8 hours
    Then 1 "ShiftPlanCommitted" event was published

  @bdd
  Scenario: A charge forecast is recorded with its location
    When a charge forecast with a single bucket of 500 units is received for process path "pick-zone-a"
    Then the request is accepted with status 201
    And the response Location is "/paths/pick-zone-a/charge"
    And the charge forecast reports 1 CPT buckets
    And the charge forecast reports a total quantity of 500 units
    And 1 "ChargeForecastReceived" event was published

  @bdd
  Scenario Outline: A charge forecast that is not valid is rejected
    When the charge forecast body <body> is received for process path "pick-zone-a"
    Then the request is rejected with status 400
    And 0 "ChargeForecastReceived" events were published

    Examples:
      | body                                                                     |
      | {"buckets": [{"cpt": "2026-08-21T12:00:00Z", "quantity": -5}]}           |
      | {"buckets": []}                                                          |
      | {"buckets": [{"cpt": "soon", "quantity": 5}]}                            |
      | {"buckets": [{"quantity": 5}]}                                           |
      | {"buckets": [{"cpt": "2026-08-21T12:00:00Z", "quantity": |
      | {}                                                                       |

  @bdd
  Scenario: A new charge forecast replaces the previous one
    Given a charge forecast with buckets of 400 and 600 units is received for process path "pick-zone-a"
    When a charge forecast with a single bucket of 250 units is received for process path "pick-zone-a"
    Then the charge forecast reports 1 CPT buckets
    And the charge forecast reports a total quantity of 250 units

  @bdd
  Scenario: A rebalance read does not invent a labor plan
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    When the rebalance decision is requested for process path "pick-zone-a"
    Then the request is accepted with status 200
    And the rebalance recommendation carries no labor plan

  @bdd
  Scenario: A rebalance read includes the labor plan Workforce Management reported
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
