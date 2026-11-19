# The charge forecast and the shift plan moved to warehouse-planning.
#
# Derived from:
#   - docs/docs/adr/0049-charge-forecast-and-shift-plan-move-to-warehouse-planning.md
#     decision 1 (stage C): POST /paths/{pathId}/charge answers 410
#     charge-forecast-moved naming PUT /workload-forecasts/{site_id}/{path_id},
#     POST /paths/{pathId}/plan answers 410 shift-plan-moved naming
#     PUT /labour-plans/{site_id}/{shift_id}/{path_id}; decision 2 (the drift
#     reconciliation is removed); decision 5 (LaborPlanObserved and its GET
#     are unchanged).
#   - apis/openapi.yaml receiveChargeForecast / commitShiftPlan: deprecated,
#     always 410 for any body; getLaborPlanView: driftHeads no longer computed.
Feature: The charge forecast and the shift plan are warehouse-planning's
  As a planner who used to send the morning's charge and heads here
  I want both retired routes to say where the decision lives now
  So that a stale client learns the new route instead of silently writing dead state

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario Outline: A charge forecast is answered 410 naming warehouse-planning's route, whatever the body
    When a POST request is sent to "/paths/<path>/charge" with the body <body>
    Then the request is rejected with status 410
    And the problem type slug is "charge-forecast-moved"
    And the problem detail title is "Charge forecast moved to warehouse-planning"
    And the problem detail names "PUT /workload-forecasts/{site_id}/{path_id}"
    And no event was published

    Examples:
      | path        | body                                                               |
      | pick-zone-a | {"buckets": [{"cpt": "2026-08-21T12:00:00Z", "quantity": 500}]}    |
      | pick-zone-a | {"buckets": []}                                                    |
      | pick-zone-a | {not json                                                          |
      | sorter-7    | {"buckets": [{"cpt": "2026-08-21T12:00:00Z", "quantity": 500}]}    |

  @bdd
  Scenario Outline: A shift plan is answered 410 naming warehouse-planning's route, whatever the body
    When a POST request is sent to "/paths/<path>/plan" with the body <body>
    Then the request is rejected with status 410
    And the problem type slug is "shift-plan-moved"
    And the problem detail title is "Shift plan moved to warehouse-planning"
    And the problem detail names "PUT /labour-plans/{site_id}/{shift_id}/{path_id}"
    And no event was published

    Examples:
      | path        | body                                                                                       |
      | pick-zone-a | {"plannedHeads": 6, "installedStations": 8, "rateUnitsPerHour": 95.5, "hours": 8}          |
      | pick-zone-a | {"plannedHeads": 9, "installedStations": 8, "rateUnitsPerHour": 95.5, "hours": 8}          |
      | pick-zone-a | {}                                                                                         |
      | sorter-7    | {"plannedHeads": 6, "installedStations": 8, "rateUnitsPerHour": 95.5, "hours": 8}          |

  @bdd
  Scenario: An observed labor plan is projected as reported and compared with nothing
    Given a POST request is sent to "/paths/pick-zone-a/plan" with the body {"plannedHeads": 6, "installedStations": 8, "rateUnitsPerHour": 95.5, "hours": 8}
    And Workforce Management committed a labor plan of 7 heads at 95.5 units per hour for 8 hours for process path "pick-zone-a"
    When the labor plan view is requested for process path "pick-zone-a"
    Then the request is accepted with status 200
    And the labor plan view reports 7 planned heads at a rate of 95.5 units per hour for 8 hours
    And the labor plan view was observed at "2026-08-21T08:00:00Z"
    And the labor plan view reports no drift
    And no event was published
