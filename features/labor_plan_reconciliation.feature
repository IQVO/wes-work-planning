# Scenarios in this file derive from:
#   - docs/docs/adr/0019-labor-plan-committed-shift-plan-reconciliation.md —
#     "2. Trigger: evaluated at write-time on either side" (whichever side
#     commits second computes the comparison; the side that commits first
#     finds nothing to compare and raises nothing) and "3. The signal:
#     PathPlanDriftDetected, surfacing only, never a verdict" (raised only
#     when DriftHeads != 0; DriftHeads = observed - ours, signed).
#   - apis/openapi.yaml — "getLaborPlanView": OPTIONAL driftHeads /
#     driftDetectedAt, "present only once a comparison has been computed;
#     omitted, not zeroed, otherwise"; driftDetectedAt only when driftHeads
#     is non-zero.
Feature: Reconciling our committed PathPlan against Workforce's observed labor plan
  As an operator reading the labor plan view
  I want to see when our committed plan and Workforce's committed plan disagree on planned heads
  So that a human can notice drift between the two plans without either side being overridden

  Background:
    Given the WES Work Planning service is running
    And process path "pick-zone-a" has 20 installed stations

  @bdd
  Scenario: Our plan committing second against a different observed plan surfaces the drift
    Given Workforce Management committed a labor plan of 7 heads at 95.5 units per hour for 8 hours for process path "pick-zone-a"
    When a ShiftPlan is committed for process path "pick-zone-a" with 6 planned heads at a rate of 95.5 units per hour for 8 hours
    Then the request is accepted with status 201
    And 1 PathPlanDriftDetected event was raised
    When the labor plan view is requested for process path "pick-zone-a"
    Then the request is accepted with status 200
    And the labor plan view reports 7 planned heads at a rate of 95.5 units per hour for 8 hours
    And the labor plan view reports a drift of 1 heads
    And the labor plan view reports the drift was detected at "2026-08-21T08:00:00Z"

  @bdd
  Scenario: Workforce's plan committing second against a different committed plan surfaces a negative drift
    Given a ShiftPlan is committed for process path "pick-zone-a" with 6 planned heads at a rate of 95.5 units per hour for 8 hours
    And Workforce Management committed a labor plan of 4 heads at 95.5 units per hour for 8 hours for process path "pick-zone-a"
    Then 1 PathPlanDriftDetected event was raised
    When the labor plan view is requested for process path "pick-zone-a"
    Then the request is accepted with status 200
    And the labor plan view reports a drift of -2 heads
    And the labor plan view reports the drift was detected at "2026-08-21T08:00:00Z"

  @bdd
  Scenario: Agreeing plans raise no event and report a drift of zero
    Given Workforce Management committed a labor plan of 6 heads at 95.5 units per hour for 8 hours for process path "pick-zone-a"
    When a ShiftPlan is committed for process path "pick-zone-a" with 6 planned heads at a rate of 95.5 units per hour for 8 hours
    Then 0 PathPlanDriftDetected events were raised
    When the labor plan view is requested for process path "pick-zone-a"
    Then the request is accepted with status 200
    And the labor plan view reports a drift of 0 heads
    And the labor plan view reports no drift detection time

  @bdd
  Scenario: Nothing is compared until both plans have been committed
    Given Workforce Management committed a labor plan of 7 heads at 95.5 units per hour for 8 hours for process path "pick-zone-a"
    Then 0 PathPlanDriftDetected events were raised
    When the labor plan view is requested for process path "pick-zone-a"
    Then the request is accepted with status 200
    And the labor plan view reports no drift
