# Scenarios in this file derive from:
#   - apis/openapi.yaml — "receiveChargeForecast" operation (POST
#     /paths/{pathId}/charge): 201 response schema ChargeForecastResponse
#     (totalQuantity = "Sum of all bucket quantities", receivedAt = "When
#     this forecast was recorded (server clock)") and the shared 400
#     BadRequest Problem response (invalid-quantity example).
#   - docs/docs/api/errors.md — "The full catalogue", 400 table:
#     invalid-quantity ("negative quantity") and
#     charge-forecast-requires-buckets ("empty buckets").
#   - .claude/rules/domain-model.md — use case 1
#     `ReceiveChargeForecast(path, cptBuckets)` → ChargeForecast.
Feature: Receiving a shift's Charge forecast for a Process Path
  As the WES conductor
  I want the volume due by each CPT recorded as a bucketed Charge forecast
  So that a plan can be committed and work released against real demand
  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: Receiving a charge forecast returns the total quantity due across its CPT buckets
    When a charge forecast with buckets of 4200 and 1800 units is received for process path "pick-zone-a"
    Then the request is accepted with status 201
    And the charge forecast reports 2 CPT buckets
    And the charge forecast reports a total quantity of 6000 units
    And the charge forecast was received at "2026-08-21T08:00:00Z"

  @bdd
  Scenario: Receiving a charge forecast with a negative quantity is rejected
    When a charge forecast with a negative quantity is received for process path "pick-zone-a"
    Then the request is rejected with status 400
    And the problem detail title is "Invalid quantity"
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/invalid-quantity"

  @bdd
  Scenario: Receiving a charge forecast with no CPT buckets is rejected
    When a charge forecast with no CPT buckets is received for process path "pick-zone-a"
    Then the request is rejected with status 400
    And the problem detail title is "Charge forecast requires at least one CPT bucket"
    And the problem detail type is "https://errors.wes-work-planning.warehouse-systems.dev/charge-forecast-requires-buckets"
