# Derived from: docs/docs/adr/0049-charge-forecast-and-shift-plan-move-to-warehouse-planning.md
# (stage E: the two moved routes are gone after one release of 410; the spec
# no longer lists them). ADR-0049's decisions 1-2 live in warehouse-planning
# (its ADR-0023) and are covered by its own suite.
@bdd
Feature: The moved planning routes are gone
  As the WES operator
  I want the retired declaration routes to no longer exist
  So that no client can still send a charge or a shift plan here

  Scenario: POST /paths/{pathId}/charge is no route at all
    When a POST request is sent to "/paths/<path>/charge" with the body {}
    Then the request is rejected with status 404

  Scenario: POST /paths/{pathId}/plan is no route at all
    When a POST request is sent to "/paths/<path>/plan" with the body {}
    Then the request is rejected with status 404
