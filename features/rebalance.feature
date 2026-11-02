Feature: Flow balancing rebalance decisions for a Process Path
  As the WES conductor
  I want a Drum-Buffer-Rope rebalance recommendation from live buffer telemetry
  So that a saturated process path is flagged for labor reassignment instead of silently backing up

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: A path with backlog above plan surfaces a rebalance recommendation
    Given 1001 WorkUnits are enqueued to process path "pick-zone-a"
    And 1000 WorkUnits are released from process path "pick-zone-a"
    And the Work Pool telemetry for process path "pick-zone-a" reports backlog depth 1 and WIP 1000
    When the rebalance decision is requested for process path "pick-zone-a"
    Then the request is accepted with status 201
    And the rebalance recommendation action is "ReassignLabor"
    And the rebalance recommendation reports backlog depth 1 and WIP 1000

  @bdd
  Scenario: A path with backlog within plan needs no rebalancing
    Given a WorkUnit "wu-1" with CPT "2026-08-21T10:00:00Z" and reference "order-line-1" is enqueued to process path "pick-zone-a"
    And work is released from process path "pick-zone-a"
    When the rebalance decision is requested for process path "pick-zone-a"
    Then the request is accepted with status 201
    And the rebalance recommendation action is "NoActionNeeded"
    And the rebalance recommendation reports backlog depth 0 and WIP 1
