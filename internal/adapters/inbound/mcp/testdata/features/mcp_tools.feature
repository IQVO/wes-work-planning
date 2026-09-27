# MCP behavioral evals for the wes-work-planning tool surface.
#
# Each scenario drives tools/call over the REAL Streamable HTTP handler
# with a connected SDK client — exactly the call a model host makes — and
# pins the structured result and the state side effects. Arguments are
# deliberately model-realistic: extra keys, wrong types, unknown ids.
#
# Derived from the tool contracts documented in:
#   - internal/adapters/inbound/mcp/tools.go and report_tool.go (tool
#     descriptions and semantics: telemetry = the SampleBacklog read
#     model, rebalance = the Drum-Buffer-Rope decision, release = the
#     WIP-invariant-bounded write)
#   - docs/docs/mcp/governance-charter.md (§2 tool curation: intent-level
#     tools; §4 write tools must be safe-by-construction)
#   - apis/openapi.yaml GET /paths/{pathId}/telemetry,
#     GET /paths/{pathId}/rebalance and POST /paths/{pathId}/release —
#     the same read models and write the tools serve.

Feature: MCP tool behavioral evals
  The wes-work-planning MCP tools expose the WES core to AI agents:
  live backlog telemetry per process path, a Drum-Buffer-Rope rebalance
  recommendation, a WIP-invariant-bounded release write, and (when the
  reports service is configured) the release-throughput report. An agent
  relying on them must get the same semantics the REST API guarantees,
  through the schema-decoded argument path a model host actually uses.

  Background:
    Given the MCP server is running with the canonical eval state (two pending units on pick-a, a flow-fed pack-a pool over its alarm threshold, and a saturated release-fed sat-a pool)

  Scenario: A healthy release-fed path reports its backlog and WIP
    When I call the tool "get_backlog_telemetry" with argument "pathId" = "pick-a"
    Then the tool call succeeds
    And the structured result field "pathId" is "pick-a"
    And the structured result field "backlogDepth" is 2
    And the structured result field "wip" is 0
    And the structured result field "mode" is "ReleaseFed"
    And the structured result field "overAlarmThreshold" is false

  Scenario: A flow-fed path over its alarm threshold is flagged and breaches
    The telemetry read is not pure: sampling a flow-fed pool that is over
    its alarm threshold publishes BacklogThresholdBreached — the same
    side effect the HTTP telemetry endpoint has (charter §4 documents
    read tools by outcome, not by side-effect freedom).
    When I call the tool "get_backlog_telemetry" with argument "pathId" = "pack-a"
    Then the tool call succeeds
    And the structured result field "mode" is "FlowFed"
    And the structured result field "backlogDepth" is 5
    And the structured result field "overAlarmThreshold" is true
    And the domain event "BacklogThresholdBreached" was published

  Scenario: An unknown path is a clean tool error, not empty telemetry
    Unlike inventory-storage's check_availability (zero for an unknown
    SKU), this read model has no pool to project: an unknown pathId
    surfaces the repository's not-found error.
    When I call the tool "get_backlog_telemetry" with argument "pathId" = "never-heard-of"
    Then the tool call reports a problem mentioning "not found"

  Scenario: An empty pathId is a clean tool error
    When I call the tool "get_backlog_telemetry" with argument "pathId" = ""
    Then the tool call reports a problem mentioning "pathId"

  Scenario: Model chatter in the arguments is rejected, not ignored
    The typed tool schemas are strict (additionalProperties: false, the
    SDK default): a host forwarding stray model-generated keys gets a
    clean schema validation error rather than a silent ignore.
    When I call the tool "get_backlog_telemetry" with arguments
      | pathId        | pick-a          |
      | model_chatter | maybe congested |
      | step          | 2               |
    Then the tool call reports a problem mentioning "model_chatter"

  Scenario: A wrong-typed argument is rejected without coercion
    When I call the tool "get_backlog_telemetry" with argument "pathId" = 42
    Then the tool call does not succeed silently

  Scenario: A healthy release-fed path needs no rebalance action
    Backlog is waiting but WIP is far under the limit: the release policy,
    not labor, is the lever — NoActionNeeded, with no event published.
    When I call the tool "get_rebalance_recommendation" with argument "pathId" = "pick-a"
    Then the tool call succeeds
    And the structured result field "action" is "NoActionNeeded"
    And the domain event "LaborReassignmentFlagged" was not published

  Scenario: A flow-fed path over its alarm threshold throttles upstream
    When I call the tool "get_rebalance_recommendation" with argument "pathId" = "pack-a"
    Then the tool call succeeds
    And the structured result field "action" is "ThrottleUpstream"
    And the domain event "PathThrottled" was published

  Scenario: A saturated release-fed path flags labor reassignment
    WIP is at its limit with backlog still waiting: releasing more is
    pointless, so the recommendation moves people, not work.
    When I call the tool "get_rebalance_recommendation" with argument "pathId" = "sat-a"
    Then the tool call succeeds
    And the structured result field "action" is "ReassignLabor"
    And the structured result field "wip" is 1
    And the domain event "LaborReassignmentFlagged" was published

  Scenario: An unknown path is a clean rebalance error
    When I call the tool "get_rebalance_recommendation" with argument "pathId" = "no-such-path"
    Then the tool call reports a problem mentioning "not found"

  Scenario: Releasing admits the earliest-CPT unit and shifts the read model
    When I call the tool "release_next_work" with argument "pathId" = "pick-a"
    Then the tool call succeeds
    And the structured result field "workUnitId" is "wu-o1"
    And the structured result field "ref" is "o1"
    And the structured result field "cpt" is "2026-01-01T13:00:00Z"
    And the domain event "WorkReleased" was published
    When I call the tool "get_backlog_telemetry" with argument "pathId" = "pick-a"
    Then the tool call succeeds
    And the structured result field "backlogDepth" is 1
    And the structured result field "wip" is 1

  Scenario: Releasing from a drained pool is a clean tool error
    When I call the tool "release_next_work" with argument "pathId" = "pick-a"
    And I call the tool "release_next_work" with argument "pathId" = "pick-a"
    And I call the tool "release_next_work" with argument "pathId" = "pick-a"
    Then the tool call reports a problem mentioning "no pending work"

  Scenario: Releasing into a saturated pool is rejected by the WIP invariant
    The pool's enforced WIP limit bounds the risk of a mistaken model
    call: at the limit, release is rejected rather than queued.
    When I call the tool "release_next_work" with argument "pathId" = "sat-a"
    Then the tool call reports a problem mentioning "WIP limit"

  Scenario: Releasing on an unknown path is a clean tool error
    When I call the tool "release_next_work" with argument "pathId" = "ghost-path"
    Then the tool call reports a problem mentioning "not found"

  Scenario: The throughput report serves its rows when reports are configured
    The report tool registers only when a reports client is configured
    (ADR-0011); this scenario drives that wider surface.
    Given the MCP server is running with the reports client returning one throughput row
    When I call the tool "get_release_throughput_report" with arguments
      | from        | 2026-01-01T00:00:00Z |
      | to          | 2026-01-02T00:00:00Z |
      | pathId      | pick-a               |
      | granularity | hour                 |
    Then the tool call succeeds
    And the structured result field "rows" has 1 entries
    And the structured result field "rows" entry 0 has "pathId" = "pick-a"
    And the structured result field "rows" entry 0 has "workReleased" = 4

  Scenario: The throughput report requires its window
    Given the MCP server is running with the reports client returning one throughput row
    When I call the tool "get_release_throughput_report" with arguments
      | from        | |
      | to          | 2026-01-02T00:00:00Z |
    Then the tool call reports a problem mentioning "from and to"

  Scenario: The report tool is absent without a reports client
    On the default deployment (no reports service configured) the
    conditional tool is not registered at all, so a model asking for it
    gets the unknown-tool protocol error.
    When I call the tool "get_release_throughput_report" with arguments
      | from | 2026-01-01T00:00:00Z |
      | to   | 2026-01-02T00:00:00Z |
    Then the tool call reports a problem mentioning "get_release_throughput_report"
