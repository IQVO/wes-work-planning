# Documentation-derived rules of the ChargeForecast and ShiftPlan aggregates
# and of the work-unit REST surface.
#
# Sources:
#   - warehouse-docs docs/contexts/wes-work-planning/aggregate-design-canvas.md
#     ChargeForecast C1-C3, ShiftPlan P1-P4 (and the shared value objects
#     table), WorkUnit U4-U6;
#   - use-cases.md ReceiveChargeForecast / CommitShiftPlan / EnqueueWorkUnit
#     invariants and the Queries table (keyset paging, 400 reference-required);
#   - troubleshooting.md "HTTP problem types": unknown-path-id on the routes
#     that can create an aggregate, malformed-request-body for a NUL, the
#     work-unit lifecycle slugs;
#   - docs/docs/adr/0012 (catalogue prefix-family match) and the fleet REST
#     guideline (page.go: limit 1..500, opaque cursor, unknown parameters).
Feature: Plan, charge and work-unit rules from the documentation
  As the WES conductor
  I want the documented invariants, limits and error rows of the commands and queries pinned at their boundaries
  So that a bad commitment is refused with the documented slug and a read is paged exactly as the contract says

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario Outline: Planned heads may equal the installed stations and a count may reach the 32-bit maximum
    When a ShiftPlan is committed for process path "pick-zone-a" with <heads> planned heads and <stations> installed stations at a rate of 1 units per hour for 1 hours
    Then the request is accepted with status 201
    And the committed PathPlan reports <heads> planned heads against <stations> installed stations

    Examples:
      | heads      | stations   |
      | 8          | 8          |
      | 0          | 0          |
      | 2147483647 | 2147483647 |

  @bdd
  Scenario Outline: A shift plan that breaks an invariant is refused with its documented slug and announces nothing
    When a ShiftPlan is committed for process path "pick-zone-a" with <heads> planned heads and <stations> installed stations at a rate of <rate> units per hour for <hours> hours
    Then the request is rejected with status 400
    And the problem type slug is "<slug>"
    And 0 "ShiftPlanCommitted" events were published

    Examples:
      | heads      | stations   | rate  | hours | slug                           |
      | 9          | 8          | 95.5  | 8     | heads-exceed-installed-stations |
      | 2147483648 | 2147483648 | 95.5  | 8     | invalid-station-count          |
      | -1         | 8          | 95.5  | 8     | invalid-station-count          |
      | 6          | -1         | 95.5  | 8     | invalid-station-count          |
      | 6          | 8          | 0     | 8     | invalid-rate                   |
      | 6          | 8          | -10   | 8     | invalid-rate                   |
      | 6          | 8          | 95.5  | 0     | invalid-hours                  |
      | 6          | 8          | 95.5  | -1    | invalid-hours                  |
      | 4          | 8          | 1e308 | 8     | planned-throughput-not-finite  |

  @bdd
  Scenario Outline: A shift plan without a required count is malformed
    When a ShiftPlan is committed for process path "pick-zone-a" without the "<field>" field
    Then the request is rejected with status 400
    And the problem type slug is "malformed-request-body"

    Examples:
      | field             |
      | plannedHeads      |
      | installedStations |

  @bdd
  Scenario Outline: Commands that can create an aggregate refuse a path outside the catalogue family
    When a charge forecast with a single bucket of 500 units is received for process path "<path>"
    Then the request is rejected with status 400
    And the problem type slug is "unknown-path-id"
    And 0 "ChargeForecastReceived" events were published
    When a ShiftPlan is committed for process path "<path>" with 6 planned heads and 8 installed stations at a rate of 95.5 units per hour for 8 hours
    Then the request is rejected with status 400
    And the problem type slug is "unknown-path-id"
    And 0 "ShiftPlanCommitted" events were published
    When a WorkUnit "wu-1" with CPT "2026-08-21T12:00:00Z" and reference "order-line-1" is submitted to process path "<path>"
    Then the request is rejected with status 400
    And the problem type slug is "unknown-path-id"
    When the pool for process path "<path>" is configured as "ReleaseFed" with a WIP limit of 5
    Then the request is rejected with status 400
    And the problem type slug is "unknown-path-id"

    Examples:
      | path          |
      | sorter-7      |
      | pickup-zone-a |

  @bdd
  Scenario Outline: A path of a declared family is accepted whatever its case or suffix
    When a charge forecast with a single bucket of 500 units is received for process path "<path>"
    Then the request is accepted with status 201

    Examples:
      | path        |
      | pick        |
      | PICK-Zone-B |
      | pack-3-b    |

  @bdd
  Scenario Outline: A charge quantity is non-negative, so zero is accepted and below zero is refused
    When the charge forecast body {"buckets": [{"cpt": "2026-08-21T12:00:00Z", "quantity": <quantity>}]} is received for process path "pick-zone-a"
    Then the request is <outcome> with status <status>

    Examples:
      | quantity | outcome  | status |
      | 0        | accepted | 201    |
      | 1        | accepted | 201    |
      | -1       | rejected | 400    |

  @bdd
  Scenario: A charge forecast totals every bucket and is announced once per revision
    When the charge forecast body {"buckets": [{"cpt": "2026-08-21T12:00:00Z", "quantity": 120}, {"cpt": "2026-08-21T15:00:00Z", "quantity": 80}, {"cpt": "2026-08-21T18:00:00Z", "quantity": 50}]} is received for process path "pick-zone-a"
    Then the request is accepted with status 201
    And the charge forecast reports 3 CPT buckets
    And the charge forecast reports a total quantity of 250 units
    When a charge forecast with a single bucket of 40 units is received for process path "pick-zone-a"
    Then 2 "ChargeForecastReceived" events were published

  @bdd
  Scenario Outline: A charge forecast that breaks the documented shape is refused with its slug
    When the charge forecast body <body> is received for process path "pick-zone-a"
    Then the request is rejected with status 400
    And the problem type slug is "<slug>"
    And 0 "ChargeForecastReceived" events were published

    Examples:
      | body                                                           | slug                             |
      | {}                                                             | charge-forecast-requires-buckets |
      | {"buckets": []}                                                | charge-forecast-requires-buckets |
      | {"buckets": [{"quantity": 5}]}                                 | malformed-request-body           |
      | {"buckets": [{"cpt": "2026-08-21T12:00:00Z"}]}                 | malformed-request-body           |
      | {"buckets": [{"cpt": "2026-08-21T12:00:00Z", "quantity": -3}]} | invalid-quantity                 |

  @bdd
  Scenario Outline: A line number must be 1 to 2147483647 when given
    When a WorkUnit "wu-1" is submitted to process path "pick-zone-a" with line number <line>
    Then the request is <outcome> with status <status>

    Examples:
      | line       | outcome  | status |
      | 1          | accepted | 201    |
      | 2147483647 | accepted | 201    |
      | 0          | rejected | 400    |
      | -1         | rejected | 400    |
      | 2147483648 | rejected | 400    |

  @bdd
  Scenario: A refused line number names its slug and enqueues nothing
    When a WorkUnit "wu-1" is submitted to process path "pick-zone-a" with line number 0
    Then the problem type slug is "invalid-line-no"
    And 0 "WorkUnitCreated" events were published

  @bdd
  Scenario Outline: A work unit needs an id, a reference and a CPT
    When an enqueue request with the body <body> is submitted to process path "pick-zone-a"
    Then the request is rejected with status 400
    And the problem type slug is "<slug>"
    And 0 "WorkUnitCreated" events were published

    Examples:
      | body                                                                       | slug                         |
      | {"workUnitId": "", "cpt": "2026-08-21T12:00:00Z", "reference": "order-1"}  | work-unit-id-required        |
      | {"workUnitId": "wu-1", "cpt": "2026-08-21T12:00:00Z", "reference": ""}     | work-unit-reference-required |
      | {"workUnitId": "wu-1", "reference": "order-1"}                             | malformed-request-body       |

  @bdd
  Scenario: A work unit lookup is paged by CPT with an opaque cursor that fills the limit exactly
    Given a WorkUnit "wu-c" with CPT "2026-08-21T14:00:00Z" and reference "order-9001" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-a" with CPT "2026-08-21T10:00:00Z" and reference "order-9001" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-b" with CPT "2026-08-21T12:00:00Z" and reference "order-9001" is enqueued to process path "pick-zone-a"
    And a WorkUnit "wu-other" with CPT "2026-08-21T09:00:00Z" and reference "order-other" is enqueued to process path "pick-zone-a"
    When a GET request is sent to "/work-units?reference=order-9001&limit=2"
    Then the page lists work units "wu-a", "wu-b"
    And the page has a next page cursor
    When the next page of the last list request is fetched with its cursor given twice
    Then the request is rejected with status 400
    And the problem type slug is "invalid-cursor"
    When a GET request is sent to "/work-units?reference=order-9001&limit=2"
    And the next page of the last list request is fetched
    Then the page lists work units "wu-c"
    And the page has no next page cursor
    When a GET request is sent to "/work-units?reference=order-9001&limit=3"
    Then the page lists work units "wu-a", "wu-b", "wu-c"
    And the page has no next page cursor

  @bdd
  Scenario: A reference nobody used answers an empty page, not a 404, and the largest page size is accepted
    When a GET request is sent to "/work-units?reference=order-never-seen"
    Then the request is accepted with status 200
    And the page is an empty array of items
    When a GET request is sent to "/work-units?reference=order-never-seen&limit=500"
    Then the request is accepted with status 200

  @bdd
  Scenario Outline: A collection read refuses a bad limit, cursor or parameter with its slug
    When a GET request is sent to "/work-units?reference=order-1&<query>"
    Then the request is rejected with status 400
    And the problem type slug is "<slug>"

    Examples:
      | query       | slug                    |
      | limit=0     | invalid-limit           |
      | limit=501   | invalid-limit           |
      | limit=abc   | invalid-limit           |
      | limit=      | invalid-limit           |
      | limit=1&limit=2 | invalid-limit       |
      | cursor=!!!  | invalid-cursor          |
      | after=wu-1  | unknown-query-parameter |

  @bdd
  Scenario: A NUL character in a body, query or path is a client error before anything is recorded
    When a POST request is sent to "/paths/pick-zone-a/work-units" with the body {"workUnitId": "wu\u00001", "cpt": "2026-08-21T12:00:00Z", "reference": "order-1"}
    Then the request is rejected with status 400
    And the problem type slug is "malformed-request-body"
    When a GET request is sent to "/work-units?reference=order%001"
    Then the request is rejected with status 400
    And the problem type slug is "malformed-request-body"
    When a GET request is sent to "/work-units/wu%001"
    Then the request is rejected with status 404
    And 0 "WorkUnitCreated" events were published
