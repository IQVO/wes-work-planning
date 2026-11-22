# Documentation-derived rules of the work-unit REST surface.
#
# Sources:
#   - warehouse-docs docs/contexts/wes-work-planning/aggregate-design-canvas.md
#     WorkUnit U4-U6 (and the shared value objects table);
#   - use-cases.md EnqueueWorkUnit invariants and the Queries table (keyset
#     paging, 400 reference-required);
#   - troubleshooting.md "HTTP problem types": unknown-path-id on the routes
#     that can create an aggregate, malformed-request-body for a NUL, the
#     work-unit lifecycle slugs;
#   - docs/docs/adr/0012 (catalogue prefix-family match) and the fleet REST
#     guideline (page.go: limit 1..500, opaque cursor, unknown parameters).
# The charge forecast and shift plan rules moved to warehouse-planning with
# their aggregates (ADR-0049); features/planning_moved.feature pins the 404s
# of the removed routes (stage E).
Feature: Work-unit rules from the documentation
  As the WES conductor
  I want the documented invariants, limits and error rows of the commands and queries pinned at their boundaries
  So that a bad commitment is refused with the documented slug and a read is paged exactly as the contract says

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario Outline: Commands that can create an aggregate refuse a path outside the catalogue family
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
    When the pool for process path "<path>" is configured as "ReleaseFed" with a WIP limit of 5
    Then the request is accepted with status 200

    Examples:
      | path        |
      | pick        |
      | PICK-Zone-B |
      | pack-3-b    |

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
