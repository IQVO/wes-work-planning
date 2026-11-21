# Documentation-derived rules of pack-aware work units, their pack-level
# copy and their forward pick faces.
#
# Sources:
#   - docs/docs/adr/0045-pack-aware-work-units.md decisions 1 (CASE_PICK /
#     EACH_PICK / BREAK_CASE and the pick / case_break objects on released
#     work) and 3 (poison is dead-lettered, never coerced);
#   - docs/docs/adr/0048-site-and-forward-pick-face-on-released-work.md
#     decisions 2 (site_id for every work kind), 3 (from_bin derived at
#     enqueue from the local slot-plan copy, omitted when unknown) and
#     "a caller-given fromBin still wins";
#   - warehouse-docs use-cases.md EnqueueWorkUnit invariants and the
#     eventstorming "Pack-aware work" paragraph (a CASE pick is checked at
#     enqueue against the local copy: an active CASE level and a whole
#     number of cases);
#   - troubleshooting.md rows no-active-case-level / quantity-not-case-multiple.
Feature: Pack-aware work unit rules from the documentation
  As the WES planner
  I want case and each picks and case breaks planned only against the pack facts this context holds
  So that an operator is never handed a case that cannot exist and every pick knows where it happens

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: An EACH pick is enqueued as EACH_PICK and its released work is a CloudEvents 1.0 event carrying the pick
    When a WorkUnit "wu-each" is submitted to process path "pick-zone-a" for 5 eaches of SKU "SKU-9" picked as "EACH"
    Then the request is accepted with status 201
    And the WorkUnit in the response has workKind "EACH_PICK"
    And the WorkUnit in the response picks 5 "EACH"
    And the WorkUnit in the response picks from no bin
    When work is released from process path "pick-zone-a"
    Then the released WorkUnit is "wu-each"
    And the "WorkReleased" wire message is CloudEvents 1.0 typed "com.warehouse.wes.work-planning.workunit.WorkReleased" with dataschema "urn:warehouse:wes-work-planning:events:WorkReleased:v1" and subject "wu-each"
    And the "WorkReleased" wire data picks 5 "EACH" of SKU "SKU-9"
    And the "WorkReleased" wire data picks from no bin
    And the "WorkReleased" wire data omits "case_break"
    And the "WorkReleased" wire data omits "replenishment"
    And the "WorkReleased" wire data omits "work_kind"

  @bdd
  Scenario: A CASE pick the local copy backs is enqueued as CASE_PICK
    Given product-master declares pack levels:
      | sku    | level | gtin          | units_per_pack | version |
      | SKU-24 | CASE  | 24006381333935 | 12             | 3       |
    When a WorkUnit "wu-case" is submitted to process path "pick-zone-a" for 24 eaches of SKU "SKU-24" picked as "CASE"
    Then the request is accepted with status 201
    And the WorkUnit in the response has workKind "CASE_PICK"
    And the WorkUnit in the response picks 24 "CASE"
    When work is released from process path "pick-zone-a"
    Then the "WorkReleased" wire data picks 24 "CASE" of SKU "SKU-24"

  @bdd
  Scenario: A CASE pick for a SKU with no active CASE level is refused and enqueues nothing
    When a WorkUnit "wu-none" is submitted to process path "pick-zone-a" for 12 eaches of SKU "SKU-NEVER-DECLARED" picked as "CASE"
    Then the request is rejected with status 422
    And the problem type slug is "no-active-case-level"
    And 0 "WorkUnitCreated" events were published
    When the Work Pool telemetry for process path "pick-zone-a" is sampled
    Then the request is rejected with status 404

  @bdd
  Scenario: A CASE pick that is not a whole number of cases is refused
    Given product-master declares pack levels:
      | sku    | level | gtin          | units_per_pack | version |
      | SKU-24 | CASE  | 24006381333935 | 12             | 3       |
    When a WorkUnit "wu-odd" is submitted to process path "pick-zone-a" for 13 eaches of SKU "SKU-24" picked as "CASE"
    Then the request is rejected with status 422
    And the problem type slug is "quantity-not-case-multiple"
    And 0 "WorkUnitCreated" events were published

  @bdd
  Scenario: A retired CASE level no longer backs a CASE pick
    Given product-master declares pack levels:
      | sku    | level | gtin          | units_per_pack | version |
      | SKU-24 | CASE  | 24006381333935 | 12             | 3       |
    And product-master retires the pack level of GTIN "24006381333935" at version 4
    Then the pack level event is applied
    When a WorkUnit "wu-retired" is submitted to process path "pick-zone-a" for 24 eaches of SKU "SKU-24" picked as "CASE"
    Then the request is rejected with status 422
    And the problem type slug is "no-active-case-level"

  @bdd
  Scenario: An older Retired version is a stale no-op and the level stays active
    Given product-master declares pack levels:
      | sku    | level | gtin          | units_per_pack | version |
      | SKU-24 | CASE  | 24006381333935 | 12             | 3       |
    When product-master retires the pack level of GTIN "24006381333935" at version 2
    Then the pack level event is a stale no-op
    And a WorkUnit "wu-still" is submitted to process path "pick-zone-a" for 24 eaches of SKU "SKU-24" picked as "CASE"
    And the request is accepted with status 201

  @bdd
  Scenario: A Measured event for a GTIN the copy does not hold is a stale no-op
    When product-master measures the pack level of GTIN "0000000000000" at 100x80x60 mm and 450 g at version 9
    Then the pack level event is a stale no-op

  @bdd
  Scenario: A redelivered pack level event changes nothing
    Given product-master declares pack levels:
      | sku    | level | gtin          | units_per_pack | version |
      | SKU-24 | CASE  | 24006381333935 | 12             | 5       |
    And product-master retires the pack level of GTIN "24006381333935" at version 9
    When product-master retires the pack level of GTIN "24006381333935" at version 9
    Then the pack level event is ignored as a redelivery

  @bdd
  Scenario Outline: A pick or case break on the wrong path, or a broken pack-aware body, is refused with its slug
    When an enqueue request with the body <body> is submitted to process path "<path>"
    Then the request is rejected with status 400
    And the problem type slug is "<slug>"
    And 0 "WorkUnitCreated" events were published

    Examples:
      | path        | body                                                                                                                                                                   | slug                |
      | pick-zone-a | {"workUnitId":"wu-1","cpt":"2026-08-21T12:00:00Z","reference":"order-9001","sku":"SKU-9","quantity":5,"pickUnit":"PALLET"}                                              | unknown-pick-unit   |
      | pack-zone-a | {"workUnitId":"wu-1","cpt":"2026-08-21T12:00:00Z","reference":"order-9001","sku":"SKU-9","quantity":5,"pickUnit":"EACH"}                                                 | invalid-pick-plan   |
      | pick-zone-a | {"workUnitId":"wu-1","cpt":"2026-08-21T12:00:00Z","reference":"order-9001","quantity":5,"pickUnit":"EACH"}                                                              | invalid-pick-plan   |
      | pick-zone-a | {"workUnitId":"wu-1","cpt":"2026-08-21T12:00:00Z","reference":"order-9001","sku":"SKU-9","pickUnit":"EACH"}                                                             | invalid-pick-plan   |
      | pick-zone-a | {"workUnitId":"wu-1","cpt":"2026-08-21T12:00:00Z","reference":"order-9001","sku":"SKU-9","fromBin":"A-1"}                                                               | invalid-pick-plan   |
      | pick-zone-a | {"workUnitId":"wu-1","cpt":"2026-08-21T12:00:00Z","reference":"order-9001","sku":"SKU-9","caseBreak":{"lpn":"LPN-1","pickTaskId":"pick-7"}}                             | invalid-case-break  |
      | pick-zone-a | {"workUnitId":"wu-1","cpt":"2026-08-21T12:00:00Z","reference":"order-9001","sku":"SKU-9","quantity":5,"caseBreak":{"lpn":"LPN-1","looseBin":"LOOSE-1","pickTaskId":"pick-7"}} | invalid-case-break |

  @bdd
  Scenario: A case break is enqueued as BREAK_CASE on the BREAK_CASE path only
    Given the process-path catalogue declares BREAK_CASE
    When a WorkUnit "wu-brk" is submitted to process path "BREAK_CASE" to break LPN "LPN-000123" of SKU "SKU-8" into bin "LOOSE-01" for pick task "pick-77"
    Then the request is accepted with status 201
    And the WorkUnit in the response has workKind "BREAK_CASE"
    And the WorkUnit in the response breaks LPN "LPN-000123" into bin "LOOSE-01" for pick task "pick-77"
    When work is released from process path "BREAK_CASE"
    Then the released WorkUnit is "wu-brk"
    And the "WorkReleased" wire data breaks LPN "LPN-000123" of SKU "SKU-8" into bin "LOOSE-01" for pick task "pick-77"
    And the "WorkReleased" wire data omits "pick"
    And the "WorkReleased" wire data omits "relocation"

  @bdd
  Scenario: Work on the BREAK_CASE path without a case break is refused
    Given the process-path catalogue declares BREAK_CASE
    When a WorkUnit "wu-plain" with CPT "2026-08-21T12:00:00Z" and reference "order-9001" is submitted to process path "BREAK_CASE"
    Then the request is rejected with status 400
    And the problem type slug is "invalid-case-break"

  @bdd
  Scenario: A pick plan without a from-bin inherits the SKU's forward pick face from the newest slot plan
    Given slotting-optimization approves slot plan "plan-F" for site "SITE-9" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-F | Assign |           | S-FWD   |
    When a WorkUnit "wu-face" is submitted to process path "pick-zone-a" for 5 eaches of SKU "SKU-F" picked as "EACH"
    Then the request is accepted with status 201
    And the WorkUnit in the response picks from bin "S-FWD"
    When work is released from process path "pick-zone-a"
    Then the "WorkReleased" wire data picks from bin "S-FWD"

  @bdd
  Scenario: A caller-stated from-bin wins over the derived forward pick face
    Given slotting-optimization approves slot plan "plan-F" for site "SITE-9" at "2026-08-21T07:00:00Z" with moves:
      | sku   | kind   | from_slot | to_slot |
      | SKU-F | Assign |           | S-FWD   |
    When a WorkUnit "wu-keep" is submitted to process path "pick-zone-a" for 5 eaches of SKU "SKU-F" picked as "EACH" from bin "BIN-MANUAL"
    Then the request is accepted with status 201
    And the WorkUnit in the response picks from bin "BIN-MANUAL"
