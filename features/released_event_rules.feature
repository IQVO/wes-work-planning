# Documentation-derived rules of the published WorkReleased / WorkUnitCreated
# payloads: classification hints, the site, and the origin fields.
#
# Sources:
#   - docs/docs/adr/0009 (read the classification once at release and stamp
#     it onto WorkReleased), 0035 (the local version-guarded copy; an older
#     or equal version is a stale no-op), and .claude/rules/domain-model.md
#     "Product classification propagation" (required_capabilities/fragile
#     are OMITTED, not false/empty, when the SKU is unclassified);
#   - .claude/rules/domain-model.md trap 5: gift_wrap is caller-stated and
#     never derived from a classification;
#   - docs/docs/adr/0048 decision 1/2 (order work gets its fulfillment site;
#     WorkReleased publishes site_id for every work kind that has one);
#   - docs/docs/adr/0041 (WorkUnitCreated carries the origin reference and
#     line_no; order-management's release-confirmation consumer matches
#     them) and domain-events.md (line_no omitted when unknown, never on
#     transfer units).
Feature: Released-event payload rules from the documentation
  As the fulfillment executor
  I want released work to carry exactly the facts the documentation promises and nothing invented
  So that a Task is built without a callback and a fragile or hazmat unit is never handled blind

  Background:
    Given the WES Work Planning service is running

  @bdd
  Scenario: A hazmat and fragile SKU stamps both hints onto the released work
    Given product-master publishes classification event "cls-hz-2" for SKU "SKU-HZ" with tags "Hazmat, Fragile" at version 2
    And the classification event is applied
    And a WorkUnit "wu-hz" is submitted to process path "pick-zone-a" with SKU "SKU-HZ", gift wrap requested and line number 1
    When work is released from process path "pick-zone-a"
    Then the released WorkUnit is "wu-hz"
    And the "WorkReleased" wire data requires capabilities "hazmat"
    And the "WorkReleased" wire data field "fragile" is "true"

  @bdd
  Scenario: An unclassified SKU omits both hints while a caller-stated gift wrap still travels
    Given a WorkUnit "wu-plain" is submitted to process path "pick-zone-a" with SKU "SKU-UNKNOWN", gift wrap requested and line number 3
    When work is released from process path "pick-zone-a"
    Then the released WorkUnit is "wu-plain"
    And the "WorkReleased" wire data omits "required_capabilities"
    And the "WorkReleased" wire data omits "fragile"
    And the "WorkReleased" wire data field "gift_wrap" is "true"
    And the "WorkReleased" wire data field "line_no" is "3"

  @bdd
  Scenario: A classification event older than the copy is a stale no-op and never regresses the hints
    Given product-master publishes classification event "cls-v-5" for SKU "SKU-V" with tags "Fragile" at version 5
    And the classification event is applied
    When product-master publishes classification event "cls-v-3" for SKU "SKU-V" with tags "Hazmat" at version 3
    Then the classification event is a stale no-op
    And a WorkUnit "wu-v" is submitted to process path "pick-zone-a" with SKU "SKU-V", gift wrap requested and line number 1
    And work is released from process path "pick-zone-a"
    And the released WorkUnit is "wu-v"
    And the "WorkReleased" wire data field "fragile" is "true"
    And the "WorkReleased" wire data omits "required_capabilities"

  @bdd
  Scenario: A redelivered classification event is a no-op
    Given product-master publishes classification event "cls-d-4" for SKU "SKU-D" with tags "Fragile" at version 4
    And the classification event is applied
    When product-master publishes classification event "cls-d-4" for SKU "SKU-D" with tags "Hazmat, Fragile" at version 9
    Then the classification event is ignored as a redelivery

  @bdd
  Scenario: Released order work carries the order's fulfillment site
    When order-management publishes OrderAllocated event "evt-site" for order "order-950" promised for "2026-08-22T12:00:00Z" fulfilled at site "SITE-7" with the single line 1 of SKU "SKU-A" on process path "pick-zone-a"
    Then the OrderAllocated event is applied
    When work is released from process path "pick-zone-a"
    Then the released WorkUnit is "order-950-line-1"
    And the "WorkReleased" wire data field "site_id" is "SITE-7"

  @bdd
  Scenario: A created work unit carries its origin reference and line on the wire
    When order-management publishes OrderAllocated event "evt-oc" for order "order-960" promised for "2026-08-22T12:00:00Z" with the single line 2 of SKU "SKU-B" on process path "pick-zone-a"
    Then the OrderAllocated event is applied
    And the "WorkUnitCreated" wire data field "work_unit_id" is "order-960-line-2"
    And the "WorkUnitCreated" wire data field "reference" is "order-960"
    And the "WorkUnitCreated" wire data field "line_no" is "2"
    And the "WorkUnitCreated" wire data field "path_id" is "pick-zone-a"

  @bdd
  Scenario: A transfer unit's creation carries its reference and omits the line it never had
    When network-inventory-planning publishes WorkDemandReleased event "evt-9" for demand "dem-88" of kind "TRANSFER_DISPATCH" on process path "pack-zone-a" for 40 units of SKU "SKU-T" due at "2026-08-22T09:30:00Z"
    Then the WorkDemandReleased event is applied
    And the "WorkUnitCreated" wire data field "work_unit_id" is "dem-88"
    And the "WorkUnitCreated" wire data field "reference" is "dem-88"
    And the "WorkUnitCreated" wire data omits "line_no"
