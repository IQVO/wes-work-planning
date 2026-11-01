package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// workReleasedData builds the WorkReleased payload: the path/work-unit
// identity plus the CPT, reference and gift-wrap characteristics READ off
// the WorkUnit repo at encode time, and the strictly-additive
// classification hints (ADR-0009/0010). A transfer-referenced unit also
// carries its transfer context (ADR-0033), read off the WorkUnit exactly
// like cpt/ref — never looked up from another service.
func (p *Publisher) workReleasedData(ctx context.Context, ev shared.WorkReleased) (json.RawMessage, error) {
	cpt, ref, sku, giftWrap := "", "", "", false
	transferRef, siteId, quantity, lineNo := "", "", 0, 0
	var workKind workunit.WorkKind
	var relocation, replenishment, pick, caseBreak map[string]any
	if unit, err := p.workUnits.FindById(ctx, ev.WorkUnitId); err == nil {
		cpt = unit.CPT().Time().Format(time.RFC3339)
		ref = unit.Reference()
		sku = unit.SKU()
		giftWrap = unit.GiftWrap()
		transferRef = unit.TransferRef()
		workKind = unit.WorkKind()
		siteId = unit.SiteId()
		quantity = unit.Quantity()
		lineNo = unit.LineNo()
		relocation = relocationData(unit)
		replenishment = replenishmentData(unit)
		pick = pickData(unit)
		caseBreak = caseBreakData(unit)
	}

	requiredCapabilities, fragile := p.classificationHints(ctx, sku)

	data := map[string]any{
		"path_id":      ev.PathId.String(),
		"work_unit_id": ev.WorkUnitId,
		"cpt":          cpt,
		"ref":          ref,
	}
	// Strictly additive and backward compatible: only set these two
	// optional fields when there is something to say. An unclassified
	// SKU or an unavailable lookup omits them entirely rather than
	// publishing an empty array / explicit false, so a consumer that
	// already treats "absent" as "no hint" (fulfillment-execution's
	// documented default) sees no difference from before this feature
	// existed.
	if len(requiredCapabilities) > 0 {
		data["required_capabilities"] = requiredCapabilities
	}
	if fragile {
		data["fragile"] = fragile
	}
	// gift_wrap is a caller-stated WorkReleased characteristic, not a
	// classification hint — read straight off the WorkUnit (like
	// cpt/ref), never derived from classificationHints (see ADR-0010).
	// Same omit-when-false discipline as fragile.
	if giftWrap {
		data["gift_wrap"] = giftWrap
	}
	// line_no (ADR-0036): the order line this unit was made for, read off
	// the WorkUnit like cpt/ref and omitted when unknown (a unit created
	// before the column existed, a REST unit that gave none). Transfer
	// units never carry one. Consumers must treat absent as "unknown".
	if lineNo > 0 {
		data["line_no"] = lineNo
	}
	// Transfer context (ADR-0033): same strictly-additive discipline —
	// the four OPTIONAL fields appear only on a transfer-referenced unit
	// (a transfer work_kind is the discriminator: order work has none, and
	// RELOCATE work carries its relocation object instead), so the payload
	// of every order-driven unit stays byte-identical to before this
	// feature existed. Consumers must treat absent as "not transfer work".
	if workKind.IsTransfer() {
		data["work_kind"] = workKind.String()
		if transferRef != "" {
			data["transfer_ref"] = transferRef
		}
		if siteId != "" {
			data["site_id"] = siteId
		}
		if quantity > 0 {
			data["quantity"] = quantity
		}
	}
	// Slot-plan move (ADR-0040): ONE optional nested object, present only
	// on a RELOCATE unit — exactly what fulfillment-execution's RELOCATE
	// task decodes (its ADR 0047).
	if relocation != nil {
		data["relocation"] = relocation
	}
	// Pick-face replenishment (ADR-0042): ONE optional nested object next to
	// `relocation`, present only on a REPLENISH unit — exactly the six
	// members fulfillment-execution's REPLENISH task decodes (its ADR 0050).
	if replenishment != nil {
		data["replenishment"] = replenishment
	}
	// Pack-aware work (ADR-0045): ONE optional nested object each, exactly the
	// shapes fulfillment-execution already decodes (its ADR 0054): `pick` on a
	// CASE_PICK / EACH_PICK unit, `case_break` on a BREAK_CASE unit. The kind
	// is carried by pick.unit and by the object itself; the top-level
	// work_kind stays transfer-only (ADR-0033).
	if pick != nil {
		data["pick"] = pick
	}
	if caseBreak != nil {
		data["case_break"] = caseBreak
	}
	return json.Marshal(data)
}

// pickData is WorkReleased.data.pick for a CASE_PICK / EACH_PICK unit, nil for
// every other unit: sku, quantity (EACHES whatever the unit), unit (EACH |
// CASE) and from_bin, omitted when no source bin was planned.
func pickData(unit *workunit.WorkUnit) map[string]any {
	plan, ok := unit.Pick()
	if !ok {
		return nil
	}
	out := map[string]any{
		"sku":      plan.SKU,
		"quantity": plan.Quantity,
		"unit":     plan.Unit.String(),
	}
	if plan.FromBin != "" {
		out["from_bin"] = plan.FromBin
	}
	return out
}

// caseBreakData is WorkReleased.data.case_break for a BREAK_CASE unit, nil for
// every other unit: lpn, sku, loose_bin and pick_task_id, all required.
func caseBreakData(unit *workunit.WorkUnit) map[string]any {
	brk, ok := unit.CaseBreak()
	if !ok {
		return nil
	}
	return map[string]any{
		"lpn":          brk.LPN,
		"sku":          brk.SKU,
		"loose_bin":    brk.LooseBin,
		"pick_task_id": brk.PickTaskId,
	}
}

// replenishmentData is WorkReleased.data.replenishment for a REPLENISH unit,
// nil for every other unit: request_id, sku, from_bin, to_bin, quantity
// (always present) and priority (NORMAL | URGENT).
func replenishmentData(unit *workunit.WorkUnit) map[string]any {
	move, ok := unit.Replenishment()
	if !ok {
		return nil
	}
	return map[string]any{
		"request_id": move.RequestId,
		"sku":        move.SKU,
		"from_bin":   move.FromBin,
		"to_bin":     move.ToBin,
		"quantity":   move.Quantity,
		"priority":   move.Priority.String(),
	}
}

// relocationData is WorkReleased.data.relocation for a RELOCATE unit, nil
// for every other unit: plan_id, move_seq (always present, 0 included),
// kind, sku, and from_bin / to_bin omitted when the move leaves that side
// to be scanned at execution (Assign / Vacate).
func relocationData(unit *workunit.WorkUnit) map[string]any {
	move, ok := unit.Relocation()
	if !ok {
		return nil
	}
	out := map[string]any{
		"plan_id":  move.PlanId,
		"move_seq": move.MoveSeq,
		"kind":     move.Kind.String(),
		"sku":      unit.SKU(),
	}
	if move.FromSlot != "" {
		out["from_bin"] = move.FromSlot
	}
	if move.ToSlot != "" {
		out["to_bin"] = move.ToSlot
	}
	return out
}

// classificationHints looks up sku's ProductClassification once, at
// publish time, and derives the two optional WorkReleased hints from it:
// "hazmat" appended to requiredCapabilities when the SKU is classified
// Hazmat, and fragile=true when it is classified Fragile. This is the
// concrete implementation of ADR-0009's "read-once-at-release, stamp onto
// WorkReleased" decision — fulfillment-execution's Task then carries these
// hints without ever calling back to a classification owner. Since ADR-0035
// the read is against this context's own local copy of product-master's
// ProductClassified events.
//
// A missing sku, a nil classifications port, an unclassified SKU (Known
// but no relevant tag, or altogether unknown), or a lookup error are all
// treated identically: no hints. This is deliberately permissive/fail-open
// — unlike inventory-storage's own StowStock placement check, a
// classification-lookup problem here must never block or delay releasing
// work; it can only omit an optional enrichment.
func (p *Publisher) classificationHints(ctx context.Context, sku string) (requiredCapabilities []string, fragile bool) {
	if sku == "" || p.classifications == nil {
		return nil, false
	}

	view, err := p.classifications.GetClassification(ctx, sku)
	if err != nil || !view.Known {
		return nil, false
	}

	if view.HasTag(hazmatTag) {
		requiredCapabilities = append(requiredCapabilities, "hazmat")
	}
	fragile = view.HasTag(fragileTag)
	return requiredCapabilities, fragile
}
