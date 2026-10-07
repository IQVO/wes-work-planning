// Package productclassificationview holds the read-only view of a SKU's
// product classification as this context knows it.
//
// The classification is owned by product-master (its ADR 0001). Since
// ADR-0035 this context keeps a LOCAL COPY of it, fed by product-master's
// ProductClassified events on warehouse.product-master.events and stored
// one row per SKU (guarded by the producer's version); the outbound
// WorkReleased encoder reads it through ports.ProductClassificationLookup
// once, at release time. Before ADR-0035 it was read synchronously from
// inventory-storage's GET /products/{sku}/classification (ADR-0009), and the
// view is deliberately the same value either way. The package lives in
// internal/domain because it is a pure value type with no adapter/framework
// dependency, matching this repo's convention that domain holds every plain
// read-model value regardless of how it is populated.
package productclassificationview

// ProductClassificationView is the classification this context holds for a
// SKU at the moment it was looked up. Plain read-model value, not an
// aggregate with invariants — the classification is owned by product-master;
// this service only observes it and republishes derived hints.
//
// Known distinguishes "SKU has no classification in the local copy" or
// "copy unreadable" (Known=false, both treated identically —
// permissive/fail-open, see ADR-0009) from "classification confirmed, tags
// may still be empty in principle" (Known=true).
type ProductClassificationView struct {
	SKU              string
	HandlingTags     []string
	TemperatureClass string
	Known            bool
}

// HasTag reports whether the view carries tag (e.g. "Hazmat", "Fragile") —
// a small convenience so callers do not repeat a linear scan.
func (v ProductClassificationView) HasTag(tag string) bool {
	for _, t := range v.HandlingTags {
		if t == tag {
			return true
		}
	}
	return false
}
