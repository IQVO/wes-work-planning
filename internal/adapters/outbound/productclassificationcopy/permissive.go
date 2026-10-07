// Package productclassificationcopy holds the outbound adapters behind
// ports.ProductClassificationLookup and ports.ProductClassificationCopyRepo
// (ADR-0035): this context's local copy of product-master's
// ProductClassified events, in Postgres (Store) or in memory (MemoryStore),
// plus the PermissiveLookup no-op selected by PRODUCT_CLASSIFICATION_MODE=
// permissive.
//
// Every lookup maps the copy to exactly what the retired inventory-storage
// HTTP client returned (ADR-0009): Known=true with HandlingTags and
// TemperatureClass for a stored SKU, Known=false for an unknown one, and
// Known=false with a nil error when the copy cannot be read (fail-open — a
// classification problem never blocks a release).
package productclassificationcopy

import (
	"context"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// PermissiveLookup is the default ports.ProductClassificationLookup: it
// reads nothing and always reports Known=false, which the outbound
// WorkReleased encoder treats as "no derived hazmat/fragile hint available,
// publish the event with those fields omitted" (fail-open). Selected via
// PRODUCT_CLASSIFICATION_MODE=permissive (the default).
type PermissiveLookup struct{}

var _ ports.ProductClassificationLookup = PermissiveLookup{}

// NewPermissiveLookup constructs a PermissiveLookup.
func NewPermissiveLookup() *PermissiveLookup {
	return &PermissiveLookup{}
}

// GetClassification always answers "not classified".
func (PermissiveLookup) GetClassification(_ context.Context, sku string) (productclassificationview.ProductClassificationView, error) {
	return unknown(sku), nil
}

// unknown is the Known=false answer for sku, shared by every adapter here.
func unknown(sku string) productclassificationview.ProductClassificationView {
	return productclassificationview.ProductClassificationView{SKU: sku, Known: false}
}
