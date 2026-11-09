package usecases

import (
	"context"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// productClassificationFor reads sku's view (with its pack facts) through
// lookup; a nil lookup or a failed read is an empty view for the SKU, so the
// estimate that follows fails or degrades on "no pack data" rather than
// crashing (fail-open, ADR-0009 section 5).
func productClassificationFor(ctx context.Context, lookup ports.ProductClassificationLookup, sku string) productclassificationview.ProductClassificationView {
	if lookup == nil {
		return productclassificationview.ProductClassificationView{SKU: sku}
	}
	view, err := lookup.GetClassification(ctx, sku)
	if err != nil {
		return productclassificationview.ProductClassificationView{SKU: sku}
	}
	return view
}
