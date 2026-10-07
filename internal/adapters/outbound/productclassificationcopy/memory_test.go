package productclassificationcopy_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

func classified(sku string, tags ...string) productclassificationview.ProductClassificationView {
	return productclassificationview.ProductClassificationView{SKU: sku, HandlingTags: tags, Known: true}
}

func TestMemoryStore_FoundAfterApply(t *testing.T) {
	ctx := context.Background()
	store := productclassificationcopy.NewMemoryStore()
	view := classified("SKU-1", "Hazmat", "TemperatureSensitive")
	view.TemperatureClass = "Frozen"

	applied, err := store.ApplyIfNewer(ctx, view, 3, 1, time.Now())
	if err != nil || !applied {
		t.Fatalf("ApplyIfNewer = %v, %v; want applied", applied, err)
	}
	got, err := store.GetClassification(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if !got.Known || got.SKU != "SKU-1" || got.TemperatureClass != "Frozen" || !got.HasTag("Hazmat") || !got.HasTag("TemperatureSensitive") || len(got.HandlingTags) != 2 {
		t.Fatalf("view = %+v", got)
	}
}

func TestMemoryStore_UnknownSKUIsNotKnown(t *testing.T) {
	got, err := productclassificationcopy.NewMemoryStore().GetClassification(context.Background(), "SKU-unknown")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if got.Known || got.SKU != "SKU-unknown" || len(got.HandlingTags) != 0 {
		t.Fatalf("view = %+v, want Known=false for the SKU", got)
	}
}

func TestMemoryStore_VersionGuard(t *testing.T) {
	ctx := context.Background()
	store := productclassificationcopy.NewMemoryStore()
	if applied, _ := store.ApplyIfNewer(ctx, classified("SKU-1", "Hazmat"), 0, 3, time.Now()); !applied {
		t.Fatal("first version must apply")
	}

	for _, stale := range []int64{3, 2} {
		applied, err := store.ApplyIfNewer(ctx, classified("SKU-1", "Fragile"), 0, stale, time.Now())
		if err != nil || applied {
			t.Fatalf("version %d over stored 3: applied=%v err=%v, want ignored", stale, applied, err)
		}
	}
	if got, _ := store.GetClassification(ctx, "SKU-1"); !got.HasTag("Hazmat") || got.HasTag("Fragile") {
		t.Fatalf("stale version overwrote the copy: %+v", got)
	}

	if applied, _ := store.ApplyIfNewer(ctx, classified("SKU-1", "Fragile"), 0, 4, time.Now()); !applied {
		t.Fatal("newer version must apply")
	}
	got, _ := store.GetClassification(ctx, "SKU-1")
	if got.HasTag("Hazmat") || !got.HasTag("Fragile") || store.Version("SKU-1") != 4 {
		t.Fatalf("newer version not a full replacement: %+v (version %d)", got, store.Version("SKU-1"))
	}
}

func TestMemoryStore_CopiesTagsDefensively(t *testing.T) {
	ctx := context.Background()
	store := productclassificationcopy.NewMemoryStore()
	tags := []string{"Hazmat"}
	_, _ = store.ApplyIfNewer(ctx, classified("SKU-1", tags...), 0, 1, time.Now())
	tags[0] = "Fragile"
	got, _ := store.GetClassification(ctx, "SKU-1")
	got.HandlingTags[0] = "HighValue"
	again, _ := store.GetClassification(ctx, "SKU-1")
	if again.HandlingTags[0] != "Hazmat" {
		t.Fatalf("stored tags were aliased: %v", again.HandlingTags)
	}
}

func TestPermissiveLookup_AlwaysUnknown(t *testing.T) {
	got, err := productclassificationcopy.NewPermissiveLookup().GetClassification(context.Background(), "SKU-1")
	if err != nil || got.Known || got.SKU != "SKU-1" {
		t.Fatalf("permissive = %+v, %v; want Known=false, nil", got, err)
	}
}
