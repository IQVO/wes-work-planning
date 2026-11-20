package workunit

import (
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func TestState_String(t *testing.T) {
	tests := []struct {
		name  string
		state State
		want  string
	}{
		{"pending", Pending, "Pending"},
		{"released", Released, "Released"},
		{"completed", Completed, "Completed"},
		{"unknown", State(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWorkUnit_Accessors(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(time.Now())

	w, err := NewWorkUnit("wu-1", pathId, cpt, "order-line-1")
	if err != nil {
		t.Fatalf("unexpected error building fixture: %v", err)
	}

	if got := w.Id(); got != "wu-1" {
		t.Fatalf("got Id %q, want %q", got, "wu-1")
	}
	if got := w.PathId(); !got.Equals(pathId) {
		t.Fatalf("got PathId %v, want %v", got, pathId)
	}
	if got := w.CPT(); !got.Equals(cpt) {
		t.Fatalf("got CPT %v, want %v", got, cpt)
	}
	if got := w.Reference(); got != "order-line-1" {
		t.Fatalf("got Reference %q, want %q", got, "order-line-1")
	}
}

func TestWorkUnit_ReleasedAtAndCompletedAt_Lifecycle(t *testing.T) {
	w := mustWorkUnit(t)

	if got := w.ReleasedAt(); got != nil {
		t.Fatalf("got ReleasedAt %v, want nil before release", got)
	}
	if got := w.CompletedAt(); got != nil {
		t.Fatalf("got CompletedAt %v, want nil before release", got)
	}

	releaseTime := time.Now()
	if err := w.Release(releaseTime); err != nil {
		t.Fatalf("unexpected error on release: %v", err)
	}

	if got := w.ReleasedAt(); got == nil || !got.Equal(releaseTime) {
		t.Fatalf("got ReleasedAt %v, want %v", got, releaseTime)
	}
	if got := w.CompletedAt(); got != nil {
		t.Fatalf("got CompletedAt %v, want nil before completion", got)
	}

	completeTime := releaseTime.Add(time.Hour)
	if err := w.Complete(completeTime); err != nil {
		t.Fatalf("unexpected error on complete: %v", err)
	}

	if got := w.CompletedAt(); got == nil || !got.Equal(completeTime) {
		t.Fatalf("got CompletedAt %v, want %v", got, completeTime)
	}
	if got := w.ReleasedAt(); got == nil || !got.Equal(releaseTime) {
		t.Fatalf("got ReleasedAt %v, want %v (unchanged after completion)", got, releaseTime)
	}
}

func TestWorkUnit_SKU_DefaultsEmptyAndIsSetAtConstruction(t *testing.T) {
	w := mustWorkUnit(t)

	if got := w.SKU(); got != "" {
		t.Fatalf("got SKU %q, want empty by default", got)
	}

	pathId, _ := shared.NewPathId("pick-a")
	w, err := NewWorkUnit("wu-1", pathId, shared.NewCPT(time.Now()), "order-line-1", WithSKU("sku-482910"))
	if err != nil {
		t.Fatalf("unexpected error building fixture: %v", err)
	}
	if got := w.SKU(); got != "sku-482910" {
		t.Fatalf("got SKU %q, want sku-482910", got)
	}
}

func TestWorkUnit_GiftWrap_DefaultsFalseAndIsSetAtConstruction(t *testing.T) {
	w := mustWorkUnit(t)

	if got := w.GiftWrap(); got != false {
		t.Fatalf("got GiftWrap %v, want false by default", got)
	}

	pathId, _ := shared.NewPathId("pick-a")
	w, err := NewWorkUnit("wu-1", pathId, shared.NewCPT(time.Now()), "order-line-1", WithGiftWrap(true))
	if err != nil {
		t.Fatalf("unexpected error building fixture: %v", err)
	}
	if got := w.GiftWrap(); got != true {
		t.Fatalf("got GiftWrap %v, want true", got)
	}

	w, err = NewWorkUnit("wu-1", pathId, shared.NewCPT(time.Now()), "order-line-1", WithGiftWrap(false))
	if err != nil {
		t.Fatalf("unexpected error building fixture: %v", err)
	}
	if got := w.GiftWrap(); got != false {
		t.Fatalf("got GiftWrap %v, want false", got)
	}
}
