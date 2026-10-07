package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// failingCopies is a ProductClassificationCopyRepo whose upsert fails
// `remaining` times before delegating.
type failingCopies struct {
	*productclassificationcopy.MemoryStore
	remaining int
}

func (f *failingCopies) ApplyIfNewer(ctx context.Context, view productclassificationview.ProductClassificationView, dot int, version int64, at time.Time) (bool, error) {
	if f.remaining > 0 {
		f.remaining--
		return false, errTransient
	}
	return f.MemoryStore.ApplyIfNewer(ctx, view, dot, version, at)
}

func classifiedRequest(eventId string, version int64, tags ...string) usecases.ObserveProductClassificationRequest {
	return usecases.ObserveProductClassificationRequest{
		EventId: eventId, SKU: "SKU-1", HandlingTags: tags, TemperatureClass: "Frozen", DOTHazardClass: 3,
		Version: version, ObservedAt: time.Now(),
	}
}

func TestObserveProductClassification_AppliesStaleAndRedelivered(t *testing.T) {
	ctx := context.Background()
	copies := productclassificationcopy.NewMemoryStore()
	uc := usecases.NewObserveProductClassification(copies, memory.NewProcessedEventRepo())

	steps := []struct {
		name    string
		req     usecases.ObserveProductClassificationRequest
		want    usecases.ProductClassificationOutcome
		wantTag string
		version int64
	}{
		{"first version applies", classifiedRequest("evt-1", 2, "Hazmat"), usecases.ProductClassificationApplied, "Hazmat", 2},
		{"same id is a redelivery", classifiedRequest("evt-1", 2, "Hazmat"), usecases.ProductClassificationRedelivered, "Hazmat", 2},
		{"older version under a new id is stale", classifiedRequest("evt-0", 1, "Fragile"), usecases.ProductClassificationStale, "Hazmat", 2},
		{"equal version under a new id is stale", classifiedRequest("evt-2", 2, "Fragile"), usecases.ProductClassificationStale, "Hazmat", 2},
		{"newer version replaces", classifiedRequest("evt-3", 3, "Fragile"), usecases.ProductClassificationApplied, "Fragile", 3},
	}
	for _, s := range steps {
		got, err := uc.Execute(ctx, s.req)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if got != s.want {
			t.Fatalf("%s: outcome = %v, want %v", s.name, got, s.want)
		}
		view, _ := copies.GetClassification(ctx, "SKU-1")
		if !view.Known || !view.HasTag(s.wantTag) || len(view.HandlingTags) != 1 || copies.Version("SKU-1") != s.version {
			t.Fatalf("%s: copy = %+v version %d, want only %s at %d", s.name, view, copies.Version("SKU-1"), s.wantTag, s.version)
		}
	}
	if view, _ := copies.GetClassification(ctx, "SKU-1"); view.TemperatureClass != "Frozen" {
		t.Fatalf("temperature class not stored: %+v", view)
	}
}

func TestObserveProductClassification_FailedUpsertIsRetriedNotSwallowed(t *testing.T) {
	ctx := context.Background()
	copies := &failingCopies{MemoryStore: productclassificationcopy.NewMemoryStore(), remaining: 1}
	uc := usecases.NewObserveProductClassification(copies, memory.NewProcessedEventRepo())

	if _, err := uc.Execute(ctx, classifiedRequest("evt-1", 1, "Hazmat")); !errors.Is(err, errTransient) {
		t.Fatalf("first attempt err = %v, want the transient error", err)
	}
	got, err := uc.Execute(ctx, classifiedRequest("evt-1", 1, "Hazmat"))
	if err != nil || got != usecases.ProductClassificationApplied {
		t.Fatalf("retry = %v, %v; want applied (the failed attempt must not leave its mark)", got, err)
	}
}

func TestObserveProductClassification_UnitOfWorkRollsBackTheMark(t *testing.T) {
	ctx := context.Background()
	processed := newStagingProcessed()
	copies := &failingCopies{MemoryStore: productclassificationcopy.NewMemoryStore(), remaining: 1}
	uc := usecases.NewObserveProductClassification(copies, processed).WithUnitOfWork(fakeUoW{processed: processed})

	if _, err := uc.Execute(ctx, classifiedRequest("evt-1", 1, "Hazmat")); err == nil {
		t.Fatal("expected the upsert failure to surface")
	}
	got, err := uc.Execute(ctx, classifiedRequest("evt-1", 1, "Hazmat"))
	if err != nil || got != usecases.ProductClassificationApplied {
		t.Fatalf("retry = %v, %v; want applied", got, err)
	}
	if again, _ := uc.Execute(ctx, classifiedRequest("evt-1", 1, "Hazmat")); again != usecases.ProductClassificationRedelivered {
		t.Fatalf("third delivery = %v, want redelivered", again)
	}
}

func TestProductClassificationOutcome_String(t *testing.T) {
	for outcome, want := range map[usecases.ProductClassificationOutcome]string{
		usecases.ProductClassificationApplied:     "applied",
		usecases.ProductClassificationStale:       "stale",
		usecases.ProductClassificationRedelivered: "redelivered",
		usecases.ProductClassificationOutcome(99): "unknown",
	} {
		if got := outcome.String(); got != want {
			t.Fatalf("%d.String() = %q, want %q", outcome, got, want)
		}
	}
}
