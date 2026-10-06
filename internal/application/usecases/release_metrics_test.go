package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

type recordingReleaseMetrics struct{ paths []string }

func (r *recordingReleaseMetrics) WorkUnitReleased(_ context.Context, p shared.PathId) {
	r.paths = append(r.paths, p.String())
}

// ADR-0013 Tier 2: the metric is driven by the domain event (a work unit
// actually leaving the pool) through the ports.ReleaseMetrics port — the
// application layer no longer touches OTel.
func TestReleaseNextWork_RecordsReleaseThroughMetricsPort(t *testing.T) {
	f := newFixture()
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	metrics := &recordingReleaseMetrics{}
	releaseUC := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock).WithMetrics(metrics)
	pathId, _ := shared.NewPathId("metrics-path")

	ctx := context.Background()
	for _, id := range []string{"wu-1", "wu-2"} {
		if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
			WorkUnitId: id, PathId: pathId, CPT: shared.NewCPT(f.clock.Now().Add(time.Hour)), Reference: "ref-" + id,
		}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for i := range 2 {
		if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
	}
	// A failed release (empty pool) must not be counted.
	if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err == nil {
		t.Fatal("expected an error releasing from an empty pool")
	}

	if len(metrics.paths) != 2 || metrics.paths[0] != "metrics-path" || metrics.paths[1] != "metrics-path" {
		t.Fatalf("recorded = %v, want exactly two releases for metrics-path", metrics.paths)
	}
}

// A nil metrics port (the default) records nothing and never panics.
func TestReleaseNextWork_WithoutMetricsPortStillReleases(t *testing.T) {
	f := newFixture()
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	releaseUC := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("no-metrics")
	ctx := context.Background()
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: shared.NewCPT(f.clock.Now().Add(time.Hour)), Reference: "ref",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("release: %v", err)
	}
}
