package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

type fixture struct {
	pools     *memory.WorkPoolRepo
	workUnits *memory.WorkUnitRepo
	publisher *events.LogPublisher
	clock     memory.FixedClock
}

func newFixture() fixture {
	return fixture{
		pools:     memory.NewWorkPoolRepo(),
		workUnits: memory.NewWorkUnitRepo(),
		publisher: events.NewLogPublisher(nil),
		clock:     memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)},
	}
}

func TestEnqueueWorkUnit_CreatesPoolOnFirstUse(t *testing.T) {
	f := newFixture()
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	unit, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1",
		PathId:     pathId,
		CPT:        cpt,
		Reference:  "order-line-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if unit.State() != workunit.Pending {
		t.Fatalf("got state %v, want Pending", unit.State())
	}

	pool, err := f.pools.FindByPathId(context.Background(), pathId)
	if err != nil {
		t.Fatalf("unexpected error fetching pool: %v", err)
	}
	if pool.BacklogDepth() != 1 {
		t.Fatalf("got backlog depth %d, want 1", pool.BacklogDepth())
	}
}

func TestEnqueueWorkUnit_ThreadsOptionalSKUThroughToTheWorkUnit(t *testing.T) {
	f := newFixture()
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	unit, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-sku-1",
		PathId:     pathId,
		CPT:        cpt,
		Reference:  "order-line-1",
		SKU:        "sku-482910",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := unit.SKU(); got != "sku-482910" {
		t.Fatalf("got SKU %q, want sku-482910", got)
	}

	stored, err := f.workUnits.FindById(context.Background(), "wu-sku-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := stored.SKU(); got != "sku-482910" {
		t.Fatalf("got stored SKU %q, want sku-482910", got)
	}
}

func TestEnqueueWorkUnit_OmittedSKUDefaultsEmpty(t *testing.T) {
	f := newFixture()
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	unit, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-no-sku",
		PathId:     pathId,
		CPT:        cpt,
		Reference:  "order-line-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := unit.SKU(); got != "" {
		t.Fatalf("got SKU %q, want empty", got)
	}
}

func TestEnqueueWorkUnit_ThreadsOptionalGiftWrapThroughToTheWorkUnit(t *testing.T) {
	f := newFixture()
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	unit, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-gift-wrap-1",
		PathId:     pathId,
		CPT:        cpt,
		Reference:  "order-line-1",
		GiftWrap:   true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := unit.GiftWrap(); got != true {
		t.Fatalf("got GiftWrap %v, want true", got)
	}

	stored, err := f.workUnits.FindById(context.Background(), "wu-gift-wrap-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := stored.GiftWrap(); got != true {
		t.Fatalf("got stored GiftWrap %v, want true", got)
	}
}

func TestEnqueueWorkUnit_OmittedGiftWrapDefaultsFalse(t *testing.T) {
	f := newFixture()
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	unit, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-no-gift-wrap",
		PathId:     pathId,
		CPT:        cpt,
		Reference:  "order-line-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := unit.GiftWrap(); got != false {
		t.Fatalf("got GiftWrap %v, want false", got)
	}
}

func TestReleaseNextWork_ReleasesEarliestCPT(t *testing.T) {
	f := newFixture()
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	releaseUC := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")

	late := shared.NewCPT(f.clock.Now().Add(2 * time.Hour))
	early := shared.NewCPT(f.clock.Now().Add(time.Hour))

	ctx := context.Background()
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{WorkUnitId: "wu-late", PathId: pathId, CPT: late, Reference: "ref-late"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{WorkUnitId: "wu-early", PathId: pathId, CPT: early, Reference: "ref-early"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	released, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if released.Id() != "wu-early" {
		t.Fatalf("got %q, want wu-early", released.Id())
	}
	if released.State() != workunit.Released {
		t.Fatalf("got state %v, want Released", released.State())
	}
}

func TestRecordCompletion_RejectsDoubleComplete(t *testing.T) {
	f := newFixture()
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	releaseUC := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	complete := usecases.NewRecordCompletion(f.workUnits, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	ctx := context.Background()
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{WorkUnitId: "wu-1", PathId: pathId, CPT: cpt, Reference: "ref-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: "wu-1"}); err != nil {
		t.Fatalf("unexpected error on first completion: %v", err)
	}
	if _, err := complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: "wu-1"}); !errors.Is(err, workunit.ErrAlreadyCompleted) {
		t.Fatalf("got err %v, want %v", err, workunit.ErrAlreadyCompleted)
	}
}

// TestRecordCompletion_FreesWorkPoolWIPSlot is the end-to-end regression
// test (through the wired use case, not just the domain aggregate) for the
// soak-scenario bug: RecordCompletion must free the completed unit's WIP
// slot on its release-fed work pool, or every subsequent ReleaseNextWork
// call on that path 409s forever once the pool's lifetime-released count
// reaches its WIP limit -- regardless of how much of that work has since
// finished.
func TestRecordCompletion_FreesWorkPoolWIPSlot(t *testing.T) {
	f := newFixture()
	enqueue := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	releaseUC := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	complete := usecases.NewRecordCompletion(f.workUnits, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-wip-1")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	ctx := context.Background()

	// A release-fed pool with WIP limit 1: only one outstanding release
	// allowed at a time.
	pool := release.NewWorkPool(pathId, release.ReleaseFed, 1, 0)
	if err := f.pools.Save(ctx, pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{WorkUnitId: "wu-1", PathId: pathId, CPT: cpt, Reference: "ref-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{WorkUnitId: "wu-2", PathId: pathId, CPT: cpt, Reference: "ref-2"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("unexpected error releasing wu-1: %v", err)
	}

	// At the WIP limit: releasing again must 409 (ErrWIPLimitReached) --
	// this is the state the soak scenario got permanently stuck in.
	if _, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); !errors.Is(err, release.ErrWIPLimitReached) {
		t.Fatalf("got err %v, want %v", err, release.ErrWIPLimitReached)
	}

	if _, err := complete.Execute(ctx, usecases.RecordCompletionRequest{WorkUnitId: "wu-1"}); err != nil {
		t.Fatalf("unexpected error completing wu-1: %v", err)
	}

	// The slot RecordCompletion just freed must admit wu-2.
	released, err := releaseUC.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("expected the freed WIP slot to admit wu-2, got error: %v", err)
	}
	if released.Id() != "wu-2" {
		t.Fatalf("got %q, want wu-2", released.Id())
	}
}

func TestSampleBacklog_RaisesThresholdEvent(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.FlowFed, 0, 1)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-2", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uc := usecases.NewSampleBacklog(f.pools, f.publisher, f.clock)
	snapshot, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !snapshot.OverAlarmThreshold {
		t.Fatalf("expected snapshot to be over alarm threshold")
	}
	if len(f.publisher.Events()) != 1 {
		t.Fatalf("got %d events, want 1", len(f.publisher.Events()))
	}
}

func TestSampleBacklog_CutoffAtOmitted_DoesNotPublishCapacityEvent(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.ReleaseFed, 5, 3)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uc := usecases.NewSampleBacklog(f.pools, f.publisher, f.clock)
	snapshot, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snapshot.RemainingCapacityKnown {
		t.Fatal("expected RemainingCapacityKnown to stay false when CutoffAt is omitted")
	}
	if len(f.publisher.Events()) != 0 {
		t.Fatalf("got %d events, want 0 when CutoffAt is omitted and pool is under threshold", len(f.publisher.Events()))
	}
}

func TestSampleBacklog_CutoffAtSupplied_ReleaseFed_PublishesPathCapacityChanged(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.ReleaseFed, 5, 100)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	for _, id := range []string{"wu-1", "wu-2"} {
		if err := pool.Enqueue(id, cpt); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if err := pool.Release("wu-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cutoff := f.clock.Now().Add(3 * time.Hour)
	uc := usecases.NewSampleBacklog(f.pools, f.publisher, f.clock)
	snapshot, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId, CutoffAt: cutoff})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !snapshot.RemainingCapacityKnown {
		t.Fatal("expected RemainingCapacityKnown=true for a ReleaseFed pool with a positive WIP limit")
	}
	if snapshot.RemainingCapacityUnits != 4 {
		t.Fatalf("got RemainingCapacityUnits %d, want 4 (limit 5 - WIP 1)", snapshot.RemainingCapacityUnits)
	}

	events := f.publisher.Events()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	capEvent, ok := events[0].(shared.PathCapacityChanged)
	if !ok {
		t.Fatalf("got event %T, want shared.PathCapacityChanged", events[0])
	}
	if !capEvent.Known {
		t.Fatal("expected Known=true on the published event")
	}
	if capEvent.RemainingUnits != 4 {
		t.Fatalf("got RemainingUnits %d, want 4", capEvent.RemainingUnits)
	}
	if !capEvent.CutoffAt.Equal(cutoff) {
		t.Fatalf("got CutoffAt %v, want %v", capEvent.CutoffAt, cutoff)
	}
	if !capEvent.PathId.Equals(pathId) {
		t.Fatalf("got PathId %v, want %v", capEvent.PathId, pathId)
	}
}

func TestSampleBacklog_CutoffAtSupplied_FlowFed_PublishesUnknownCapacity(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.FlowFed, 0, 100)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uc := usecases.NewSampleBacklog(f.pools, f.publisher, f.clock)
	cutoff := f.clock.Now().Add(time.Hour)
	snapshot, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId, CutoffAt: cutoff})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snapshot.RemainingCapacityKnown {
		t.Fatal("expected RemainingCapacityKnown=false for a FlowFed pool")
	}

	events := f.publisher.Events()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	capEvent, ok := events[0].(shared.PathCapacityChanged)
	if !ok {
		t.Fatalf("got event %T, want shared.PathCapacityChanged", events[0])
	}
	if capEvent.Known {
		t.Fatal("expected Known=false for a FlowFed pool")
	}
	if capEvent.RemainingUnits != 0 {
		t.Fatalf("got RemainingUnits %d, want 0 when unknown", capEvent.RemainingUnits)
	}
}

func TestSampleBacklog_CutoffAtSupplied_AndOverThreshold_PublishesBothEventsInOneCall(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.FlowFed, 0, 1)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	for _, id := range []string{"wu-1", "wu-2"} {
		if err := pool.Enqueue(id, cpt); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if err := f.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uc := usecases.NewSampleBacklog(f.pools, f.publisher, f.clock)
	cutoff := f.clock.Now().Add(time.Hour)
	snapshot, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId, CutoffAt: cutoff})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !snapshot.OverAlarmThreshold {
		t.Fatal("expected snapshot to be over alarm threshold")
	}

	events := f.publisher.Events()
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (BacklogThresholdBreached + PathCapacityChanged)", len(events))
	}
	if _, ok := events[0].(shared.BacklogThresholdBreached); !ok {
		t.Fatalf("got events[0] %T, want shared.BacklogThresholdBreached", events[0])
	}
	if _, ok := events[1].(shared.PathCapacityChanged); !ok {
		t.Fatalf("got events[1] %T, want shared.PathCapacityChanged", events[1])
	}
}

func TestRebalanceDecision_ReleaseFedRecommendsReassignLabor(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.ReleaseFed, 1, 0)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := pool.Enqueue("wu-2", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uc := usecases.NewRebalanceDecision(f.pools, f.publisher, f.clock)
	rec, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Action != usecases.ReassignLabor {
		t.Fatalf("got action %v, want ReassignLabor", rec.Action)
	}
}
