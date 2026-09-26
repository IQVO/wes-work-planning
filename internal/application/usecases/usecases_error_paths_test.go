package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/charge"
	"github.com/claudioed/wes-work-planning/internal/domain/plan"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

type erroringEventPublisher struct {
	err error
}

func (p erroringEventPublisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
	return p.err
}

type saveErrPlanRepo struct {
	*memory.PlanRepo
	err error
}

func (r saveErrPlanRepo) Save(ctx context.Context, pathId shared.PathId, shiftPlan *plan.ShiftPlan) error {
	return r.err
}

type saveErrChargeRepo struct {
	*memory.ChargeRepo
	err error
}

func (r saveErrChargeRepo) Save(ctx context.Context, forecast *charge.ChargeForecast) error {
	return r.err
}

type saveErrWorkUnitRepo struct {
	*memory.WorkUnitRepo
	err error
}

func (r saveErrWorkUnitRepo) Save(ctx context.Context, unit *workunit.WorkUnit) error {
	return r.err
}

type saveErrWorkPoolRepo struct {
	*memory.WorkPoolRepo
	err error
}

func (r saveErrWorkPoolRepo) Save(ctx context.Context, pool *release.WorkPool) error {
	return r.err
}

type findErrWorkPoolRepo struct {
	err error
}

func (r findErrWorkPoolRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (*release.WorkPool, error) {
	return nil, r.err
}

func (r findErrWorkPoolRepo) Save(ctx context.Context, pool *release.WorkPool) error {
	return nil
}

func TestCommitShiftPlan_InvalidHours(t *testing.T) {
	f := newFixture()
	uc := usecases.NewCommitShiftPlan(f.plans, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	rate, _ := shared.NewRate(50)
	heads, _ := shared.NewStationCount(4)
	installed, _ := shared.NewStationCount(5)

	_, err := uc.Execute(context.Background(), usecases.CommitShiftPlanRequest{
		PathId:            pathId,
		PlannedHeads:      heads,
		InstalledStations: installed,
		Rate:              rate,
		Hours:             0,
	})
	if !errors.Is(err, shared.ErrInvalidHours) {
		t.Fatalf("got err %v, want %v", err, shared.ErrInvalidHours)
	}
}

func TestEnqueueWorkUnit_ValidatesWorkUnit(t *testing.T) {
	f := newFixture()
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	_, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "",
		PathId:     pathId,
		CPT:        cpt,
		Reference:  "order-line-1",
	})
	if !errors.Is(err, workunit.ErrEmptyId) {
		t.Fatalf("got err %v, want %v", err, workunit.ErrEmptyId)
	}
}

func TestEnqueueWorkUnit_RejectsDuplicateEntry(t *testing.T) {
	f := newFixture()
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	ctx := context.Background()

	if _, err := uc.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: cpt, Reference: "order-line-1",
	}); err != nil {
		t.Fatalf("unexpected error on first enqueue: %v", err)
	}

	_, err := uc.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: cpt, Reference: "order-line-1",
	})
	if !errors.Is(err, release.ErrDuplicateEntry) {
		t.Fatalf("got err %v, want %v", err, release.ErrDuplicateEntry)
	}
}

func TestReceiveChargeForecast_NoBuckets(t *testing.T) {
	f := newFixture()
	uc := usecases.NewReceiveChargeForecast(f.charges, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")

	_, err := uc.Execute(context.Background(), usecases.ReceiveChargeForecastRequest{
		PathId:  pathId,
		Buckets: nil,
	})
	if !errors.Is(err, charge.ErrNoBuckets) {
		t.Fatalf("got err %v, want %v", err, charge.ErrNoBuckets)
	}
}

func TestRecordCompletion_UnknownWorkUnit(t *testing.T) {
	f := newFixture()
	uc := usecases.NewRecordCompletion(f.workUnits, f.pools, f.publisher, f.clock)

	_, err := uc.Execute(context.Background(), usecases.RecordCompletionRequest{WorkUnitId: "does-not-exist"})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("got err %v, want %v", err, ports.ErrNotFound)
	}
}

func TestReleaseNextWork_UnknownPath(t *testing.T) {
	f := newFixture()
	uc := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")

	_, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("got err %v, want %v", err, ports.ErrNotFound)
	}
}

func TestReleaseNextWork_EmptyPool(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.ReleaseFed, 10, 0)
	if err := f.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uc := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	_, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if !errors.Is(err, release.ErrEmptyPool) {
		t.Fatalf("got err %v, want %v", err, release.ErrEmptyPool)
	}
}

func TestReleaseNextWork_WIPLimitReached(t *testing.T) {
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

	uc := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	_, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if !errors.Is(err, release.ErrWIPLimitReached) {
		t.Fatalf("got err %v, want %v", err, release.ErrWIPLimitReached)
	}
}

func TestReleaseNextWork_UnknownWorkUnitInRepo(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.ReleaseFed, 10, 0)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	if err := pool.Enqueue("wu-ghost", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uc := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	_, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("got err %v, want %v", err, ports.ErrNotFound)
	}
}

func TestReleaseNextWork_UnitAlreadyReleasedInconsistentState(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	ctx := context.Background()

	unit, err := workunit.NewWorkUnit("wu-1", pathId, cpt, "ref-1")
	if err != nil {
		t.Fatalf("unexpected error building fixture: %v", err)
	}
	if err := unit.Release(f.clock.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.workUnits.Save(ctx, unit); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pool := release.NewWorkPool(pathId, release.ReleaseFed, 10, 0)
	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.pools.Save(ctx, pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uc := usecases.NewReleaseNextWork(f.pools, f.workUnits, f.publisher, f.clock)
	_, err = uc.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId})
	if !errors.Is(err, workunit.ErrAlreadyReleased) {
		t.Fatalf("got err %v, want %v", err, workunit.ErrAlreadyReleased)
	}
}

func TestSampleBacklog_UnknownPath(t *testing.T) {
	f := newFixture()
	uc := usecases.NewSampleBacklog(f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")

	_, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("got err %v, want %v", err, ports.ErrNotFound)
	}
}

func TestSampleBacklog_UnderThresholdRaisesNoEvent(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.FlowFed, 0, 10)
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
	if snapshot.OverAlarmThreshold {
		t.Fatalf("expected snapshot to be under alarm threshold")
	}
	if snapshot.Mode != "FlowFed" {
		t.Fatalf("got mode %q, want FlowFed", snapshot.Mode)
	}
	if len(f.publisher.Events()) != 0 {
		t.Fatalf("got %d events, want 0", len(f.publisher.Events()))
	}
}

func TestRebalanceDecision_UnknownPath(t *testing.T) {
	f := newFixture()
	uc := usecases.NewRebalanceDecision(f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")

	_, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("got err %v, want %v", err, ports.ErrNotFound)
	}
}

func TestRebalanceDecision_FlowFedOverThresholdRecommendsThrottle(t *testing.T) {
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

	uc := usecases.NewRebalanceDecision(f.pools, f.publisher, f.clock)
	rec, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Action != usecases.ThrottleUpstream {
		t.Fatalf("got action %v, want ThrottleUpstream", rec.Action)
	}
	if len(f.publisher.Events()) != 1 {
		t.Fatalf("got %d events, want 1", len(f.publisher.Events()))
	}
}

func TestRebalanceDecision_FlowFedUnderThresholdRecommendsNoAction(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.FlowFed, 0, 10)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	if err := pool.Enqueue("wu-1", cpt); err != nil {
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
	if rec.Action != usecases.NoActionNeeded {
		t.Fatalf("got action %v, want NoActionNeeded", rec.Action)
	}
	if len(f.publisher.Events()) != 0 {
		t.Fatalf("got %d events, want 0", len(f.publisher.Events()))
	}
}

func TestRebalanceDecision_ReleaseFedUnderWIPLimitRecommendsNoAction(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.ReleaseFed, 10, 0)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	if err := pool.Enqueue("wu-1", cpt); err != nil {
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
	if rec.Action != usecases.NoActionNeeded {
		t.Fatalf("got action %v, want NoActionNeeded", rec.Action)
	}
}

func TestRebalanceAction_String(t *testing.T) {
	tests := []struct {
		name   string
		action usecases.RebalanceAction
		want   string
	}{
		{"no action needed", usecases.NoActionNeeded, "NoActionNeeded"},
		{"throttle upstream", usecases.ThrottleUpstream, "ThrottleUpstream"},
		{"reassign labor", usecases.ReassignLabor, "ReassignLabor"},
		{"unknown", usecases.RebalanceAction(99), "NoActionNeeded"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.action.String(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

type erroringProcessedEventRepo struct {
	err error
}

func (r erroringProcessedEventRepo) TryMarkProcessed(ctx context.Context, eventId string, processedAt time.Time) (bool, error) {
	return false, r.err
}

func TestObserveLaborPlan_TryMarkProcessedError(t *testing.T) {
	views := memory.NewLaborPlanViewRepo()
	wantErr := errors.New("processed-event-store unavailable")
	uc := usecases.NewObserveLaborPlan(views, erroringProcessedEventRepo{err: wantErr})

	pathId, _ := shared.NewPathId("pick-a")
	now := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)

	err := uc.Execute(context.Background(), usecases.ObserveLaborPlanRequest{
		EventId: "evt-1", PathId: pathId, PlannedHeads: 5, PlannedRate: 120, PlannedHours: 8, ObservedAt: now,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestObserveInventoryChange_TryMarkProcessedError(t *testing.T) {
	views := memory.NewInventoryViewRepo()
	wantErr := errors.New("processed-event-store unavailable")
	uc := usecases.NewObserveInventoryChange(views, erroringProcessedEventRepo{err: wantErr})

	now := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)

	_, err := uc.Execute(context.Background(), usecases.ObserveInventoryChangeRequest{
		EventId: "evt-1", SKU: "sku-1", Quantity: 10, Delta: -10, ObservedAt: now,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestCommitShiftPlan_PublishesShiftPlanCommittedEvent(t *testing.T) {
	f := newFixture()
	uc := usecases.NewCommitShiftPlan(f.plans, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-b")
	rate, _ := shared.NewRate(30)
	heads, _ := shared.NewStationCount(2)
	installed, _ := shared.NewStationCount(2)

	_, err := uc.Execute(context.Background(), usecases.CommitShiftPlanRequest{
		PathId:            pathId,
		PlannedHeads:      heads,
		InstalledStations: installed,
		Rate:              rate,
		Hours:             6,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stored, err := f.plans.FindByPathId(context.Background(), pathId)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := stored.PathPlan(pathId); !ok {
		t.Fatalf("expected stored plan to contain path plan")
	}
	if len(f.publisher.Events()) != 1 {
		t.Fatalf("got %d events, want 1", len(f.publisher.Events()))
	}
}

func TestCommitShiftPlan_SaveError(t *testing.T) {
	f := newFixture()
	wantErr := errors.New("plan store unavailable")
	repo := saveErrPlanRepo{PlanRepo: f.plans, err: wantErr}
	uc := usecases.NewCommitShiftPlan(repo, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	rate, _ := shared.NewRate(50)
	heads, _ := shared.NewStationCount(4)
	installed, _ := shared.NewStationCount(5)

	_, err := uc.Execute(context.Background(), usecases.CommitShiftPlanRequest{
		PathId: pathId, PlannedHeads: heads, InstalledStations: installed, Rate: rate, Hours: 8,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestCommitShiftPlan_PublishError(t *testing.T) {
	f := newFixture()
	wantErr := errors.New("broker unavailable")
	uc := usecases.NewCommitShiftPlan(f.plans, erroringEventPublisher{err: wantErr}, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	rate, _ := shared.NewRate(50)
	heads, _ := shared.NewStationCount(4)
	installed, _ := shared.NewStationCount(5)

	_, err := uc.Execute(context.Background(), usecases.CommitShiftPlanRequest{
		PathId: pathId, PlannedHeads: heads, InstalledStations: installed, Rate: rate, Hours: 8,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestEnqueueWorkUnit_PoolLookupError(t *testing.T) {
	f := newFixture()
	wantErr := errors.New("pool store unavailable")
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, findErrWorkPoolRepo{err: wantErr}, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	_, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: cpt, Reference: "ref-1",
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestEnqueueWorkUnit_WorkUnitSaveError(t *testing.T) {
	f := newFixture()
	wantErr := errors.New("work unit store unavailable")
	repo := saveErrWorkUnitRepo{WorkUnitRepo: f.workUnits, err: wantErr}
	uc := usecases.NewEnqueueWorkUnit(repo, f.pools, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	_, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: cpt, Reference: "ref-1",
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestEnqueueWorkUnit_PoolSaveError(t *testing.T) {
	f := newFixture()
	wantErr := errors.New("pool store unavailable")
	repo := saveErrWorkPoolRepo{WorkPoolRepo: f.pools, err: wantErr}
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, repo, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	_, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: cpt, Reference: "ref-1",
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestEnqueueWorkUnit_PublishError(t *testing.T) {
	f := newFixture()
	wantErr := errors.New("broker unavailable")
	uc := usecases.NewEnqueueWorkUnit(f.workUnits, f.pools, erroringEventPublisher{err: wantErr}, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	_, err := uc.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: cpt, Reference: "ref-1",
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestReceiveChargeForecast_SaveError(t *testing.T) {
	f := newFixture()
	wantErr := errors.New("charge store unavailable")
	repo := saveErrChargeRepo{ChargeRepo: f.charges, err: wantErr}
	uc := usecases.NewReceiveChargeForecast(repo, f.publisher, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	qty, _ := shared.NewQuantity(100)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	_, err := uc.Execute(context.Background(), usecases.ReceiveChargeForecastRequest{
		PathId: pathId, Buckets: []usecases.CPTBucketInput{{CPT: cpt, Quantity: qty}},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestReceiveChargeForecast_PublishError(t *testing.T) {
	f := newFixture()
	wantErr := errors.New("broker unavailable")
	uc := usecases.NewReceiveChargeForecast(f.charges, erroringEventPublisher{err: wantErr}, f.clock)
	pathId, _ := shared.NewPathId("pick-a")
	qty, _ := shared.NewQuantity(100)
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))

	_, err := uc.Execute(context.Background(), usecases.ReceiveChargeForecastRequest{
		PathId: pathId, Buckets: []usecases.CPTBucketInput{{CPT: cpt, Quantity: qty}},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestRecordCompletion_SaveError(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	unit, err := workunit.NewWorkUnit("wu-1", pathId, cpt, "ref-1")
	if err != nil {
		t.Fatalf("unexpected error building fixture: %v", err)
	}
	if err := unit.Release(f.clock.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantErr := errors.New("work unit store unavailable")
	repo := saveErrWorkUnitRepo{WorkUnitRepo: f.workUnits, err: wantErr}
	uc := usecases.NewRecordCompletion(repo, f.pools, f.publisher, f.clock)

	_, err = uc.Execute(context.Background(), usecases.RecordCompletionRequest{WorkUnitId: "wu-1"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestRecordCompletion_PublishError(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	unit, err := workunit.NewWorkUnit("wu-1", pathId, cpt, "ref-1")
	if err != nil {
		t.Fatalf("unexpected error building fixture: %v", err)
	}
	if err := unit.Release(f.clock.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantErr := errors.New("broker unavailable")
	uc := usecases.NewRecordCompletion(f.workUnits, f.pools, erroringEventPublisher{err: wantErr}, f.clock)

	_, err = uc.Execute(context.Background(), usecases.RecordCompletionRequest{WorkUnitId: "wu-1"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

// TestRecordCompletion_PoolLookupErrorFailsScope proves the distinction
// between the two pool-side outcomes on RecordCompletion: a MISSING pool
// (ports.ErrNotFound) is best-effort skipped, but a pool lookup that
// FAILS for another reason (store down, timeout) is not swallowed — it
// fails the whole scope so the completion is retried atomically, keeping
// the WorkUnit save and its WIP-slot free consistent.
func TestRecordCompletion_PoolLookupErrorFailsScope(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	unit, err := workunit.NewWorkUnit("wu-1", pathId, cpt, "ref-1")
	if err != nil {
		t.Fatalf("unexpected error building fixture: %v", err)
	}
	if err := unit.Release(f.clock.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantErr := errors.New("pool store unavailable")
	uc := usecases.NewRecordCompletion(f.workUnits, findErrWorkPoolRepo{err: wantErr}, f.publisher, f.clock)

	if _, err := uc.Execute(context.Background(), usecases.RecordCompletionRequest{WorkUnitId: "wu-1"}); !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v (a failing pool lookup must fail the scope)", err, wantErr)
	}
	if len(f.publisher.Events()) != 0 {
		t.Fatalf("got %d events, want 0 (nothing published on a failed scope)", len(f.publisher.Events()))
	}
}

func releaseFixtureUnit(t *testing.T, f fixture, pathId shared.PathId) {
	t.Helper()
	cpt := shared.NewCPT(f.clock.Now().Add(time.Hour))
	unit, err := workunit.NewWorkUnit("wu-1", pathId, cpt, "ref-1")
	if err != nil {
		t.Fatalf("unexpected error building fixture: %v", err)
	}
	if err := f.workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pool := release.NewWorkPool(pathId, release.ReleaseFed, 10, 0)
	if err := pool.Enqueue("wu-1", cpt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := f.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReleaseNextWork_PoolSaveError(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	releaseFixtureUnit(t, f, pathId)

	wantErr := errors.New("pool store unavailable")
	repo := saveErrWorkPoolRepo{WorkPoolRepo: f.pools, err: wantErr}
	uc := usecases.NewReleaseNextWork(repo, f.workUnits, f.publisher, f.clock)

	_, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestReleaseNextWork_WorkUnitSaveError(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	releaseFixtureUnit(t, f, pathId)

	wantErr := errors.New("work unit store unavailable")
	repo := saveErrWorkUnitRepo{WorkUnitRepo: f.workUnits, err: wantErr}
	uc := usecases.NewReleaseNextWork(f.pools, repo, f.publisher, f.clock)

	_, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestReleaseNextWork_PublishError(t *testing.T) {
	f := newFixture()
	pathId, _ := shared.NewPathId("pick-a")
	releaseFixtureUnit(t, f, pathId)

	wantErr := errors.New("broker unavailable")
	uc := usecases.NewReleaseNextWork(f.pools, f.workUnits, erroringEventPublisher{err: wantErr}, f.clock)

	_, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestSampleBacklog_PublishError(t *testing.T) {
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

	wantErr := errors.New("broker unavailable")
	uc := usecases.NewSampleBacklog(f.pools, erroringEventPublisher{err: wantErr}, f.clock)

	_, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestRebalanceDecision_PublishErrorOnThrottle(t *testing.T) {
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

	wantErr := errors.New("broker unavailable")
	uc := usecases.NewRebalanceDecision(f.pools, erroringEventPublisher{err: wantErr}, f.clock)

	_, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}

func TestRebalanceDecision_PublishErrorOnReassign(t *testing.T) {
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

	wantErr := errors.New("broker unavailable")
	uc := usecases.NewRebalanceDecision(f.pools, erroringEventPublisher{err: wantErr}, f.clock)

	_, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
}
