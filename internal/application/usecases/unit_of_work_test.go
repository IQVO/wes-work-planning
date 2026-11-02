package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/charge"
	"github.com/claudioed/wes-work-planning/internal/domain/plan"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// scopeKey marks a context as "inside the unit of work" so the fakes can
// assert every Save/Publish happened within the scope, never outside it.
type scopeKey struct{}

// recordingUnitOfWork is a ports.UnitOfWork fake that (a) tags the ctx it
// hands to fn, (b) counts how many scopes were opened, and (c) reports
// whether each scope committed (fn returned nil) or rolled back.
type recordingUnitOfWork struct {
	opened     int
	committed  int
	rolledBack int
	beginErr   error
}

func (u *recordingUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if u.beginErr != nil {
		return u.beginErr
	}
	u.opened++
	err := fn(context.WithValue(ctx, scopeKey{}, true))
	if err != nil {
		u.rolledBack++
		return err
	}
	u.committed++
	return nil
}

func inScope(ctx context.Context) bool {
	v, _ := ctx.Value(scopeKey{}).(bool)
	return v
}

// scopedPublisher records whether each Publish happened inside a scope and
// which events were published.
type scopedPublisher struct {
	inScope []bool
	events  []shared.DomainEvent
	err     error
}

func (p *scopedPublisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
	p.inScope = append(p.inScope, inScope(ctx))
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, events...)
	return nil
}

// scopedSaves wraps the fixture's repos to record whether each Save ran
// inside the scope.
type scopedSaves struct {
	fixture
	saves []bool
}

func newScopedSaves() *scopedSaves {
	return &scopedSaves{fixture: newFixture()}
}

type scopedPlanRepo struct {
	*scopedSaves
}

func (r scopedPlanRepo) Save(ctx context.Context, pathId shared.PathId, sp *plan.ShiftPlan) error {
	r.saves = append(r.saves, inScope(ctx))
	return r.plans.Save(ctx, pathId, sp)
}

func (r scopedPlanRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (*plan.ShiftPlan, error) {
	return r.plans.FindByPathId(ctx, pathId)
}

type scopedChargeRepo struct {
	*scopedSaves
}

func (r scopedChargeRepo) Save(ctx context.Context, f *charge.ChargeForecast) error {
	r.saves = append(r.saves, inScope(ctx))
	return r.charges.Save(ctx, f)
}

func (r scopedChargeRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (*charge.ChargeForecast, error) {
	return r.charges.FindByPathId(ctx, pathId)
}

type scopedWorkUnitRepo struct {
	*scopedSaves
}

func (r scopedWorkUnitRepo) Save(ctx context.Context, u *workunit.WorkUnit) error {
	r.saves = append(r.saves, inScope(ctx))
	return r.workUnits.Save(ctx, u)
}

func (r scopedWorkUnitRepo) FindById(ctx context.Context, id string) (*workunit.WorkUnit, error) {
	return r.workUnits.FindById(ctx, id)
}

func (r scopedWorkUnitRepo) FindByPathId(ctx context.Context, pathId shared.PathId) ([]*workunit.WorkUnit, error) {
	return r.workUnits.FindByPathId(ctx, pathId)
}

func (r scopedWorkUnitRepo) FindByReferencePage(ctx context.Context, ref string, after *workunit.Cursor, limit int) ([]*workunit.WorkUnit, error) {
	return r.workUnits.FindByReferencePage(ctx, ref, after, limit)
}

func (r scopedWorkUnitRepo) FindByReference(ctx context.Context, ref string) ([]*workunit.WorkUnit, error) {
	return r.workUnits.FindByReference(ctx, ref)
}

type scopedWorkPoolRepo struct {
	*scopedSaves
}

func (r scopedWorkPoolRepo) Save(ctx context.Context, p *release.WorkPool) error {
	r.saves = append(r.saves, inScope(ctx))
	return r.pools.Save(ctx, p)
}

func (r scopedWorkPoolRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (*release.WorkPool, error) {
	return r.pools.FindByPathId(ctx, pathId)
}

func allTrue(bs []bool) bool {
	for _, b := range bs {
		if !b {
			return false
		}
	}
	return true
}

func assertOneCommittedScope(t *testing.T, uow *recordingUnitOfWork, pub *scopedPublisher, saves []bool, wantSaves int) {
	t.Helper()
	if uow.opened != 1 || uow.committed != 1 || uow.rolledBack != 0 {
		t.Fatalf("expected exactly one committed scope, got opened=%d committed=%d rolledBack=%d", uow.opened, uow.committed, uow.rolledBack)
	}
	if len(saves) != wantSaves || !allTrue(saves) {
		t.Fatalf("expected %d Saves all inside the scope, got %v", wantSaves, saves)
	}
	if len(pub.inScope) != 1 || !pub.inScope[0] {
		t.Fatalf("expected one Publish inside the scope, got %v", pub.inScope)
	}
}

func assertRolledBack(t *testing.T, err error, uow *recordingUnitOfWork) {
	t.Helper()
	if err == nil || err.Error() != "outbox insert failed" {
		t.Fatalf("expected the publish error to propagate, got %v", err)
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected the scope to roll back, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

var errOutbox = errors.New("outbox insert failed")

// --- ReceiveChargeForecast -------------------------------------------------

func chargeReq(t *testing.T, f fixture) usecases.ReceiveChargeForecastRequest {
	t.Helper()
	pathId, _ := shared.NewPathId("pick-a")
	qty, _ := shared.NewQuantity(10)
	return usecases.ReceiveChargeForecastRequest{PathId: pathId, Buckets: []usecases.CPTBucketInput{{CPT: shared.NewCPT(f.clock.Now().Add(time.Hour)), Quantity: qty}}}
}

func TestReceiveChargeForecast_SaveAndPublishInsideOneUnitOfWork(t *testing.T) {
	s := newScopedSaves()
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewReceiveChargeForecast(scopedChargeRepo{s}, pub, s.clock).WithUnitOfWork(uow)

	if _, err := uc.Execute(context.Background(), chargeReq(t, s.fixture)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, s.saves, 1)
}

func TestReceiveChargeForecast_PublishFailureRollsBack(t *testing.T) {
	s := newScopedSaves()
	uow := &recordingUnitOfWork{}
	uc := usecases.NewReceiveChargeForecast(scopedChargeRepo{s}, &scopedPublisher{err: errOutbox}, s.clock).WithUnitOfWork(uow)
	_, err := uc.Execute(context.Background(), chargeReq(t, s.fixture))
	assertRolledBack(t, err, uow)
}

func TestReceiveChargeForecast_UnitOfWorkBeginFailurePropagates(t *testing.T) {
	s := newScopedSaves()
	pub := &scopedPublisher{}
	uc := usecases.NewReceiveChargeForecast(scopedChargeRepo{s}, pub, s.clock).WithUnitOfWork(&recordingUnitOfWork{beginErr: errors.New("begin failed")})
	if _, err := uc.Execute(context.Background(), chargeReq(t, s.fixture)); err == nil || err.Error() != "begin failed" {
		t.Fatalf("expected begin error, got %v", err)
	}
	if len(pub.events) != 0 || len(s.saves) != 0 {
		t.Fatal("expected nothing saved or published when the unit of work cannot begin")
	}
}

func TestReceiveChargeForecast_NilUnitOfWorkStillSavesAndPublishes(t *testing.T) {
	s := newScopedSaves()
	pub := &scopedPublisher{}
	uc := usecases.NewReceiveChargeForecast(scopedChargeRepo{s}, pub, s.clock)
	if _, err := uc.Execute(context.Background(), chargeReq(t, s.fixture)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.events) != 1 || pub.inScope[0] || len(s.saves) != 1 || s.saves[0] {
		t.Fatalf("expected save+publish outside any scope, got saves=%v inScope=%v", s.saves, pub.inScope)
	}
}

// --- CommitShiftPlan --------------------------------------------------------

func planReq(t *testing.T) usecases.CommitShiftPlanRequest {
	t.Helper()
	pathId, _ := shared.NewPathId("pick-a")
	rate, _ := shared.NewRate(50)
	heads, _ := shared.NewStationCount(4)
	installed, _ := shared.NewStationCount(5)
	return usecases.CommitShiftPlanRequest{PathId: pathId, PlannedHeads: heads, InstalledStations: installed, Rate: rate, Hours: 8}
}

func TestCommitShiftPlan_SaveAndPublishInsideOneUnitOfWork(t *testing.T) {
	s := newScopedSaves()
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewCommitShiftPlan(scopedPlanRepo{s}, pub, s.clock).WithUnitOfWork(uow)
	if _, err := uc.Execute(context.Background(), planReq(t)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, s.saves, 1)
}

func TestCommitShiftPlan_PublishFailureRollsBack(t *testing.T) {
	s := newScopedSaves()
	uow := &recordingUnitOfWork{}
	uc := usecases.NewCommitShiftPlan(scopedPlanRepo{s}, &scopedPublisher{err: errOutbox}, s.clock).WithUnitOfWork(uow)
	_, err := uc.Execute(context.Background(), planReq(t))
	assertRolledBack(t, err, uow)
}

func TestCommitShiftPlan_InvariantViolationOpensNoScope(t *testing.T) {
	s := newScopedSaves()
	uow := &recordingUnitOfWork{}
	uc := usecases.NewCommitShiftPlan(scopedPlanRepo{s}, &scopedPublisher{}, s.clock).WithUnitOfWork(uow)
	req := planReq(t)
	req.PlannedHeads, _ = shared.NewStationCount(9)
	if _, err := uc.Execute(context.Background(), req); !errors.Is(err, plan.ErrHeadsExceedStations) {
		t.Fatalf("expected invariant error, got %v", err)
	}
	if uow.opened != 0 {
		t.Fatalf("a rejected plan must not open a unit of work, got opened=%d", uow.opened)
	}
}

func TestCommitShiftPlan_NilUnitOfWorkStillWorks(t *testing.T) {
	s := newScopedSaves()
	pub := &scopedPublisher{}
	uc := usecases.NewCommitShiftPlan(scopedPlanRepo{s}, pub, s.clock)
	if _, err := uc.Execute(context.Background(), planReq(t)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.events) != 1 || pub.inScope[0] {
		t.Fatalf("expected one publish outside any scope, got %v", pub.inScope)
	}
}

// --- EnqueueWorkUnit --------------------------------------------------------

func enqueueReq(f fixture, id string) usecases.EnqueueWorkUnitRequest {
	pathId, _ := shared.NewPathId("pick-a")
	return usecases.EnqueueWorkUnitRequest{WorkUnitId: id, PathId: pathId, CPT: shared.NewCPT(f.clock.Now().Add(time.Hour)), Reference: "ref-" + id}
}

func TestEnqueueWorkUnit_BothSavesAndPublishInsideOneUnitOfWork(t *testing.T) {
	s := newScopedSaves()
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewEnqueueWorkUnit(scopedWorkUnitRepo{s}, scopedWorkPoolRepo{s}, pub, s.clock).WithUnitOfWork(uow)
	if _, err := uc.Execute(context.Background(), enqueueReq(s.fixture, "wu-1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, s.saves, 2)
}

func TestEnqueueWorkUnit_PublishFailureRollsBack(t *testing.T) {
	s := newScopedSaves()
	uow := &recordingUnitOfWork{}
	uc := usecases.NewEnqueueWorkUnit(scopedWorkUnitRepo{s}, scopedWorkPoolRepo{s}, &scopedPublisher{err: errOutbox}, s.clock).WithUnitOfWork(uow)
	_, err := uc.Execute(context.Background(), enqueueReq(s.fixture, "wu-1"))
	assertRolledBack(t, err, uow)
}

func TestEnqueueWorkUnit_NilUnitOfWorkStillWorks(t *testing.T) {
	s := newScopedSaves()
	pub := &scopedPublisher{}
	uc := usecases.NewEnqueueWorkUnit(scopedWorkUnitRepo{s}, scopedWorkPoolRepo{s}, pub, s.clock)
	if _, err := uc.Execute(context.Background(), enqueueReq(s.fixture, "wu-1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.events) != 1 || pub.inScope[0] || len(s.saves) != 2 || allTrue(s.saves) {
		t.Fatalf("expected saves+publish outside any scope, got saves=%v inScope=%v", s.saves, pub.inScope)
	}
}

// --- ReleaseNextWork --------------------------------------------------------

func seedEnqueued(t *testing.T, s *scopedSaves, ids ...string) {
	t.Helper()
	enqueue := usecases.NewEnqueueWorkUnit(s.workUnits, s.pools, s.publisher, s.clock)
	for _, id := range ids {
		if _, err := enqueue.Execute(context.Background(), enqueueReq(s.fixture, id)); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
}

func TestReleaseNextWork_BothSavesAndPublishInsideOneUnitOfWork(t *testing.T) {
	s := newScopedSaves()
	seedEnqueued(t, s, "wu-1")
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewReleaseNextWork(scopedWorkPoolRepo{s}, scopedWorkUnitRepo{s}, pub, s.clock).WithUnitOfWork(uow)
	pathId, _ := shared.NewPathId("pick-a")
	if _, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, s.saves, 2)
}

func TestReleaseNextWork_PublishFailureRollsBack(t *testing.T) {
	s := newScopedSaves()
	seedEnqueued(t, s, "wu-1")
	uow := &recordingUnitOfWork{}
	uc := usecases.NewReleaseNextWork(scopedWorkPoolRepo{s}, scopedWorkUnitRepo{s}, &scopedPublisher{err: errOutbox}, s.clock).WithUnitOfWork(uow)
	pathId, _ := shared.NewPathId("pick-a")
	_, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	assertRolledBack(t, err, uow)
}

func TestReleaseNextWork_NothingToReleaseOpensNoScope(t *testing.T) {
	s := newScopedSaves()
	pathId, _ := shared.NewPathId("pick-a")
	if err := s.pools.Save(context.Background(), release.NewWorkPool(pathId, release.ReleaseFed, 10, 10)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewReleaseNextWork(scopedWorkPoolRepo{s}, scopedWorkUnitRepo{s}, &scopedPublisher{}, s.clock).WithUnitOfWork(uow)
	if _, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId}); err == nil {
		t.Fatal("expected an error releasing from an empty pool")
	}
	if uow.opened != 0 {
		t.Fatalf("a rejected release must not open a unit of work, got opened=%d", uow.opened)
	}
}

func TestReleaseNextWork_NilUnitOfWorkStillWorks(t *testing.T) {
	s := newScopedSaves()
	seedEnqueued(t, s, "wu-1")
	pub := &scopedPublisher{}
	uc := usecases.NewReleaseNextWork(scopedWorkPoolRepo{s}, scopedWorkUnitRepo{s}, pub, s.clock)
	pathId, _ := shared.NewPathId("pick-a")
	if _, err := uc.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.events) != 1 || pub.inScope[0] {
		t.Fatalf("expected one publish outside any scope, got %v", pub.inScope)
	}
}

// --- RecordCompletion -------------------------------------------------------

func seedReleased(t *testing.T, s *scopedSaves, id string) {
	t.Helper()
	seedEnqueued(t, s, id)
	pathId, _ := shared.NewPathId("pick-a")
	rel := usecases.NewReleaseNextWork(s.pools, s.workUnits, s.publisher, s.clock)
	if _, err := rel.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("seed release: %v", err)
	}
}

func TestRecordCompletion_BothSavesAndPublishInsideOneUnitOfWork(t *testing.T) {
	s := newScopedSaves()
	seedReleased(t, s, "wu-1")
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewRecordCompletion(scopedWorkUnitRepo{s}, scopedWorkPoolRepo{s}, pub, s.clock).WithUnitOfWork(uow)
	if _, err := uc.Execute(context.Background(), usecases.RecordCompletionRequest{WorkUnitId: "wu-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, s.saves, 2)
}

func TestRecordCompletion_PublishFailureRollsBack(t *testing.T) {
	s := newScopedSaves()
	seedReleased(t, s, "wu-1")
	uow := &recordingUnitOfWork{}
	uc := usecases.NewRecordCompletion(scopedWorkUnitRepo{s}, scopedWorkPoolRepo{s}, &scopedPublisher{err: errOutbox}, s.clock).WithUnitOfWork(uow)
	_, err := uc.Execute(context.Background(), usecases.RecordCompletionRequest{WorkUnitId: "wu-1"})
	assertRolledBack(t, err, uow)
}

func TestRecordCompletion_PoolSaveFailureRollsBack(t *testing.T) {
	s := newScopedSaves()
	seedReleased(t, s, "wu-1")
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewRecordCompletion(scopedWorkUnitRepo{s}, saveErrWorkPoolRepo{WorkPoolRepo: s.pools, err: errors.New("pool save failed")}, pub, s.clock).WithUnitOfWork(uow)
	if _, err := uc.Execute(context.Background(), usecases.RecordCompletionRequest{WorkUnitId: "wu-1"}); err == nil {
		t.Fatal("expected the pool save error to propagate")
	}
	if uow.rolledBack != 1 || len(pub.events) != 0 {
		t.Fatalf("expected rollback with nothing published, got rolledBack=%d published=%d", uow.rolledBack, len(pub.events))
	}
}

func TestRecordCompletion_NilUnitOfWorkStillWorks(t *testing.T) {
	s := newScopedSaves()
	seedReleased(t, s, "wu-1")
	pub := &scopedPublisher{}
	uc := usecases.NewRecordCompletion(scopedWorkUnitRepo{s}, scopedWorkPoolRepo{s}, pub, s.clock)
	if _, err := uc.Execute(context.Background(), usecases.RecordCompletionRequest{WorkUnitId: "wu-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.events) != 1 || pub.inScope[0] {
		t.Fatalf("expected one publish outside any scope, got %v", pub.inScope)
	}
}

// --- SampleBacklog (publishes without saving) ------------------------------

func seedOverThreshold(t *testing.T, s *scopedSaves) shared.PathId {
	t.Helper()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.FlowFed, 0, 1)
	cpt := shared.NewCPT(s.clock.Now().Add(time.Hour))
	for _, id := range []string{"wu-1", "wu-2"} {
		if err := pool.Enqueue(id, cpt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := s.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return pathId
}

func TestSampleBacklog_PublishInsideOneUnitOfWork(t *testing.T) {
	s := newScopedSaves()
	pathId := seedOverThreshold(t, s)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewSampleBacklog(scopedWorkPoolRepo{s}, pub, s.clock).WithUnitOfWork(uow)
	if _, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertOneCommittedScope(t, uow, pub, s.saves, 0)
}

func TestSampleBacklog_PublishFailureRollsBack(t *testing.T) {
	s := newScopedSaves()
	pathId := seedOverThreshold(t, s)
	uow := &recordingUnitOfWork{}
	uc := usecases.NewSampleBacklog(scopedWorkPoolRepo{s}, &scopedPublisher{err: errOutbox}, s.clock).WithUnitOfWork(uow)
	_, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId})
	assertRolledBack(t, err, uow)
}

func TestSampleBacklog_UnderThresholdOpensNoScope(t *testing.T) {
	s := newScopedSaves()
	pathId, _ := shared.NewPathId("pick-a")
	if err := s.pools.Save(context.Background(), release.NewWorkPool(pathId, release.FlowFed, 0, 100)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewSampleBacklog(scopedWorkPoolRepo{s}, &scopedPublisher{}, s.clock).WithUnitOfWork(uow)
	if _, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uow.opened != 0 {
		t.Fatalf("a quiet sample must not open a unit of work, got opened=%d", uow.opened)
	}
}

func TestSampleBacklog_NilUnitOfWorkStillPublishes(t *testing.T) {
	s := newScopedSaves()
	pathId := seedOverThreshold(t, s)
	pub := &scopedPublisher{}
	uc := usecases.NewSampleBacklog(scopedWorkPoolRepo{s}, pub, s.clock)
	if _, err := uc.Execute(context.Background(), usecases.SampleBacklogRequest{PathId: pathId}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.events) != 1 || pub.inScope[0] {
		t.Fatalf("expected one publish outside any scope, got %v", pub.inScope)
	}
}

// --- RebalanceDecision (publishes without saving) --------------------------

func seedSaturated(t *testing.T, s *scopedSaves) shared.PathId {
	t.Helper()
	pathId, _ := shared.NewPathId("pick-a")
	pool := release.NewWorkPool(pathId, release.ReleaseFed, 1, 0)
	cpt := shared.NewCPT(s.clock.Now().Add(time.Hour))
	for _, id := range []string{"wu-1", "wu-2"} {
		if err := pool.Enqueue(id, cpt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if _, err := pool.ReleaseNext(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return pathId
}

func TestRebalanceDecision_ReleaseFedPublishInsideOneUnitOfWork(t *testing.T) {
	s := newScopedSaves()
	pathId := seedSaturated(t, s)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewRebalanceDecision(scopedWorkPoolRepo{s}, pub, s.clock).WithUnitOfWork(uow)
	rec, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Action != usecases.ReassignLabor {
		t.Fatalf("action = %v, want ReassignLabor", rec.Action)
	}
	assertOneCommittedScope(t, uow, pub, s.saves, 0)
	if pub.events[0].EventName() != "LaborReassignmentFlagged" {
		t.Fatalf("published %s, want LaborReassignmentFlagged", pub.events[0].EventName())
	}
}

func TestRebalanceDecision_FlowFedPublishInsideOneUnitOfWork(t *testing.T) {
	s := newScopedSaves()
	pathId := seedOverThreshold(t, s)
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewRebalanceDecision(scopedWorkPoolRepo{s}, pub, s.clock).WithUnitOfWork(uow)
	rec, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Action != usecases.ThrottleUpstream {
		t.Fatalf("action = %v, want ThrottleUpstream", rec.Action)
	}
	assertOneCommittedScope(t, uow, pub, s.saves, 0)
	if pub.events[0].EventName() != "PathThrottled" {
		t.Fatalf("published %s, want PathThrottled", pub.events[0].EventName())
	}
}

func TestRebalanceDecision_PublishFailureRollsBack(t *testing.T) {
	s := newScopedSaves()
	pathId := seedSaturated(t, s)
	uow := &recordingUnitOfWork{}
	uc := usecases.NewRebalanceDecision(scopedWorkPoolRepo{s}, &scopedPublisher{err: errOutbox}, s.clock).WithUnitOfWork(uow)
	_, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId})
	assertRolledBack(t, err, uow)
}

func TestRebalanceDecision_NoActionOpensNoScope(t *testing.T) {
	s := newScopedSaves()
	pathId, _ := shared.NewPathId("pick-a")
	if err := s.pools.Save(context.Background(), release.NewWorkPool(pathId, release.ReleaseFed, 10, 10)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	uow := &recordingUnitOfWork{}
	uc := usecases.NewRebalanceDecision(scopedWorkPoolRepo{s}, &scopedPublisher{}, s.clock).WithUnitOfWork(uow)
	rec, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId})
	if err != nil || rec.Action != usecases.NoActionNeeded {
		t.Fatalf("expected NoActionNeeded, got %v err=%v", rec.Action, err)
	}
	if uow.opened != 0 {
		t.Fatalf("no action must not open a unit of work, got opened=%d", uow.opened)
	}
}

func TestRebalanceDecision_NilUnitOfWorkStillPublishes(t *testing.T) {
	s := newScopedSaves()
	pathId := seedSaturated(t, s)
	pub := &scopedPublisher{}
	uc := usecases.NewRebalanceDecision(scopedWorkPoolRepo{s}, pub, s.clock)
	if _, err := uc.Execute(context.Background(), usecases.RebalanceDecisionRequest{PathId: pathId}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.events) != 1 || pub.inScope[0] {
		t.Fatalf("expected one publish outside any scope, got %v", pub.inScope)
	}
}
