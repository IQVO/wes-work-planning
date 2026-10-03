package usecases_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/inventoryview"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// These tests pin ADR-0028: an inbound integration event's processed-event
// mark commits if and only if its effect commits, so a failed first attempt
// is retried rather than swallowed as "already processed".

var errTransient = errors.New("transient postgres blip")

// txKey marks a ctx as running inside fakeUoW's scope.
type txKey struct{}

// fakeUoW simulates a real transactional UnitOfWork for a stagingProcessed
// repo: marks made inside the scope are staged and only become visible if
// fn returns nil; on error they are discarded (rolled back).
type fakeUoW struct {
	processed *stagingProcessed
}

func (u fakeUoW) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if ctx.Value(txKey{}) != nil {
		return fn(ctx)
	}
	staged := map[string]struct{}{}
	if err := fn(context.WithValue(ctx, txKey{}, staged)); err != nil {
		return err
	}
	u.processed.commit(staged)
	return nil
}

// stagingProcessed is a ProcessedEventRepo WITHOUT ReleaseProcessed: under
// fakeUoW the only way a mark disappears is the scope rolling back, which is
// exactly the property the Postgres adapter has.
type stagingProcessed struct {
	mu        sync.Mutex
	committed map[string]struct{}
}

func newStagingProcessed() *stagingProcessed {
	return &stagingProcessed{committed: map[string]struct{}{}}
}

func (p *stagingProcessed) TryMarkProcessed(ctx context.Context, eventId string, _ time.Time) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.committed[eventId]; ok {
		return true, nil
	}
	staged, ok := ctx.Value(txKey{}).(map[string]struct{})
	if !ok {
		p.committed[eventId] = struct{}{}
		return false, nil
	}
	if _, ok := staged[eventId]; ok {
		return true, nil
	}
	staged[eventId] = struct{}{}
	return false, nil
}

func (p *stagingProcessed) commit(staged map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id := range staged {
		p.committed[id] = struct{}{}
	}
}

func (p *stagingProcessed) isCommitted(eventId string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.committed[eventId]
	return ok
}

// flakyWorkUnits fails the first `failures` FindById calls, then delegates.
// The failure is injected on the read (not on Save) because the in-memory
// repo hands out the stored aggregate pointer: a Save failure there would
// leave the unit already mutated to Completed with no rollback, which is an
// artefact of the test double, not of the behaviour under test.
type flakyWorkUnits struct {
	ports.WorkUnitRepo
	mu       sync.Mutex
	failures int
}

func (r *flakyWorkUnits) FindById(ctx context.Context, id string) (*workunit.WorkUnit, error) {
	r.mu.Lock()
	if r.failures > 0 {
		r.failures--
		r.mu.Unlock()
		return nil, errTransient
	}
	r.mu.Unlock()
	return r.WorkUnitRepo.FindById(ctx, id)
}

// flakyPools fails the first `failures` Saves, then delegates.
type flakyPools struct {
	ports.WorkPoolRepo
	mu       sync.Mutex
	failures int
}

func (r *flakyPools) Save(ctx context.Context, pool *release.WorkPool) error {
	r.mu.Lock()
	if r.failures > 0 {
		r.failures--
		r.mu.Unlock()
		return errTransient
	}
	r.mu.Unlock()
	return r.WorkPoolRepo.Save(ctx, pool)
}

// processedVariant is the two wirings every test runs under: in-memory with
// no UnitOfWork (the mark is undone through ProcessedEventReleaser) and a
// transactional UnitOfWork (the mark rolls back with the scope).
type processedVariant struct {
	name      string
	processed ports.ProcessedEventRepo
	uow       ports.UnitOfWork
}

func processedVariants() []func() processedVariant {
	return []func() processedVariant{
		func() processedVariant {
			return processedVariant{name: "no unit of work", processed: memory.NewProcessedEventRepo()}
		},
		func() processedVariant {
			p := newStagingProcessed()
			return processedVariant{name: "transactional unit of work", processed: p, uow: fakeUoW{processed: p}}
		},
	}
}

type completionFixture struct {
	workUnits *memory.WorkUnitRepo
	flaky     *flakyWorkUnits
	uc        *usecases.ApplyTaskCompleted
}

func newCompletionFixture(t *testing.T, v processedVariant, failures int) completionFixture {
	t.Helper()
	ctx := context.Background()
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	pathId, _ := shared.NewPathId("pick-a")

	if _, err := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock).Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: shared.NewCPT(clock.At.Add(2 * time.Hour)), Reference: "order-1",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock).Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("release: %v", err)
	}

	flaky := &flakyWorkUnits{WorkUnitRepo: workUnits, failures: failures}
	record := usecases.NewRecordCompletion(flaky, pools, publisher, clock).WithUnitOfWork(v.uow)
	return completionFixture{
		workUnits: workUnits,
		flaky:     flaky,
		uc:        usecases.NewApplyTaskCompleted(record, v.processed).WithUnitOfWork(v.uow),
	}
}

func completionRequest(eventId, workUnitId string) usecases.ApplyTaskCompletedRequest {
	return usecases.ApplyTaskCompletedRequest{EventId: eventId, WorkUnitId: workUnitId, OccurredAt: time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)}
}

// The bug this ADR fixes: attempt 1 marked the event processed and then
// failed; attempt 2 saw the mark and returned success without completing
// anything. Now attempt 1 leaves no mark, so attempt 2 really completes.
func TestApplyTaskCompleted_FailedAttemptIsRetriedNotSwallowed(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			f := newCompletionFixture(t, v, 1)
			ctx := context.Background()

			if _, err := f.uc.Execute(ctx, completionRequest("evt-1", "wu-1")); !errors.Is(err, errTransient) {
				t.Fatalf("attempt 1: got %v, want the transient error", err)
			}
			outcome, err := f.uc.Execute(ctx, completionRequest("evt-1", "wu-1"))
			if err != nil {
				t.Fatalf("attempt 2: %v", err)
			}
			if outcome != usecases.TaskCompletedApplied {
				t.Fatalf("attempt 2 outcome = %v, want TaskCompletedApplied (a swallowed retry reports AlreadyProcessed)", outcome)
			}
			unit, _ := f.workUnits.FindById(ctx, "wu-1")
			if unit.State() != workunit.Completed {
				t.Fatalf("work unit state = %v, want Completed", unit.State())
			}

			// Only now is the event marked: a redelivery is a cheap no-op.
			outcome, err = f.uc.Execute(ctx, completionRequest("evt-1", "wu-1"))
			if err != nil || outcome != usecases.TaskCompletedAlreadyProcessed {
				t.Fatalf("redelivery = (%v, %v), want (TaskCompletedAlreadyProcessed, nil)", outcome, err)
			}
		})
	}
}

func TestApplyTaskCompleted_UnknownWorkUnitIsAProcessedSkip(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			f := newCompletionFixture(t, v, 0)
			ctx := context.Background()

			outcome, err := f.uc.Execute(ctx, completionRequest("evt-pack", "order-1"))
			if err != nil {
				t.Fatalf("unknown work unit must not be an error, got %v", err)
			}
			if outcome != usecases.TaskCompletedUnknownWorkUnit {
				t.Fatalf("outcome = %v, want TaskCompletedUnknownWorkUnit", outcome)
			}
			outcome, err = f.uc.Execute(ctx, completionRequest("evt-pack", "order-1"))
			if err != nil || outcome != usecases.TaskCompletedAlreadyProcessed {
				t.Fatalf("redelivery = (%v, %v), want the skip to have been marked processed", outcome, err)
			}
		})
	}
}

// A domain error other than ErrNotFound (here: double-complete under a new
// event id) is NOT a skip: it propagates so the consumer retries/DLQs it,
// and the event stays unmarked.
func TestApplyTaskCompleted_OtherErrorsPropagateAndLeaveNoMark(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			f := newCompletionFixture(t, v, 0)
			ctx := context.Background()
			if _, err := f.uc.Execute(ctx, completionRequest("evt-a", "wu-1")); err != nil {
				t.Fatalf("first completion: %v", err)
			}
			for attempt := 1; attempt <= 2; attempt++ {
				_, err := f.uc.Execute(ctx, completionRequest("evt-b", "wu-1"))
				if !errors.Is(err, workunit.ErrAlreadyCompleted) {
					t.Fatalf("attempt %d: got %v, want ErrAlreadyCompleted every time (a mark left behind would turn attempt 2 into a silent success)", attempt, err)
				}
			}
		})
	}
}

func TestApplyTaskCompleted_TryMarkProcessedErrorPropagates(t *testing.T) {
	f := newCompletionFixture(t, processedVariant{processed: memory.NewProcessedEventRepo()}, 0)
	wantErr := errors.New("processed-event-store unavailable")
	uc := usecases.NewApplyTaskCompleted(
		usecases.NewRecordCompletion(f.workUnits, memory.NewWorkPoolRepo(), events.NewLogPublisher(nil), memory.SystemClock{}),
		erroringProcessedEventRepo{err: wantErr})
	if _, err := uc.Execute(context.Background(), completionRequest("evt-1", "wu-1")); !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
}

type orderFixture struct {
	workUnits *memory.WorkUnitRepo
	pools     *memory.WorkPoolRepo
	uc        *usecases.ApplyOrderAllocated
}

func orderCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}}})
}

func newOrderFixture(v processedVariant, poolFailures int) orderFixture {
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	flaky := &flakyPools{WorkPoolRepo: pools, failures: poolFailures}
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, flaky, events.NewLogPublisher(nil), clock).WithUnitOfWork(v.uow)
	return orderFixture{
		workUnits: workUnits,
		pools:     pools,
		uc:        usecases.NewApplyOrderAllocated(enqueue, v.processed, orderCatalogue()).WithUnitOfWork(v.uow),
	}
}

func orderRequest(eventId string, lines ...usecases.OrderAllocatedLine) usecases.ApplyOrderAllocatedRequest {
	return usecases.ApplyOrderAllocatedRequest{
		EventId:     eventId,
		OccurredAt:  time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC),
		OrderId:     "order-1",
		PromiseDate: time.Date(2026, 8, 21, 23, 0, 0, 0, time.UTC),
		Lines:       lines,
	}
}

func TestApplyOrderAllocated_FailedAttemptIsRetriedNotSwallowed(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			f := newOrderFixture(v, 1)
			ctx := context.Background()
			req := orderRequest("evt-order", usecases.OrderAllocatedLine{LineNo: 1, SKU: "SKU-1", PathId: "pick-a", GiftWrap: true})

			if _, err := f.uc.Execute(ctx, req); !errors.Is(err, errTransient) {
				t.Fatalf("attempt 1: got %v, want the transient error", err)
			}
			already, err := f.uc.Execute(ctx, req)
			if err != nil || already {
				t.Fatalf("attempt 2 = (already=%v, %v), want a real apply", already, err)
			}
			unit, err := f.workUnits.FindById(ctx, "order-1-line-1")
			if err != nil {
				t.Fatalf("order line was never enqueued: %v", err)
			}
			if unit.SKU() != "SKU-1" || !unit.GiftWrap() || unit.Reference() != "order-1" {
				t.Fatalf("unexpected unit: sku=%q giftWrap=%v ref=%q", unit.SKU(), unit.GiftWrap(), unit.Reference())
			}
			already, err = f.uc.Execute(ctx, req)
			if err != nil || !already {
				t.Fatalf("redelivery = (already=%v, %v), want (true, nil)", already, err)
			}
		})
	}
}

func TestApplyOrderAllocated_UnknownPathFailsLoudAndLeavesNoMark(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			f := newOrderFixture(v, 0)
			ctx := context.Background()
			req := orderRequest("evt-bad",
				usecases.OrderAllocatedLine{LineNo: 1, SKU: "SKU-1", PathId: "pick-a"},
				usecases.OrderAllocatedLine{LineNo: 2, SKU: "SKU-2", PathId: "not-a-real-path"})
			for attempt := 1; attempt <= 2; attempt++ {
				if _, err := f.uc.Execute(ctx, req); !errors.Is(err, pathcatalog.ErrUnknownPath) {
					t.Fatalf("attempt %d: got %v, want ErrUnknownPath", attempt, err)
				}
			}
			if _, err := f.workUnits.FindById(ctx, "order-1-line-1"); !errors.Is(err, ports.ErrNotFound) {
				t.Fatalf("no line may be enqueued when any line's path is unknown, got %v", err)
			}
		})
	}
}

func TestApplyOrderAllocated_InvalidPathIdFails(t *testing.T) {
	f := newOrderFixture(processedVariant{processed: memory.NewProcessedEventRepo()}, 0)
	if _, err := f.uc.Execute(context.Background(), orderRequest("evt-empty", usecases.OrderAllocatedLine{LineNo: 1, PathId: ""})); err == nil {
		t.Fatal("an empty path_id must fail")
	}
}

// A line whose deterministic WorkUnitId is already in the pool (same order
// line re-sent under a new event id) is a benign no-op, not a failure.
func TestApplyOrderAllocated_DuplicateLineUnderNewEventIdIsBenign(t *testing.T) {
	f := newOrderFixture(processedVariant{processed: memory.NewProcessedEventRepo()}, 0)
	ctx := context.Background()
	line := usecases.OrderAllocatedLine{LineNo: 1, SKU: "SKU-1", PathId: "pick-a"}
	if _, err := f.uc.Execute(ctx, orderRequest("evt-1", line)); err != nil {
		t.Fatalf("first: %v", err)
	}
	already, err := f.uc.Execute(ctx, orderRequest("evt-2", line, usecases.OrderAllocatedLine{LineNo: 2, SKU: "SKU-2", PathId: "pick-a"}))
	if err != nil || already {
		t.Fatalf("second = (already=%v, %v), want a real apply", already, err)
	}
	pool, err := f.pools.FindByPathId(ctx, mustPath(t, "pick-a"))
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	if got := pool.BacklogDepth(); got != 2 {
		t.Fatalf("backlog depth = %d, want 2 (line 1 once, line 2 once)", got)
	}
}

func mustPath(t *testing.T, v string) shared.PathId {
	t.Helper()
	p, err := shared.NewPathId(v)
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	return p
}

// flakyLaborViews fails the first Save.
type flakyLaborViews struct {
	*memory.LaborPlanViewRepo
	failures int
}

func (r *flakyLaborViews) Save(ctx context.Context, view laborview.LaborPlanObserved) error {
	if r.failures > 0 {
		r.failures--
		return errTransient
	}
	return r.LaborPlanViewRepo.Save(ctx, view)
}

// ObserveLaborPlan had the same mark-first shape; pin the fix there too.
func TestObserveLaborPlan_FailedSaveIsRetriedNotSwallowed(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			views := &flakyLaborViews{LaborPlanViewRepo: memory.NewLaborPlanViewRepo(), failures: 1}
			uc := usecases.NewObserveLaborPlan(views, v.processed).WithUnitOfWork(v.uow)
			req := usecases.ObserveLaborPlanRequest{EventId: "evt-1", PathId: mustPath(t, "pick-a"), PlannedHeads: 5, ObservedAt: time.Now()}
			ctx := context.Background()

			if err := uc.Execute(ctx, req); !errors.Is(err, errTransient) {
				t.Fatalf("attempt 1: got %v", err)
			}
			if err := uc.Execute(ctx, req); err != nil {
				t.Fatalf("attempt 2: %v", err)
			}
			view, err := views.FindByPathId(ctx, req.PathId)
			if err != nil || view.PlannedHeads != 5 {
				t.Fatalf("labor plan view = (%+v, %v), want PlannedHeads 5", view, err)
			}
		})
	}
}

// flakyInventoryViews fails the first ApplyDelta.
type flakyInventoryViews struct {
	*memory.InventoryViewRepo
	failures int
}

func (r *flakyInventoryViews) ApplyDelta(ctx context.Context, sku string, delta int, at time.Time) (inventoryview.UsableInventoryObserved, error) {
	if r.failures > 0 {
		r.failures--
		return inventoryview.UsableInventoryObserved{}, errTransient
	}
	return r.InventoryViewRepo.ApplyDelta(ctx, sku, delta, at)
}

func TestObserveInventoryChange_FailedDeltaIsRetriedNotSwallowed(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			views := &flakyInventoryViews{InventoryViewRepo: memory.NewInventoryViewRepo(), failures: 1}
			uc := usecases.NewObserveInventoryChange(views, v.processed).WithUnitOfWork(v.uow)
			req := usecases.ObserveInventoryChangeRequest{EventId: "evt-1", SKU: "sku-1", Quantity: 4, Delta: -4, ObservedAt: time.Now()}
			ctx := context.Background()

			if _, err := uc.Execute(ctx, req); !errors.Is(err, errTransient) {
				t.Fatalf("attempt 1: got %v", err)
			}
			got, err := uc.Execute(ctx, req)
			if err != nil || got.UsableQuantity != -4 {
				t.Fatalf("attempt 2 = (%+v, %v), want usable -4", got, err)
			}
			got, err = uc.Execute(ctx, req)
			if err != nil || got.UsableQuantity != -4 {
				t.Fatalf("redelivery = (%+v, %v), want usable still -4 (not double-applied)", got, err)
			}
		})
	}
}

// releasingFailsProcessed marks normally but cannot release.
type releasingFailsProcessed struct {
	*memory.ProcessedEventRepo
	err error
}

func (r releasingFailsProcessed) ReleaseProcessed(context.Context, string) error { return r.err }

func TestInboundEvent_ReleaseFailureIsJoinedOntoTheCause(t *testing.T) {
	releaseErr := errors.New("release failed")
	views := &flakyLaborViews{LaborPlanViewRepo: memory.NewLaborPlanViewRepo(), failures: 1}
	uc := usecases.NewObserveLaborPlan(views, releasingFailsProcessed{ProcessedEventRepo: memory.NewProcessedEventRepo(), err: releaseErr})
	err := uc.Execute(context.Background(), usecases.ObserveLaborPlanRequest{EventId: "evt-1", PathId: mustPath(t, "pick-a"), ObservedAt: time.Now()})
	if !errors.Is(err, errTransient) || !errors.Is(err, releaseErr) {
		t.Fatalf("got %v, want both the cause and the release failure", err)
	}
}

// markOnlyProcessed has no ReleaseProcessed: without a UnitOfWork the mark
// cannot be undone, and the cause is still returned unchanged.
type markOnlyProcessed struct{ inner *memory.ProcessedEventRepo }

func (r markOnlyProcessed) TryMarkProcessed(ctx context.Context, id string, at time.Time) (bool, error) {
	return r.inner.TryMarkProcessed(ctx, id, at)
}

func TestInboundEvent_NonReleasableRepoReturnsTheCause(t *testing.T) {
	views := &flakyLaborViews{LaborPlanViewRepo: memory.NewLaborPlanViewRepo(), failures: 1}
	uc := usecases.NewObserveLaborPlan(views, markOnlyProcessed{inner: memory.NewProcessedEventRepo()})
	err := uc.Execute(context.Background(), usecases.ObserveLaborPlanRequest{EventId: "evt-1", PathId: mustPath(t, "pick-a"), ObservedAt: time.Now()})
	if !errors.Is(err, errTransient) {
		t.Fatalf("got %v, want the cause", err)
	}
}

// Under a transactional UnitOfWork the failed attempt's mark must never
// reach the committed set — that is the Postgres rollback property.
func TestApplyTaskCompleted_FailedAttemptNeverCommitsTheMark(t *testing.T) {
	p := newStagingProcessed()
	v := processedVariant{processed: p, uow: fakeUoW{processed: p}}
	f := newCompletionFixture(t, v, 1)
	if _, err := f.uc.Execute(context.Background(), completionRequest("evt-1", "wu-1")); err == nil {
		t.Fatal("attempt 1 should fail")
	}
	if p.isCommitted("evt-1") {
		t.Fatal("the processed mark was committed although the completion rolled back")
	}
}
