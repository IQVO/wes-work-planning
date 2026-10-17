package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// These tests pin ADR-0042's ApplyReplenishmentRequested and
// ApplyReplenishmentCancelled: one REPLENISH work unit per request id,
// URGENT in the front band, no-ops for every replay, and a cancellation that
// does for a Pending unit what ADR-0040 does for a superseded RELOCATE move.

var replRequestedAt = time.Date(2026, 10, 17, 9, 30, 0, 0, time.UTC)

type replFixture struct {
	workUnits *memory.WorkUnitRepo
	pools     *memory.WorkPoolRepo
	requested *usecases.ApplyReplenishmentRequested
	cancelled *usecases.ApplyReplenishmentCancelled
	release   *usecases.ReleaseNextWork
	pathId    shared.PathId
}

func replCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
		{Id: "REPLENISH", MatchPrefix: "replenish", RequiredCapabilities: []string{"replenish"}},
	})
}

func newReplFixture(t *testing.T, v processedVariant, catalogue ports.PathCatalogue, poolFailures int) replFixture {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	flaky := &flakyPools{WorkPoolRepo: pools, failures: poolFailures}
	clock := memory.FixedClock{At: replRequestedAt.Add(time.Minute)}
	publisher := events.NewLogPublisher(nil)
	return replFixture{
		workUnits: workUnits,
		pools:     pools,
		requested: usecases.NewApplyReplenishmentRequested(workUnits, flaky, v.processed, catalogue, publisher, clock).WithUnitOfWork(v.uow),
		cancelled: usecases.NewApplyReplenishmentCancelled(workUnits, flaky, v.processed, clock).WithUnitOfWork(v.uow),
		release:   usecases.NewReleaseNextWork(pools, workUnits, publisher, clock),
		pathId:    mustPath(t, "REPLENISH"),
	}
}

func newPlainReplFixture(t *testing.T) replFixture {
	t.Helper()
	return newReplFixture(t, processedVariant{processed: memory.NewProcessedEventRepo()}, replCatalogue(), 0)
}

func replRequest(eventId, requestId, priority string) usecases.ApplyReplenishmentRequestedRequest {
	return usecases.ApplyReplenishmentRequestedRequest{
		EventId: eventId, OccurredAt: replRequestedAt, RequestId: requestId, SiteId: "WH1", SKU: "SKU-1",
		FromBin: "WH1-RSV-A07-03-02-B", ToBin: "WH1-FWD-A01-01-01-A", Quantity: 24, Priority: priority, RequestedAt: replRequestedAt,
	}
}

func replCancel(eventId, requestId string) usecases.ApplyReplenishmentCancelledRequest {
	return usecases.ApplyReplenishmentCancelledRequest{EventId: eventId, OccurredAt: replRequestedAt, RequestId: requestId, Reason: "ASSIGNMENT_CHANGED"}
}

func (f replFixture) mustPlan(t *testing.T, req usecases.ApplyReplenishmentRequestedRequest) usecases.ReplenishmentOutcome {
	t.Helper()
	outcome, err := f.requested.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("ApplyReplenishmentRequested: %v", err)
	}
	return outcome
}

func (f replFixture) backlog(t *testing.T) int {
	t.Helper()
	pool, err := f.pools.FindByPathId(context.Background(), f.pathId)
	if err != nil {
		t.Fatalf("REPLENISH pool: %v", err)
	}
	return pool.BacklogDepth()
}

func (f replFixture) unit(t *testing.T, requestId string) *workunit.WorkUnit {
	t.Helper()
	u, err := f.workUnits.FindById(context.Background(), workunit.ReplenishmentWorkUnitId(requestId))
	if err != nil {
		t.Fatalf("work unit of %s: %v", requestId, err)
	}
	return u
}

func TestApplyReplenishmentRequested_PlansOneReplenishUnitPerRequest(t *testing.T) {
	f := newPlainReplFixture(t)
	if got := f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL")); got != usecases.ReplenishmentPlanned {
		t.Fatalf("outcome = %v, want planned", got)
	}
	u := f.unit(t, "repl-1")
	assertReplenishMove(t, u)
	assertReplenishIdentity(t, u)
	if f.backlog(t) != 1 {
		t.Fatalf("backlog = %d, want 1", f.backlog(t))
	}
}

func assertReplenishMove(t *testing.T, u *workunit.WorkUnit) {
	t.Helper()
	move, ok := u.Replenishment()
	if !ok || move.RequestId != "repl-1" || move.FromBin != "WH1-RSV-A07-03-02-B" || move.ToBin != "WH1-FWD-A01-01-01-A" || move.Quantity != 24 || move.Priority != workunit.ReplenishmentPriorityNormal {
		t.Fatalf("replenishment = (%+v, %v)", move, ok)
	}
}

func assertReplenishIdentity(t *testing.T, u *workunit.WorkUnit) {
	t.Helper()
	if u.Id() != "replenish-repl-1" || u.WorkKind() != workunit.WorkKindReplenish || u.PathId().String() != "REPLENISH" || u.Reference() != "repl-1" {
		t.Fatalf("unit = id %s kind %s path %s ref %s", u.Id(), u.WorkKind(), u.PathId(), u.Reference())
	}
	if u.SiteId() != "WH1" || u.SKU() != "SKU-1" || u.Quantity() != 24 || u.State() != workunit.Pending {
		t.Fatalf("unit site=%s sku=%s qty=%d state=%v", u.SiteId(), u.SKU(), u.Quantity(), u.State())
	}
}

func TestApplyReplenishmentRequested_CPTFollowsPriority(t *testing.T) {
	f := newPlainReplFixture(t)
	f.mustPlan(t, replRequest("evt-n", "repl-n", "NORMAL"))
	f.mustPlan(t, replRequest("evt-u", "repl-u", "URGENT"))
	if got, want := f.unit(t, "repl-n").CPT().Time(), replRequestedAt.Add(usecases.DefaultReplenishmentCPTHorizon); !got.Equal(want) {
		t.Fatalf("NORMAL cpt = %v, want %v", got, want)
	}
	if got, want := f.unit(t, "repl-u").CPT().Time(), replRequestedAt.Add(usecases.DefaultReplenishmentUrgentCPTHorizon); !got.Equal(want) {
		t.Fatalf("URGENT cpt = %v, want %v", got, want)
	}
}

// URGENT sits in the front band: planned AFTER a NORMAL request, it is
// released FIRST.
func TestApplyReplenishmentRequested_UrgentIsReleasedAheadOfNormal(t *testing.T) {
	f := newPlainReplFixture(t)
	f.mustPlan(t, replRequest("evt-n", "repl-n", "NORMAL"))
	f.mustPlan(t, replRequest("evt-u", "repl-u", "URGENT"))
	first, err := f.release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: f.pathId})
	if err != nil || first.Id() != "replenish-repl-u" {
		t.Fatalf("first release = (%v, %v), want the URGENT request", first, err)
	}
}

func TestApplyReplenishmentRequested_CPTHorizonsAreConfigurable(t *testing.T) {
	f := newPlainReplFixture(t)
	f.requested.WithCPTHorizons(4*time.Hour, 30*time.Minute)
	f.mustPlan(t, replRequest("evt-n", "repl-n", "NORMAL"))
	f.mustPlan(t, replRequest("evt-u", "repl-u", "URGENT"))
	if got, want := f.unit(t, "repl-n").CPT().Time(), replRequestedAt.Add(4*time.Hour); !got.Equal(want) {
		t.Fatalf("NORMAL cpt = %v, want %v", got, want)
	}
	if got, want := f.unit(t, "repl-u").CPT().Time(), replRequestedAt.Add(30*time.Minute); !got.Equal(want) {
		t.Fatalf("URGENT cpt = %v, want %v", got, want)
	}
}

func TestApplyReplenishmentRequested_NonPositiveHorizonsKeepTheCurrentOnes(t *testing.T) {
	f := newPlainReplFixture(t)
	f.requested.WithCPTHorizons(0, -time.Hour)
	f.mustPlan(t, replRequest("evt-n", "repl-n", "NORMAL"))
	f.mustPlan(t, replRequest("evt-u", "repl-u", "URGENT"))
	if got := f.unit(t, "repl-n").CPT().Time(); !got.Equal(replRequestedAt.Add(usecases.DefaultReplenishmentCPTHorizon)) {
		t.Fatalf("NORMAL cpt = %v", got)
	}
	if got := f.unit(t, "repl-u").CPT().Time(); !got.Equal(replRequestedAt.Add(usecases.DefaultReplenishmentUrgentCPTHorizon)) {
		t.Fatalf("URGENT cpt = %v", got)
	}
}

// An urgent horizon above the normal one would sort URGENT behind NORMAL:
// it is capped at the normal horizon.
func TestApplyReplenishmentRequested_UrgentHorizonIsCappedAtTheNormalOne(t *testing.T) {
	f := newPlainReplFixture(t)
	f.requested.WithCPTHorizons(2*time.Hour, 10*time.Hour)
	f.mustPlan(t, replRequest("evt-u", "repl-u", "URGENT"))
	if got := f.unit(t, "repl-u").CPT().Time(); !got.Equal(replRequestedAt.Add(2 * time.Hour)) {
		t.Fatalf("URGENT cpt = %v, want capped at requested_at + 2h", got)
	}
}

func TestApplyReplenishmentRequested_RedeliveryOfTheSameEventIsAProcessedNoOp(t *testing.T) {
	f := newPlainReplFixture(t)
	f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL"))
	if got := f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL")); got != usecases.ReplenishmentAlreadyProcessed {
		t.Fatalf("outcome = %v, want already processed", got)
	}
	if f.backlog(t) != 1 {
		t.Fatalf("backlog = %d, want 1", f.backlog(t))
	}
}

// A second DIFFERENT event for an already-planned request id (a re-sent
// request) must never become a second unit, even with other field values.
func TestApplyReplenishmentRequested_SameRequestUnderAnotherEventIdIsANoOp(t *testing.T) {
	f := newPlainReplFixture(t)
	f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL"))
	other := replRequest("evt-2", "repl-1", "URGENT")
	other.Quantity = 99
	if got := f.mustPlan(t, other); got != usecases.ReplenishmentAlreadyPlanned {
		t.Fatalf("outcome = %v, want already planned", got)
	}
	u := f.unit(t, "repl-1")
	if move, _ := u.Replenishment(); move.Quantity != 24 || move.Priority != workunit.ReplenishmentPriorityNormal {
		t.Fatalf("the first plan was overwritten: %+v", move)
	}
	if f.backlog(t) != 1 {
		t.Fatalf("backlog = %d, want exactly one unit", f.backlog(t))
	}
}

// Once released, the pool entry is no longer Pending, but the unit still
// exists: a re-sent request stays a no-op.
func TestApplyReplenishmentRequested_SameRequestAfterReleaseIsStillANoOp(t *testing.T) {
	f := newPlainReplFixture(t)
	f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL"))
	if _, err := f.release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: f.pathId}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := f.mustPlan(t, replRequest("evt-2", "repl-1", "NORMAL")); got != usecases.ReplenishmentAlreadyPlanned {
		t.Fatalf("outcome = %v, want already planned", got)
	}
	if f.unit(t, "repl-1").State() != workunit.Released {
		t.Fatal("the released unit was touched")
	}
}

func TestApplyReplenishmentRequested_RejectsABrokenRequestAndPlansNothing(t *testing.T) {
	mutations := map[string]func(*usecases.ApplyReplenishmentRequestedRequest){
		"no request id":     func(r *usecases.ApplyReplenishmentRequestedRequest) { r.RequestId = "" },
		"no site":           func(r *usecases.ApplyReplenishmentRequestedRequest) { r.SiteId = "  " },
		"no sku":            func(r *usecases.ApplyReplenishmentRequestedRequest) { r.SKU = "" },
		"no source bin":     func(r *usecases.ApplyReplenishmentRequestedRequest) { r.FromBin = "" },
		"no forward bin":    func(r *usecases.ApplyReplenishmentRequestedRequest) { r.ToBin = "" },
		"same bins":         func(r *usecases.ApplyReplenishmentRequestedRequest) { r.ToBin = r.FromBin },
		"zero quantity":     func(r *usecases.ApplyReplenishmentRequestedRequest) { r.Quantity = 0 },
		"unknown priority":  func(r *usecases.ApplyReplenishmentRequestedRequest) { r.Priority = "HIGH" },
		"no requested time": func(r *usecases.ApplyReplenishmentRequestedRequest) { r.RequestedAt = time.Time{} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			for _, newVariant := range processedVariants() {
				v := newVariant()
				f := newReplFixture(t, v, replCatalogue(), 0)
				req := replRequest("evt-bad", "repl-1", "NORMAL")
				mutate(&req)
				for attempt := 1; attempt <= 2; attempt++ {
					_, err := f.requested.Execute(context.Background(), req)
					if !errors.Is(err, workunit.ErrInvalidReplenishmentMove) && !errors.Is(err, workunit.ErrUnknownReplenishmentPriority) && !errors.Is(err, usecases.ErrInvalidReplenishmentRequest) {
						t.Fatalf("%s attempt %d: error = %v, want a deterministic validation error every time (a left-behind mark would turn attempt 2 into a silent success)", v.name, attempt, err)
					}
				}
				if _, err := f.workUnits.FindById(context.Background(), "replenish-repl-1"); !errors.Is(err, ports.ErrNotFound) {
					t.Fatalf("%s: a broken request planned a unit: %v", v.name, err)
				}
			}
		})
	}
}

func TestApplyReplenishmentRequested_UnknownReplenishPathFailsLoud(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			noReplenish := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "PICK", MatchPrefix: "pick"}})
			f := newReplFixture(t, v, noReplenish, 0)
			for attempt := 1; attempt <= 2; attempt++ {
				if _, err := f.requested.Execute(context.Background(), replRequest("evt-1", "repl-1", "NORMAL")); !errors.Is(err, pathcatalog.ErrUnknownPath) {
					t.Fatalf("attempt %d: error = %v, want ErrUnknownPath", attempt, err)
				}
			}
		})
	}
}

// REPLENISH is reached by explicit id: an intake the catalogue declares for
// other demand sources does not refuse it (no replenishment intake value
// exists yet in process-path-management).
func TestApplyReplenishmentRequested_DeclaredIntakeDoesNotRefuseTheExplicitPath(t *testing.T) {
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "REPLENISH", MatchPrefix: "replenish", Intake: []string{"STOCK_TRANSFER"}}})
	f := newReplFixture(t, processedVariant{processed: memory.NewProcessedEventRepo()}, catalogue, 0)
	if got := f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL")); got != usecases.ReplenishmentPlanned {
		t.Fatalf("outcome = %v", got)
	}
}

func TestApplyReplenishmentRequested_FailedAttemptIsRetriedNotSwallowed(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			f := newReplFixture(t, v, replCatalogue(), 1)
			ctx := context.Background()
			if _, err := f.requested.Execute(ctx, replRequest("evt-1", "repl-1", "NORMAL")); !errors.Is(err, errTransient) {
				t.Fatalf("attempt 1: error = %v, want the transient error", err)
			}
			outcome, err := f.requested.Execute(ctx, replRequest("evt-1", "repl-1", "NORMAL"))
			if err != nil || outcome != usecases.ReplenishmentPlanned {
				t.Fatalf("attempt 2 = (%v, %v), want a real plan (a swallowed retry reports already processed)", outcome, err)
			}
			if f.unit(t, "repl-1").State() != workunit.Pending {
				t.Fatal("the unit was not planned by the retry")
			}
			outcome, err = f.requested.Execute(ctx, replRequest("evt-1", "repl-1", "NORMAL"))
			if err != nil || outcome != usecases.ReplenishmentAlreadyProcessed {
				t.Fatalf("redelivery = (%v, %v), want already processed", outcome, err)
			}
		})
	}
}

func TestApplyReplenishmentCancelled_CancelsAPendingUnitAndWithdrawsItsPoolEntry(t *testing.T) {
	f := newPlainReplFixture(t)
	f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL"))
	outcome, err := f.cancelled.Execute(context.Background(), replCancel("evt-c", "repl-1"))
	if err != nil || outcome != usecases.ReplenishmentPlanned {
		t.Fatalf("cancel = (%v, %v)", outcome, err)
	}
	u := f.unit(t, "repl-1")
	if u.State() != workunit.Cancelled || u.CancelledAt() == nil {
		t.Fatalf("state = %v, want Cancelled with a time", u.State())
	}
	if f.backlog(t) != 0 {
		t.Fatalf("backlog = %d, want the pool entry withdrawn", f.backlog(t))
	}
	if _, err := f.release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: f.pathId}); err == nil {
		t.Fatal("a cancelled request must never be released")
	}
}

func TestApplyReplenishmentCancelled_ReleasedWorkRunsToCompletion(t *testing.T) {
	f := newPlainReplFixture(t)
	f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL"))
	if _, err := f.release.Execute(context.Background(), usecases.ReleaseNextWorkRequest{PathId: f.pathId}); err != nil {
		t.Fatalf("release: %v", err)
	}
	outcome, err := f.cancelled.Execute(context.Background(), replCancel("evt-c", "repl-1"))
	if err != nil || outcome != usecases.ReplenishmentNotCancellable {
		t.Fatalf("cancel = (%v, %v), want not cancellable", outcome, err)
	}
	if f.unit(t, "repl-1").State() != workunit.Released {
		t.Fatal("a released unit was cancelled")
	}
}

func TestApplyReplenishmentCancelled_UnknownRequestIsAProcessedSkip(t *testing.T) {
	f := newPlainReplFixture(t)
	outcome, err := f.cancelled.Execute(context.Background(), replCancel("evt-c", "never-planned"))
	if err != nil || outcome != usecases.ReplenishmentUnknownRequest {
		t.Fatalf("cancel = (%v, %v), want unknown request", outcome, err)
	}
	outcome, err = f.cancelled.Execute(context.Background(), replCancel("evt-c", "never-planned"))
	if err != nil || outcome != usecases.ReplenishmentAlreadyProcessed {
		t.Fatalf("redelivery = (%v, %v), want already processed", outcome, err)
	}
}

func TestApplyReplenishmentCancelled_SecondCancellationIsANoOp(t *testing.T) {
	f := newPlainReplFixture(t)
	f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL"))
	if _, err := f.cancelled.Execute(context.Background(), replCancel("evt-c1", "repl-1")); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	outcome, err := f.cancelled.Execute(context.Background(), replCancel("evt-c2", "repl-1"))
	if err != nil || outcome != usecases.ReplenishmentNotCancellable {
		t.Fatalf("second cancel = (%v, %v), want not cancellable", outcome, err)
	}
}

func TestApplyReplenishmentCancelled_BlankRequestIdIsRejected(t *testing.T) {
	f := newPlainReplFixture(t)
	if _, err := f.cancelled.Execute(context.Background(), replCancel("evt-c", "  ")); !errors.Is(err, workunit.ErrInvalidReplenishmentMove) {
		t.Fatalf("error = %v, want ErrInvalidReplenishmentMove", err)
	}
}

func TestApplyReplenishmentCancelled_FailedAttemptIsRetriedNotSwallowed(t *testing.T) {
	for _, newVariant := range processedVariants() {
		v := newVariant()
		t.Run(v.name, func(t *testing.T) {
			f := newReplFixture(t, v, replCatalogue(), 0)
			f.mustPlan(t, replRequest("evt-1", "repl-1", "NORMAL"))
			cancelFlaky := usecases.NewApplyReplenishmentCancelled(f.workUnits, &flakyPools{WorkPoolRepo: f.pools, failures: 1}, v.processed, memory.FixedClock{At: replRequestedAt}).WithUnitOfWork(v.uow)
			if _, err := cancelFlaky.Execute(context.Background(), replCancel("evt-c", "repl-1")); !errors.Is(err, errTransient) {
				t.Fatalf("attempt 1: error = %v, want the transient error", err)
			}
			outcome, err := cancelFlaky.Execute(context.Background(), replCancel("evt-c", "repl-1"))
			if err != nil || outcome != usecases.ReplenishmentPlanned {
				t.Fatalf("attempt 2 = (%v, %v), want a real cancel", outcome, err)
			}
			if f.unit(t, "repl-1").State() != workunit.Cancelled {
				t.Fatal("the retry did not cancel the unit")
			}
		})
	}
}

func TestReplenishmentOutcome_String(t *testing.T) {
	want := map[usecases.ReplenishmentOutcome]string{
		usecases.ReplenishmentPlanned:          "planned",
		usecases.ReplenishmentAlreadyProcessed: "already_processed",
		usecases.ReplenishmentAlreadyPlanned:   "already_planned",
		usecases.ReplenishmentUnknownRequest:   "unknown_request",
		usecases.ReplenishmentNotCancellable:   "not_cancellable",
		usecases.ReplenishmentOutcome(99):      "unknown",
	}
	for outcome, name := range want {
		if outcome.String() != name {
			t.Fatalf("%d.String() = %q, want %q", outcome, outcome.String(), name)
		}
	}
}
