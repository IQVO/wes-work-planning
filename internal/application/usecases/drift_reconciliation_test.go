package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ADR-0019: PathPlan vs LaborPlanObserved reconciliation, triggered on BOTH
// sides' commit, over the same shared comparison.

type driftFixture struct {
	plans     *memory.PlanRepo
	views     *memory.LaborPlanViewRepo
	publisher *events.LogPublisher
	clock     memory.FixedClock
	commit    *usecases.CommitShiftPlan
	observe   *usecases.ObserveLaborPlan
	pathId    shared.PathId
}

func newDriftFixture(t *testing.T) driftFixture {
	t.Helper()
	f := driftFixture{
		plans:     memory.NewPlanRepo(),
		views:     memory.NewLaborPlanViewRepo(),
		publisher: events.NewLogPublisher(nil),
		clock:     memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)},
	}
	f.pathId, _ = shared.NewPathId("pick-a")
	f.commit = usecases.NewCommitShiftPlan(f.plans, f.publisher, f.clock).WithLaborPlanViews(f.views)
	f.observe = usecases.NewObserveLaborPlan(f.views, memory.NewProcessedEventRepo()).
		WithDriftReconciliation(f.plans, f.publisher, f.clock)
	return f
}

func (f driftFixture) commitPlan(t *testing.T, heads int) {
	t.Helper()
	ph, _ := shared.NewStationCount(heads)
	is, _ := shared.NewStationCount(20)
	rate, _ := shared.NewRate(100)
	if _, err := f.commit.Execute(context.Background(), usecases.CommitShiftPlanRequest{
		PathId: f.pathId, PlannedHeads: ph, InstalledStations: is, Rate: rate, Hours: 8,
	}); err != nil {
		t.Fatalf("CommitShiftPlan: %v", err)
	}
}

func (f driftFixture) observeLabor(t *testing.T, eventId string, heads int) {
	t.Helper()
	if err := f.observe.Execute(context.Background(), usecases.ObserveLaborPlanRequest{
		EventId: eventId, PathId: f.pathId, PlannedHeads: heads, PlannedRate: 100, PlannedHours: 8,
		ObservedAt: time.Date(2026, 8, 21, 7, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("ObserveLaborPlan: %v", err)
	}
}

func (f driftFixture) driftEvents() []shared.PathPlanDriftDetected {
	var out []shared.PathPlanDriftDetected
	for _, e := range f.publisher.Events() {
		if d, ok := e.(shared.PathPlanDriftDetected); ok {
			out = append(out, d)
		}
	}
	return out
}

func (f driftFixture) view(t *testing.T) laborview.LaborPlanObserved {
	t.Helper()
	v, err := f.views.FindByPathId(context.Background(), f.pathId)
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	return v
}

// Trigger 1: our plan commits SECOND (Workforce already committed).
func TestDrift_OurCommitSecond_RaisesDriftAndRecordsItOnTheView(t *testing.T) {
	f := newDriftFixture(t)
	f.observeLabor(t, "evt-1", 7) // Workforce committed first: nothing to compare yet
	if got := f.driftEvents(); len(got) != 0 {
		t.Fatalf("no PathPlan yet — drift must not be fabricated, got %d events", len(got))
	}
	if v := f.view(t); v.DriftHeads != nil || v.DriftDetectedAt != nil {
		t.Fatalf("no comparison computed yet: want nil drift fields, got %+v", v)
	}

	f.commitPlan(t, 6)

	evs := f.driftEvents()
	if len(evs) != 1 {
		t.Fatalf("got %d drift events, want 1", len(evs))
	}
	if ev := evs[0]; ev.WesPlannedHeads != 6 || ev.ObservedPlannedHeads != 7 || ev.DriftHeads != 1 || !ev.PathId.Equals(f.pathId) {
		t.Fatalf("unexpected event: %+v", ev)
	}
	v := f.view(t)
	if v.DriftHeads == nil || *v.DriftHeads != 1 || v.DriftDetectedAt == nil || !v.DriftDetectedAt.Equal(f.clock.Now()) {
		t.Fatalf("view drift = %v @ %v, want +1 @ %v", v.DriftHeads, v.DriftDetectedAt, f.clock.Now())
	}
	// The observed plan itself must be untouched by our commit.
	if v.PlannedHeads != 7 {
		t.Fatalf("observed PlannedHeads = %d, want 7 (untouched)", v.PlannedHeads)
	}
}

// Trigger 2: Workforce's plan commits SECOND (our PathPlan already exists).
func TestDrift_WorkforceCommitSecond_RaisesSignedDrift(t *testing.T) {
	f := newDriftFixture(t)
	f.commitPlan(t, 6) // first: no LaborPlanObserved, nothing to compare
	if got := f.driftEvents(); len(got) != 0 {
		t.Fatalf("no observation yet — got %d drift events", len(got))
	}

	f.observeLabor(t, "evt-1", 4)

	evs := f.driftEvents()
	if len(evs) != 1 || evs[0].DriftHeads != -2 {
		t.Fatalf("want one event with signed drift -2, got %+v", evs)
	}
	v := f.view(t)
	if v.DriftHeads == nil || *v.DriftHeads != -2 || v.DriftDetectedAt == nil {
		t.Fatalf("view drift = %v @ %v", v.DriftHeads, v.DriftDetectedAt)
	}
}

// Agreement is a computed comparison (driftHeads = 0) but NOT an event, and
// carries no detection time.
func TestDrift_AgreeingPlansRaiseNothingButRecordZero(t *testing.T) {
	f := newDriftFixture(t)
	f.commitPlan(t, 6)
	f.observeLabor(t, "evt-1", 6)

	if got := f.driftEvents(); len(got) != 0 {
		t.Fatalf("agreeing plans raised %d drift events", len(got))
	}
	v := f.view(t)
	if v.DriftHeads == nil || *v.DriftHeads != 0 || v.DriftDetectedAt != nil {
		t.Fatalf("view drift = %v @ %v, want 0 and no detection time", v.DriftHeads, v.DriftDetectedAt)
	}
}

// Drift clears when the sides converge: re-committing to agree resets it.
func TestDrift_ConvergingClearsDriftDetectedAt(t *testing.T) {
	f := newDriftFixture(t)
	f.observeLabor(t, "evt-1", 8)
	f.commitPlan(t, 6)
	if v := f.view(t); v.DriftDetectedAt == nil {
		t.Fatal("precondition: drift should be detected")
	}

	f.commitPlan(t, 8)

	v := f.view(t)
	if v.DriftHeads == nil || *v.DriftHeads != 0 || v.DriftDetectedAt != nil {
		t.Fatalf("after converging: drift = %v @ %v, want 0 and no time", v.DriftHeads, v.DriftDetectedAt)
	}
	if got := f.driftEvents(); len(got) != 1 {
		t.Fatalf("converging must not raise another event, got %d total", len(got))
	}
}

// Without the opt-in wiring both use cases behave exactly as before ADR-0019.
func TestDrift_DisabledByDefault(t *testing.T) {
	plans := memory.NewPlanRepo()
	views := memory.NewLaborPlanViewRepo()
	pub := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	pathId, _ := shared.NewPathId("pick-a")

	ph, _ := shared.NewStationCount(6)
	is, _ := shared.NewStationCount(20)
	rate, _ := shared.NewRate(100)
	if _, err := usecases.NewCommitShiftPlan(plans, pub, clock).Execute(context.Background(), usecases.CommitShiftPlanRequest{
		PathId: pathId, PlannedHeads: ph, InstalledStations: is, Rate: rate, Hours: 8,
	}); err != nil {
		t.Fatal(err)
	}
	if err := usecases.NewObserveLaborPlan(views, memory.NewProcessedEventRepo()).Execute(context.Background(), usecases.ObserveLaborPlanRequest{
		EventId: "e", PathId: pathId, PlannedHeads: 9, ObservedAt: clock.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	for _, e := range pub.Events() {
		if _, ok := e.(shared.PathPlanDriftDetected); ok {
			t.Fatal("drift must not be raised unless reconciliation is wired")
		}
	}
	if v, _ := views.FindByPathId(context.Background(), pathId); v.DriftHeads != nil {
		t.Fatalf("drift fields must stay nil, got %v", *v.DriftHeads)
	}
}

type failingDriftPublisher struct{ err error }

func (p failingDriftPublisher) Publish(_ context.Context, evs ...shared.DomainEvent) error {
	for _, e := range evs {
		if _, ok := e.(shared.PathPlanDriftDetected); ok {
			return p.err
		}
	}
	return nil
}

// A failing drift publish fails the whole use case (inside the UnitOfWork the
// caller's scope rolls everything back — here observed via the returned error).
func TestDrift_PublishFailureFailsTheUseCase(t *testing.T) {
	boom := errors.New("outbox down")
	plans := memory.NewPlanRepo()
	views := memory.NewLaborPlanViewRepo()
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	pathId, _ := shared.NewPathId("pick-a")

	// Seed our plan, then observe a disagreeing Workforce plan with a
	// publisher that rejects the drift event.
	seed := usecases.NewCommitShiftPlan(plans, events.NewLogPublisher(nil), clock)
	ph, _ := shared.NewStationCount(6)
	is, _ := shared.NewStationCount(20)
	rate, _ := shared.NewRate(100)
	if _, err := seed.Execute(context.Background(), usecases.CommitShiftPlanRequest{PathId: pathId, PlannedHeads: ph, InstalledStations: is, Rate: rate, Hours: 8}); err != nil {
		t.Fatal(err)
	}
	uc := usecases.NewObserveLaborPlan(views, memory.NewProcessedEventRepo()).
		WithDriftReconciliation(plans, failingDriftPublisher{boom}, clock)
	err := uc.Execute(context.Background(), usecases.ObserveLaborPlanRequest{EventId: "e", PathId: pathId, PlannedHeads: 9, ObservedAt: clock.Now()})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the publish failure", err)
	}
}
