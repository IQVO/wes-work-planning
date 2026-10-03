package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// testCatalogue is the fixture catalogue every consumer test uses,
// mirroring warehouse-infra's real sortable-fc.yaml declared paths and
// fulfillment-execution's own MatchPrefix matching semantics — see that
// repo's ADR-0017.
func testCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
		{Id: "PACK", MatchPrefix: "pack", RequiredCapabilities: []string{"pack"}},
		{Id: "REBIN", MatchPrefix: "rebin", RequiredCapabilities: []string{"rebin"}},
		{Id: "SLAM", MatchPrefix: "slam", RequiredCapabilities: []string{"slam"}},
	})
}

// fulfillmentFixture wires just enough of the real stack (in-memory repos,
// the existing EnqueueWorkUnit/ReleaseNextWork/RecordCompletion use cases) to
// exercise handleFulfillmentEvent without touching a broker.
type fulfillmentFixture struct {
	workUnits *memory.WorkUnitRepo
	processed *memory.ProcessedEventRepo
	consumer  *Consumer
}

func newFulfillmentFixture() fulfillmentFixture {
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	processed := memory.NewProcessedEventRepo()

	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, publisher, clock)

	return fulfillmentFixture{
		workUnits: workUnits,
		processed: processed,
		consumer: &Consumer{
			applyTaskCompleted: usecases.NewApplyTaskCompleted(recordCompletion, processed),
			catalogue:          testCatalogue(),
		},
	}.withReleasedUnit(pools, publisher, clock, workUnits)
}

func (f fulfillmentFixture) withReleasedUnit(pools *memory.WorkPoolRepo, publisher *events.LogPublisher, clock memory.FixedClock, workUnits *memory.WorkUnitRepo) fulfillmentFixture {
	ctx := context.Background()
	pathId, _ := shared.NewPathId("pick-a")
	cpt := shared.NewCPT(clock.At.Add(2 * time.Hour))

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)
	release := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock)

	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{WorkUnitId: "wu-1", PathId: pathId, CPT: cpt, Reference: "ref-1"}); err != nil {
		panic(err)
	}
	if _, err := release.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		panic(err)
	}
	return f
}

// testEvent builds a valid CloudEvents 1.0 event as another bounded
// context would publish it, for feeding the handlers directly.
func testEvent(t *testing.T, id, ceType, source, subject string, data any) ce.Event {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetType(ceType)
	e.SetSource(source)
	e.SetSubject(subject)
	e.SetTime(time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC))
	if err := e.SetData(ce.ApplicationJSON, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("invalid test event: %v", err)
	}
	return e
}

func taskCompletedEnvelope(t *testing.T, eventId, workUnitId string) ce.Event {
	t.Helper()
	return testEvent(t, eventId, cloudevents.TypeTaskCompleted, "/warehouse/fulfillment-execution", "task-1",
		taskCompletedData{TaskId: "task-1", StationId: "station-1", WorkUnitId: workUnitId})
}

func TestHandleFulfillmentEvent_CompletesTheWorkUnit(t *testing.T) {
	f := newFulfillmentFixture()

	if err := f.consumer.handleFulfillmentEvent(context.Background(), taskCompletedEnvelope(t, "evt-task-1", "wu-1")); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "wu-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.State() != workunit.Completed {
		t.Fatalf("expected work unit to be Completed, got %v", unit.State())
	}
}

func TestHandleFulfillmentEvent_IgnoresOtherEventTypes(t *testing.T) {
	f := newFulfillmentFixture()

	env := taskCompletedEnvelope(t, "evt-other", "wu-1")
	env.SetType("com.warehouse.wes.example.thing.SomethingElse")

	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("handleFulfillmentEvent: %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "wu-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.State() != workunit.Released {
		t.Fatalf("expected work unit to remain Released, got %v", unit.State())
	}
}

func TestHandleFulfillmentEvent_RedeliveryDoesNotInvokeRecordCompletionAgain(t *testing.T) {
	f := newFulfillmentFixture()
	env := taskCompletedEnvelope(t, "evt-task-dup", "wu-1")

	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("first handleFulfillmentEvent: %v", err)
	}

	// Redelivery of the same event_id must not call RecordCompletion again;
	// if it did, this would surface workunit.ErrAlreadyCompleted instead of
	// silently no-op'ing via the processed_events idempotency check.
	if err := f.consumer.handleFulfillmentEvent(context.Background(), env); err != nil {
		t.Fatalf("redelivered handleFulfillmentEvent: %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "wu-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.State() != workunit.Completed {
		t.Fatalf("expected work unit to be Completed, got %v", unit.State())
	}
}

// orderManagementFixture wires just enough of the real stack (in-memory
// repos, the existing EnqueueWorkUnit use case) to exercise
// handleOrderManagementEvent without touching a broker.
type orderManagementFixture struct {
	workUnits *memory.WorkUnitRepo
	pools     *memory.WorkPoolRepo
	processed *memory.ProcessedEventRepo
	consumer  *Consumer
}

func newOrderManagementFixture() orderManagementFixture {
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	processed := memory.NewProcessedEventRepo()

	enqueueWorkUnit := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)

	return orderManagementFixture{
		workUnits: workUnits,
		pools:     pools,
		processed: processed,
		consumer: &Consumer{
			applyOrderAllocated: usecases.NewApplyOrderAllocated(enqueueWorkUnit, processed, testCatalogue()),
			catalogue:           testCatalogue(),
		},
	}
}

func orderAllocatedEnvelope(t *testing.T, eventId, eventType, orderId string, lines []orderLineData) ce.Event {
	t.Helper()
	return testEvent(t, eventId, eventType, "/warehouse/order-management", orderId, orderAllocatedData{
		OrderId:     orderId,
		PromiseDate: time.Date(2026, 8, 21, 23, 0, 0, 0, time.UTC),
		Lines:       lines,
	})
}

func TestHandleOrderManagementEvent_EnqueuesOneWorkUnitPerLine(t *testing.T) {
	f := newOrderManagementFixture()
	lines := []orderLineData{
		{LineNo: 1, SKU: "SKU-1", PathId: "pick-a", GiftWrap: false},
		{LineNo: 2, SKU: "SKU-2", PathId: "pick-a", GiftWrap: true},
	}
	env := orderAllocatedEnvelope(t, "evt-order-1", cloudevents.TypeOrderAllocated, "order-1", lines)

	if err := f.consumer.handleOrderManagementEvent(context.Background(), env); err != nil {
		t.Fatalf("handleOrderManagementEvent: %v", err)
	}

	unit1, err := f.workUnits.FindById(context.Background(), "order-1-line-1")
	if err != nil {
		t.Fatalf("FindById line 1: %v", err)
	}
	if unit1.SKU() != "SKU-1" || unit1.GiftWrap() {
		t.Fatalf("unexpected unit1: sku=%v giftWrap=%v", unit1.SKU(), unit1.GiftWrap())
	}

	unit2, err := f.workUnits.FindById(context.Background(), "order-1-line-2")
	if err != nil {
		t.Fatalf("FindById line 2: %v", err)
	}
	if unit2.SKU() != "SKU-2" || !unit2.GiftWrap() {
		t.Fatalf("unexpected unit2: sku=%v giftWrap=%v", unit2.SKU(), unit2.GiftWrap())
	}
}

func TestHandleOrderManagementEvent_PartiallyAllocatedBehavesIdentically(t *testing.T) {
	f := newOrderManagementFixture()
	lines := []orderLineData{{LineNo: 1, SKU: "SKU-9", PathId: "pick-a", GiftWrap: false}}
	env := orderAllocatedEnvelope(t, "evt-order-partial", cloudevents.TypeOrderPartiallyAllocated, "order-9", lines)

	if err := f.consumer.handleOrderManagementEvent(context.Background(), env); err != nil {
		t.Fatalf("handleOrderManagementEvent: %v", err)
	}

	unit, err := f.workUnits.FindById(context.Background(), "order-9-line-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.SKU() != "SKU-9" {
		t.Fatalf("unexpected sku: %v", unit.SKU())
	}
}

func TestHandleOrderManagementEvent_IgnoresOtherEventTypes(t *testing.T) {
	f := newOrderManagementFixture()
	lines := []orderLineData{{LineNo: 1, SKU: "SKU-1", PathId: "pick-a"}}
	env := orderAllocatedEnvelope(t, "evt-order-other", "SomethingElse", "order-1", lines)

	if err := f.consumer.handleOrderManagementEvent(context.Background(), env); err != nil {
		t.Fatalf("handleOrderManagementEvent: %v", err)
	}

	if _, err := f.workUnits.FindById(context.Background(), "order-1-line-1"); err == nil {
		t.Fatalf("expected no work unit to be enqueued for an unknown event type")
	}
}

func TestHandleOrderManagementEvent_RedeliveryDoesNotEnqueueAgain(t *testing.T) {
	f := newOrderManagementFixture()
	lines := []orderLineData{{LineNo: 1, SKU: "SKU-1", PathId: "pick-a"}}
	env := orderAllocatedEnvelope(t, "evt-order-dup", cloudevents.TypeOrderAllocated, "order-1", lines)

	if err := f.consumer.handleOrderManagementEvent(context.Background(), env); err != nil {
		t.Fatalf("first handleOrderManagementEvent: %v", err)
	}
	// Redelivery of the same event_id must not call EnqueueWorkUnit again;
	// the processed_events idempotency check must short-circuit before any
	// enqueue attempt, so a genuine release.ErrDuplicateEntry from the pool
	// is never even reached on the common redelivery path.
	if err := f.consumer.handleOrderManagementEvent(context.Background(), env); err != nil {
		t.Fatalf("redelivered handleOrderManagementEvent: %v", err)
	}

	pool, err := f.pools.FindByPathId(context.Background(), mustPathId(t, "pick-a"))
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	if got := pool.BacklogDepth(); got != 1 {
		t.Fatalf("expected exactly one pending entry after redelivery, got %d", got)
	}
}

func mustPathId(t *testing.T, value string) shared.PathId {
	t.Helper()
	pathId, err := shared.NewPathId(value)
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	return pathId
}

// The core behavior change the process-path catalogue introduces here:
// an order-management line referencing a path_id no declared path family
// recognizes is rejected outright, and creates zero work units — not
// silently accepted into a WorkPool nothing downstream will ever
// service. See fulfillment-execution's ADR-0017.
func TestHandleOrderManagementEvent_UnknownPathId_ReturnsError(t *testing.T) {
	f := newOrderManagementFixture()
	lines := []orderLineData{{LineNo: 1, SKU: "SKU-1", PathId: "not-a-real-path", GiftWrap: false}}
	env := orderAllocatedEnvelope(t, "evt-unknown", cloudevents.TypeOrderAllocated, "order-unknown", lines)

	err := f.consumer.handleOrderManagementEvent(context.Background(), env)
	if !errors.Is(err, pathcatalog.ErrUnknownPath) {
		t.Fatalf("expected ErrUnknownPath, got %v", err)
	}
	if _, err := f.workUnits.FindById(context.Background(), "order-unknown-line-1"); err == nil {
		t.Fatalf("expected no work unit to be enqueued for an unrecognized path_id")
	}
}

// Real fleet path_id forms (order-management's default "pick", zone/
// station-qualified variants) must all resolve — the catalogue's
// MatchPrefix family matching, not an exact-match lookup. This is the
// exact regression class fulfillment-execution's ADR-0017 addendum
// documents; this service's own catalogue mirror must not repeat it.
func TestHandleOrderManagementEvent_ResolvesRealFleetPathIdVariants(t *testing.T) {
	f := newOrderManagementFixture()
	lines := []orderLineData{
		{LineNo: 1, SKU: "SKU-1", PathId: "pick"},
		{LineNo: 2, SKU: "SKU-2", PathId: "pick-zone-a"},
		{LineNo: 3, SKU: "SKU-3", PathId: "pack-soak"},
	}
	env := orderAllocatedEnvelope(t, "evt-variants", cloudevents.TypeOrderAllocated, "order-variants", lines)

	if err := f.consumer.handleOrderManagementEvent(context.Background(), env); err != nil {
		t.Fatalf("unexpected error resolving real-world path_id variants: %v", err)
	}
	for _, id := range []string{"order-variants-line-1", "order-variants-line-2", "order-variants-line-3"} {
		if _, err := f.workUnits.FindById(context.Background(), id); err != nil {
			t.Fatalf("FindById %s: %v", id, err)
		}
	}
}

// workforceFixture wires just enough of the real stack (in-memory
// labor-plan-view repo, the existing ObserveLaborPlan use case) to
// exercise handleWorkforceEvent without touching a broker.
type workforceFixture struct {
	laborPlanViews *memory.LaborPlanViewRepo
	consumer       *Consumer
}

func newWorkforceFixture() workforceFixture {
	laborPlanViews := memory.NewLaborPlanViewRepo()
	processed := memory.NewProcessedEventRepo()
	observeLabor := usecases.NewObserveLaborPlan(laborPlanViews, processed)

	return workforceFixture{
		laborPlanViews: laborPlanViews,
		consumer: &Consumer{
			observeLabor: observeLabor,
			catalogue:    testCatalogue(),
		},
	}
}

func shiftPlanCommittedEnvelope(t *testing.T, eventId, pathId string) ce.Event {
	t.Helper()
	return testEvent(t, eventId, cloudevents.TypeShiftPlanCommitted, "/warehouse/workforce-management", "shift-1", shiftPlanCommittedData{
		BuildingId:   "wh1",
		ShiftId:      "shift-1",
		PathId:       pathId,
		PlannedHeads: 4,
		PlannedRate:  90,
		PlannedHours: 8,
	})
}

func TestHandleWorkforceEvent_ObservesLaborPlanForARecognizedPath(t *testing.T) {
	f := newWorkforceFixture()
	env := shiftPlanCommittedEnvelope(t, "evt-shift-1", "pick-zone-a")

	if err := f.consumer.handleWorkforceEvent(context.Background(), env); err != nil {
		t.Fatalf("handleWorkforceEvent: %v", err)
	}

	view, err := f.laborPlanViews.FindByPathId(context.Background(), mustPathId(t, "pick-zone-a"))
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	if view.PlannedHeads != 4 {
		t.Fatalf("expected PlannedHeads 4, got %d", view.PlannedHeads)
	}
}

// The same catalogue-validation contract as the order-management path:
// an unrecognized path_id on ShiftPlanCommitted is rejected, not
// silently accepted into a labor-plan-view nothing will ever route real
// work through.
func TestHandleWorkforceEvent_UnknownPathId_ReturnsError(t *testing.T) {
	f := newWorkforceFixture()
	env := shiftPlanCommittedEnvelope(t, "evt-shift-unknown", "not-a-real-path")

	err := f.consumer.handleWorkforceEvent(context.Background(), env)
	if !errors.Is(err, pathcatalog.ErrUnknownPath) {
		t.Fatalf("expected ErrUnknownPath, got %v", err)
	}
}

func TestHandleWorkforceEvent_IgnoresOtherEventTypes(t *testing.T) {
	f := newWorkforceFixture()
	env := shiftPlanCommittedEnvelope(t, "evt-shift-other", "pick-zone-a")
	env.SetType("com.warehouse.wes.example.thing.SomethingElse")

	if err := f.consumer.handleWorkforceEvent(context.Background(), env); err != nil {
		t.Fatalf("handleWorkforceEvent: %v", err)
	}
	if _, err := f.laborPlanViews.FindByPathId(context.Background(), mustPathId(t, "pick-zone-a")); err == nil {
		t.Fatalf("expected no labor plan view to be observed for an unrelated event type")
	}
}
