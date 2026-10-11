package main_test

// BDD coverage wave 2: step definitions for the feature files that exercise
// pool configuration, enqueue/release/complete edge cases, telemetry and
// rebalance event side effects, request validation, the Kafka-consumed use
// cases (driven through the same use cases the consumers call) and the
// liveness/readiness probes. The wiring that has to exist before the router
// is built (ConfigurePool, the process-path catalogue, the readiness gate)
// lives in wireInboundExtras and is called from newServer in
// features_test.go; everything else is registered by registerWave2Steps.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// inboundExtras are the pieces newServer needs besides the use cases it
// already builds: the consumer-side use cases (the same ones the Kafka
// consumers call) and the readiness gate the /readyz probe reflects.
type inboundExtras struct {
	taskCompleted  *usecases.ApplyTaskCompleted
	orderAllocated *usecases.ApplyOrderAllocated
	workDemand     *usecases.ApplyWorkDemandReleased
	readiness      *inboundhttp.Readiness
}

// wireInboundExtras completes the production-shaped wiring: it declares the
// fleet's sortable-fc process-path families (pick/pack/rebin/slam, ADR-0012),
// enables the ConfigurePool command (ADR-0034) and the readiness gate, and
// builds the consumer-side use cases over the same in-memory repositories.
func wireInboundExtras(
	h *inboundhttp.Handlers,
	workUnits ports.WorkUnitRepo,
	pools ports.WorkPoolRepo,
	processed ports.ProcessedEventRepo,
	publisher ports.EventPublisher,
	clock ports.Clock,
) *inboundExtras {
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick"},
		{Id: "PACK", MatchPrefix: "pack"},
		{Id: "REBIN", MatchPrefix: "rebin"},
		{Id: "SLAM", MatchPrefix: "slam"},
	})
	readiness := &inboundhttp.Readiness{}

	h.Catalogue = catalogue
	h.ConfigurePool = usecases.NewConfigurePool(pools)
	h.Readiness = readiness

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock)
	return &inboundExtras{
		taskCompleted:  usecases.NewApplyTaskCompleted(usecases.NewRecordCompletion(workUnits, pools, publisher, clock), processed),
		orderAllocated: usecases.NewApplyOrderAllocated(enqueue, processed, catalogue),
		workDemand:     usecases.NewApplyWorkDemandReleased(enqueue, processed, catalogue),
		readiness:      readiness,
	}
}

// wave2 is the per-scenario state of the wave-2 steps, layered on the shared
// world (last HTTP response, harness).
type wave2 struct {
	w *world

	// Outcome of the last consumer-side use case call.
	taskOutcome       usecases.TaskCompletedOutcome
	consumerErr       error
	consumerDuplicate bool
}

func (s *wave2) reset() {
	s.taskOutcome = usecases.TaskCompletedApplied
	s.consumerErr = nil
	s.consumerDuplicate = false
}

// doRaw sends an arbitrary body (including deliberately malformed JSON) and
// records the response as the "last" one.
func (s *wave2) doRaw(method, path, body string) error {
	req, err := http.NewRequestWithContext(context.Background(), method, s.w.server.URL+path, bytes.NewReader([]byte(body)))
	if err != nil {
		return fmt.Errorf("build %s %s: %w", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.w.server.Client().Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s %s response: %w", method, path, err)
	}
	s.w.lastStatus = resp.StatusCode
	s.w.lastBody = raw
	s.w.lastHeader = resp.Header
	return nil
}

// --- pool configuration (ADR-0034) --------------------------------------

func (s *wave2) configurePool(pathId, mode string, wipLimit int) error {
	return s.w.do(http.MethodPut, "/paths/"+pathId+"/pool", map[string]any{"mode": mode, "wipLimit": wipLimit})
}

func (s *wave2) configurePoolWithBody(pathId, body string) error {
	return s.doRaw(http.MethodPut, "/paths/"+pathId+"/pool", body)
}

func (s *wave2) poolResponseReports(mode string, wipLimit, wip, backlog int) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	gotMode, _ := obj["mode"].(string)
	gotLimit, _ := obj["wipLimit"].(float64)
	gotWIP, _ := obj["wip"].(float64)
	gotBacklog, _ := obj["backlogDepth"].(float64)
	if gotMode != mode || int(gotLimit) != wipLimit || int(gotWIP) != wip || int(gotBacklog) != backlog {
		return fmt.Errorf("got pool %s/limit %d/WIP %d/backlog %d, want %s/limit %d/WIP %d/backlog %d: %s",
			gotMode, int(gotLimit), int(gotWIP), int(gotBacklog), mode, wipLimit, wip, backlog, string(s.w.lastBody))
	}
	return nil
}

// --- work unit enqueue ---------------------------------------------------

const defaultCPT = "2026-08-21T12:00:00Z"

func (s *wave2) submitWorkUnit(id, cpt, reference, pathId string) error {
	return s.w.do(http.MethodPost, "/paths/"+pathId+"/work-units", map[string]any{
		"workUnitId": id, "cpt": cpt, "reference": reference,
	})
}

func (s *wave2) submitWorkUnitWithLineNo(id, pathId string, lineNo int) error {
	return s.w.do(http.MethodPost, "/paths/"+pathId+"/work-units", map[string]any{
		"workUnitId": id, "cpt": defaultCPT, "reference": "order-77213", "lineNo": lineNo,
	})
}

func (s *wave2) submitWorkUnitWithCharacteristics(id, pathId, sku string, lineNo int) error {
	return s.w.do(http.MethodPost, "/paths/"+pathId+"/work-units", map[string]any{
		"workUnitId": id, "cpt": defaultCPT, "reference": "order-77213",
		"sku": sku, "giftWrap": true, "lineNo": lineNo,
	})
}

func (s *wave2) submitEnqueueBody(body, pathId string) error {
	return s.doRaw(http.MethodPost, "/paths/"+pathId+"/work-units", body)
}

func (s *wave2) responseLocationIs(want string) error {
	if got := s.w.lastHeader.Get("Location"); got != want {
		return fmt.Errorf("got Location %q, want %q", got, want)
	}
	return nil
}

func (s *wave2) workUnitTimestampOrSKU(field, want string) error {
	got, err := s.w.stringField(field)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("got WorkUnit %s %q, want %q", field, got, want)
	}
	return nil
}

func (s *wave2) workUnitLineNo(want int) error {
	got, err := s.w.numberField("lineNo")
	if err != nil {
		return err
	}
	if int(got) != want {
		return fmt.Errorf("got line number %d, want %d", int(got), want)
	}
	return nil
}

func (s *wave2) workUnitOmits(field string) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	if value, present := obj[field]; present {
		return fmt.Errorf("work unit carries %s = %v, want it omitted: %s", field, value, string(s.w.lastBody))
	}
	return nil
}

func (s *wave2) workUnitGiftWrap(want bool) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	got, ok := obj["giftWrap"].(bool)
	if !ok {
		return fmt.Errorf("work unit carries no boolean giftWrap: %s", string(s.w.lastBody))
	}
	if got != want {
		return fmt.Errorf("got giftWrap %v, want %v", got, want)
	}
	return nil
}

func (s *wave2) everyWorkUnitInState(state string) error {
	units, err := s.w.decodeLastArray()
	if err != nil {
		return err
	}
	for _, unit := range units {
		obj, _ := unit.(map[string]any)
		if got, _ := obj["state"].(string); got != state {
			return fmt.Errorf("got work unit state %q, want %q", got, state)
		}
	}
	return nil
}

// --- telemetry and events ------------------------------------------------

func (s *wave2) sampleTelemetry(pathId string) error {
	return s.w.do(http.MethodGet, "/paths/"+pathId+"/telemetry", nil)
}

func (s *wave2) telemetryRemainingUnknown() error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	known, present := obj["remainingCapacityKnown"].(bool)
	if !present || known {
		return fmt.Errorf("remaining capacity should be reported as not known: %s", string(s.w.lastBody))
	}
	if _, present := obj["remainingCapacityUnits"]; present {
		return fmt.Errorf("remainingCapacityUnits must be omitted when the capacity is not known: %s", string(s.w.lastBody))
	}
	return nil
}

func (s *wave2) telemetryOverAlarmThreshold() error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	if over, _ := obj["overAlarmThreshold"].(bool); !over {
		return fmt.Errorf("telemetry is not over its alarm threshold: %s", string(s.w.lastBody))
	}
	return nil
}

func (s *wave2) eventsPublished(count int, name string) error {
	if got := s.w.h.published.count(name); got != count {
		return fmt.Errorf("got %d %s events, want %d (published: %v)", got, name, count, s.w.h.published.names)
	}
	return nil
}

func (s *wave2) lastPathCapacityChanged() (shared.PathCapacityChanged, error) {
	events := s.w.h.published.events
	for i := len(events) - 1; i >= 0; i-- {
		if ev, ok := events[i].(shared.PathCapacityChanged); ok {
			return ev, nil
		}
	}
	return shared.PathCapacityChanged{}, fmt.Errorf("no PathCapacityChanged event was published (published: %v)", s.w.h.published.names)
}

func (s *wave2) capacityEventReports(cutoff string, units int) error {
	ev, err := s.lastPathCapacityChanged()
	if err != nil {
		return err
	}
	want, err := time.Parse(time.RFC3339, cutoff)
	if err != nil {
		return err
	}
	if !ev.CutoffAt.Equal(want) || !ev.Known || ev.RemainingUnits != units {
		return fmt.Errorf("got PathCapacityChanged cutoff %s known=%v remaining=%d, want cutoff %s known=true remaining=%d",
			ev.CutoffAt.Format(time.RFC3339), ev.Known, ev.RemainingUnits, cutoff, units)
	}
	return nil
}

func (s *wave2) capacityEventUnknown(cutoff string) error {
	ev, err := s.lastPathCapacityChanged()
	if err != nil {
		return err
	}
	want, err := time.Parse(time.RFC3339, cutoff)
	if err != nil {
		return err
	}
	if !ev.CutoffAt.Equal(want) || ev.Known || ev.RemainingUnits != 0 {
		return fmt.Errorf("got PathCapacityChanged cutoff %s known=%v remaining=%d, want cutoff %s known=false remaining=0",
			ev.CutoffAt.Format(time.RFC3339), ev.Known, ev.RemainingUnits, cutoff)
	}
	return nil
}

func (s *wave2) rebalanceOmitsLaborPlan() error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	if _, present := obj["laborPlan"]; present {
		return fmt.Errorf("rebalance response carries laborPlan although none was observed: %s", string(s.w.lastBody))
	}
	return nil
}

// --- shift plan and charge forecast validation ---------------------------

func (s *wave2) commitShiftPlanValues(pathId string, heads, stations int, rate, hours float64) error {
	return s.w.do(http.MethodPost, "/paths/"+pathId+"/plan", map[string]any{
		"plannedHeads":      heads,
		"installedStations": stations,
		"rateUnitsPerHour":  rate,
		"hours":             hours,
	})
}

func (s *wave2) commitShiftPlanWithout(pathId, field string) error {
	body := map[string]any{
		"plannedHeads":      6,
		"installedStations": 8,
		"rateUnitsPerHour":  95.5,
		"hours":             8,
	}
	delete(body, field)
	return s.w.do(http.MethodPost, "/paths/"+pathId+"/plan", body)
}

func (s *wave2) committedPlanHasNoTravelDistance() error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	for _, field := range []string{"travelDistanceM", "travelDistanceEstimated"} {
		if value, present := obj[field]; present {
			return fmt.Errorf("committed plan carries %s = %v although no hint was requested: %s", field, value, string(s.w.lastBody))
		}
	}
	return nil
}

func (s *wave2) chargeForecastBody(body, pathId string) error {
	return s.doRaw(http.MethodPost, "/paths/"+pathId+"/charge", body)
}

func (s *wave2) chargeForecastSingleBucket(quantity int, pathId string) error {
	return s.w.do(http.MethodPost, "/paths/"+pathId+"/charge", map[string]any{
		"buckets": []map[string]any{{"cpt": defaultCPT, "quantity": quantity}},
	})
}

// --- Kafka-consumed use cases --------------------------------------------

func (s *wave2) publishTaskCompleted(eventId, workUnitId string) error {
	s.taskOutcome, s.consumerErr = s.w.h.inbound.taskCompleted.Execute(context.Background(), usecases.ApplyTaskCompletedRequest{
		EventId:    eventId,
		WorkUnitId: workUnitId,
		OccurredAt: fixedNow,
	})
	return nil
}

func (s *wave2) taskCompletedOutcomeIs(want usecases.TaskCompletedOutcome, label string) error {
	if s.consumerErr != nil {
		return fmt.Errorf("TaskCompleted failed: %w", s.consumerErr)
	}
	if s.taskOutcome != want {
		return fmt.Errorf("got TaskCompleted outcome %d, want %d (%s)", s.taskOutcome, want, label)
	}
	return nil
}

func (s *wave2) taskCompletedApplied() error {
	return s.taskCompletedOutcomeIs(usecases.TaskCompletedApplied, "applied")
}

func (s *wave2) taskCompletedRedelivery() error {
	return s.taskCompletedOutcomeIs(usecases.TaskCompletedAlreadyProcessed, "redelivery")
}

func (s *wave2) taskCompletedUnknownUnit() error {
	return s.taskCompletedOutcomeIs(usecases.TaskCompletedUnknownWorkUnit, "unknown work unit")
}

func (s *wave2) taskCompletedFailsForRetry() error {
	if !errors.Is(s.consumerErr, workunit.ErrNotReleased) {
		return fmt.Errorf("got TaskCompleted error %v, want %v", s.consumerErr, workunit.ErrNotReleased)
	}
	return nil
}

func (s *wave2) publishOrderAllocated(eventId, orderId, promise string, table *godog.Table) error {
	promiseDate, err := time.Parse(time.RFC3339, promise)
	if err != nil {
		return fmt.Errorf("promise date %q: %w", promise, err)
	}
	if len(table.Rows) < 2 {
		return errors.New("the lines table needs a header row and at least one line")
	}
	header := table.Rows[0].Cells
	column := func(ri int, name string) (string, error) {
		cells := table.Rows[ri].Cells
		for i, h := range header {
			if h.Value == name {
				return cells[i].Value, nil
			}
		}
		return "", fmt.Errorf("lines table has no %q column", name)
	}
	var lines []usecases.OrderAllocatedLine
	for ri := 1; ri < len(table.Rows); ri++ {
		lineNoRaw, err := column(ri, "line_no")
		if err != nil {
			return err
		}
		lineNo, err := strconv.Atoi(lineNoRaw)
		if err != nil {
			return fmt.Errorf("line_no %q: %w", lineNoRaw, err)
		}
		sku, err := column(ri, "sku")
		if err != nil {
			return err
		}
		pathId, err := column(ri, "path_id")
		if err != nil {
			return err
		}
		giftWrap, err := column(ri, "gift_wrap")
		if err != nil {
			return err
		}
		lines = append(lines, usecases.OrderAllocatedLine{LineNo: lineNo, SKU: sku, PathId: pathId, GiftWrap: giftWrap == "true"})
	}
	s.consumerDuplicate, s.consumerErr = s.w.h.inbound.orderAllocated.Execute(context.Background(), usecases.ApplyOrderAllocatedRequest{
		EventId:     eventId,
		OccurredAt:  fixedNow,
		OrderId:     orderId,
		PromiseDate: promiseDate,
		Lines:       lines,
	})
	return nil
}

func (s *wave2) publishWorkDemandReleased(eventId, demandId, kind, pathId string, quantity int, sku, cpt string) error {
	cptTime, err := time.Parse(time.RFC3339, cpt)
	if err != nil {
		return fmt.Errorf("cpt %q: %w", cpt, err)
	}
	s.consumerDuplicate, s.consumerErr = s.w.h.inbound.workDemand.Execute(context.Background(), usecases.ApplyWorkDemandReleasedRequest{
		EventId:     eventId,
		OccurredAt:  fixedNow,
		DemandId:    demandId,
		WorkKind:    workunit.WorkKind(kind),
		TransferRef: "transfer-" + demandId,
		PathId:      pathId,
		SiteId:      "site-1",
		SKU:         sku,
		Quantity:    quantity,
		CPT:         cptTime,
	})
	return nil
}

func (s *wave2) consumedEventApplied() error {
	if s.consumerErr != nil {
		return fmt.Errorf("event was rejected: %w", s.consumerErr)
	}
	if s.consumerDuplicate {
		return errors.New("event was treated as a redelivery, want it applied")
	}
	return nil
}

func (s *wave2) consumedEventRedelivery() error {
	if s.consumerErr != nil {
		return fmt.Errorf("event was rejected: %w", s.consumerErr)
	}
	if !s.consumerDuplicate {
		return errors.New("event was applied again, want it ignored as a redelivery")
	}
	return nil
}

func (s *wave2) consumedEventRejectedUnknownPath() error {
	if !errors.Is(s.consumerErr, pathcatalog.ErrUnknownPath) {
		return fmt.Errorf("got error %v, want %v", s.consumerErr, pathcatalog.ErrUnknownPath)
	}
	return nil
}

func (s *wave2) consumedEventRejectedUnknownKind() error {
	if !errors.Is(s.consumerErr, workunit.ErrUnknownWorkKind) {
		return fmt.Errorf("got error %v, want %v", s.consumerErr, workunit.ErrUnknownWorkKind)
	}
	return nil
}

func (s *wave2) observeInventoryEvent(eventId, sku string, delta int) error {
	_, err := s.w.h.observeInventory.Execute(context.Background(), usecases.ObserveInventoryChangeRequest{
		EventId:    eventId,
		SKU:        sku,
		Delta:      delta,
		ObservedAt: fixedNow,
	})
	return err
}

func (s *wave2) observeLaborPlanEvent(eventId string, heads int, rate, hours float64, pathId string) error {
	pid, err := shared.NewPathId(pathId)
	if err != nil {
		return err
	}
	return s.w.h.observeLaborPlan.Execute(context.Background(), usecases.ObserveLaborPlanRequest{
		EventId:      eventId,
		PathId:       pid,
		PlannedHeads: heads,
		PlannedRate:  rate,
		PlannedHours: hours,
		ObservedAt:   fixedNow,
	})
}

// --- probes --------------------------------------------------------------

func (s *wave2) requestProbe(path string) error {
	return s.w.do(http.MethodGet, path, nil)
}

func (s *wave2) beginGracefulShutdown() error {
	s.w.h.inbound.readiness.SetNotReady()
	return nil
}

func (s *wave2) probeReportsStatus(want string) error {
	got, err := s.w.stringField("status")
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("got probe status %q, want %q", got, want)
	}
	return nil
}

// registerWave2Steps registers every wave-2 step definition on the scenario
// context, sharing the world the original steps use.
func registerWave2Steps(sc *godog.ScenarioContext, w *world) {
	s := &wave2{w: w}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		s.reset()
		return ctx, nil
	})

	// Pool configuration.
	sc.Step(`^the pool for process path "([^"]*)" is configured as "([^"]*)" with a WIP limit of (-?\d+)$`, s.configurePool)
	sc.Step(`^the pool for process path "([^"]*)" is configured with the body (.+)$`, s.configurePoolWithBody)
	sc.Step(`^the pool response reports mode "([^"]*)" with a WIP limit of (\d+), WIP (\d+) and backlog depth (\d+)$`, s.poolResponseReports)

	// Work unit enqueue and read-back.
	sc.Step(`^a WorkUnit "([^"]*)" with CPT "([^"]*)" and reference "([^"]*)" is submitted to process path "([^"]*)"$`, s.submitWorkUnit)
	sc.Step(`^a WorkUnit "([^"]*)" is submitted to process path "([^"]*)" with line number (-?\d+)$`, s.submitWorkUnitWithLineNo)
	sc.Step(`^a WorkUnit "([^"]*)" is submitted to process path "([^"]*)" with SKU "([^"]*)", gift wrap requested and line number (\d+)$`, s.submitWorkUnitWithCharacteristics)
	sc.Step(`^an enqueue request with the body (.+) is submitted to process path "([^"]*)"$`, s.submitEnqueueBody)
	sc.Step(`^the response Location is "([^"]*)"$`, s.responseLocationIs)
	sc.Step(`^the WorkUnit in the response has (releasedAt|completedAt|sku) "([^"]*)"$`, s.workUnitTimestampOrSKU)
	sc.Step(`^the WorkUnit in the response has line number (\d+)$`, s.workUnitLineNo)
	sc.Step(`^the WorkUnit in the response has no (releasedAt|completedAt|sku|lineNo)$`, s.workUnitOmits)
	sc.Step(`^the WorkUnit in the response requests gift wrap$`, func() error { return s.workUnitGiftWrap(true) })
	sc.Step(`^the WorkUnit in the response does not request gift wrap$`, func() error { return s.workUnitGiftWrap(false) })
	sc.Step(`^every returned work unit is in state "([^"]*)"$`, s.everyWorkUnitInState)

	// Telemetry, rebalance and published events.
	sc.Step(`^the Work Pool telemetry for process path "([^"]*)" is sampled$`, s.sampleTelemetry)
	sc.Step(`^the telemetry reports that remaining admission capacity is not known$`, s.telemetryRemainingUnknown)
	sc.Step(`^the telemetry reports being over its alarm threshold$`, s.telemetryOverAlarmThreshold)
	sc.Step(`^(\d+) "([^"]*)" events? (?:was|were) published$`, s.eventsPublished)
	sc.Step(`^the PathCapacityChanged event reports a cutoff of "([^"]*)" with (\d+) remaining units$`, s.capacityEventReports)
	sc.Step(`^the PathCapacityChanged event reports a cutoff of "([^"]*)" with unknown remaining capacity$`, s.capacityEventUnknown)
	sc.Step(`^the rebalance recommendation carries no labor plan$`, s.rebalanceOmitsLaborPlan)

	// Shift plan and charge forecast validation.
	sc.Step(`^a ShiftPlan is committed for process path "([^"]*)" with (-?\d+) planned heads and (-?\d+) installed stations at a rate of (-?[0-9.]+(?:[eE][+-]?\d+)?) units per hour for (-?[0-9.]+(?:[eE][+-]?\d+)?) hours$`, s.commitShiftPlanValues)
	sc.Step(`^a ShiftPlan is committed for process path "([^"]*)" without the "([^"]*)" field$`, s.commitShiftPlanWithout)
	sc.Step(`^the committed PathPlan reports no travel distance hint$`, s.committedPlanHasNoTravelDistance)
	sc.Step(`^the charge forecast body (.+) is received for process path "([^"]*)"$`, s.chargeForecastBody)
	sc.Step(`^a charge forecast with a single bucket of (\d+) units is received for process path "([^"]*)"$`, s.chargeForecastSingleBucket)

	// Kafka-consumed use cases.
	sc.Step(`^fulfillment-execution publishes TaskCompleted event "([^"]*)" for WorkUnit "([^"]*)"$`, s.publishTaskCompleted)
	sc.Step(`^the TaskCompleted event is applied$`, s.taskCompletedApplied)
	sc.Step(`^the TaskCompleted event is ignored as a redelivery$`, s.taskCompletedRedelivery)
	sc.Step(`^the TaskCompleted event is acknowledged without effect because the work unit is unknown$`, s.taskCompletedUnknownUnit)
	sc.Step(`^the TaskCompleted event fails so that it can be retried$`, s.taskCompletedFailsForRetry)
	sc.Step(`^order-management publishes OrderAllocated event "([^"]*)" for order "([^"]*)" promised for "([^"]*)" with lines:$`, s.publishOrderAllocated)
	sc.Step(`^network-inventory-planning publishes WorkDemandReleased event "([^"]*)" for demand "([^"]*)" of kind "([^"]*)" on process path "([^"]*)" for (\d+) units of SKU "([^"]*)" due at "([^"]*)"$`, s.publishWorkDemandReleased)
	sc.Step(`^the (?:OrderAllocated|WorkDemandReleased) event is applied$`, s.consumedEventApplied)
	sc.Step(`^the (?:OrderAllocated|WorkDemandReleased) event is ignored as a redelivery$`, s.consumedEventRedelivery)
	sc.Step(`^the (?:OrderAllocated|WorkDemandReleased) event is rejected for an unrecognized process path$`, s.consumedEventRejectedUnknownPath)
	sc.Step(`^the WorkDemandReleased event is rejected for an unknown work kind$`, s.consumedEventRejectedUnknownKind)
	sc.Step(`^inventory event "([^"]*)" changes SKU "([^"]*)" by ([+-]?\d+) units$`, s.observeInventoryEvent)
	sc.Step(`^Workforce Management publishes labor plan event "([^"]*)" of (\d+) heads at ([\d.]+) units per hour for ([\d.]+) hours for process path "([^"]*)"$`, s.observeLaborPlanEvent)

	// Probes.
	sc.Step(`^the liveness probe is requested$`, func() error { return s.requestProbe("/healthz") })
	sc.Step(`^the readiness probe is requested$`, func() error { return s.requestProbe("/readyz") })
	sc.Step(`^the service begins graceful shutdown$`, s.beginGracefulShutdown)
	sc.Step(`^the probe reports status "([^"]*)"$`, s.probeReportsStatus)
}
