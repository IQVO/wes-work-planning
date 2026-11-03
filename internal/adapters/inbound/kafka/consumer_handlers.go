package kafka

import (
	"context"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

func (c *Consumer) handleWorkforceEvent(ctx context.Context, env ce.Event) error {
	if env.Type() != cloudevents.TypeShiftPlanCommitted {
		return nil
	}

	var data shiftPlanCommittedData
	if err := env.DataAs(&data); err != nil {
		return err
	}
	pathId, err := shared.NewPathId(data.PathId)
	if err != nil {
		return err
	}
	// Validate against the declared process-path catalogue: a
	// ShiftPlanCommitted for a path_id no declared path family
	// recognizes is rejected outright rather than silently accepted
	// into a labor-plan-view nothing else will ever route real work
	// through. This is validation only — the WorkPool/labor-plan-view
	// key stays the original granular pathId (e.g. "pick-zone-a"), not
	// the catalogue's coarser family id, since each zone/station is a
	// genuinely distinct queue. See fulfillment-execution's ADR-0017
	// for the full catalogue rationale.
	if _, err := c.catalogue.Lookup(pathId.String()); err != nil {
		return err
	}

	return c.observeLabor.Execute(ctx, usecases.ObserveLaborPlanRequest{
		EventId:      env.ID(),
		PathId:       pathId,
		PlannedHeads: data.PlannedHeads,
		PlannedRate:  data.PlannedRate,
		PlannedHours: data.PlannedHours,
		ObservedAt:   env.Time(),
	})
}

func (c *Consumer) handleInventoryEvent(ctx context.Context, env ce.Event) error {
	var delta int
	switch env.Type() {
	case cloudevents.TypeStockReserved:
		delta = -1
	case cloudevents.TypeReservationRevoked:
		delta = 1
	default:
		return nil
	}

	var data inventoryEventData
	if err := env.DataAs(&data); err != nil {
		return err
	}

	_, err := c.observeInventory.Execute(ctx, usecases.ObserveInventoryChangeRequest{
		EventId:    env.ID(),
		SKU:        data.SKU,
		Quantity:   data.Quantity,
		Delta:      delta * data.Quantity,
		ObservedAt: env.Time(),
	})
	return err
}

// handleFulfillmentEvent filters for TaskCompleted and feeds it into
// ApplyTaskCompleted, which marks the CloudEvents id processed in the same
// atomic scope as RecordCompletion (ADR-0028). Outcomes:
//
//   - applied / redelivery: nil.
//   - unknown work unit (ports.ErrNotFound from RecordCompletion): a
//     deliberate, INFO-logged skip returning nil — neither retried nor
//     dead-lettered. warehouse.fulfillment.events is shared: a PACK task
//     fulfillment-execution created itself during rebin consolidation
//     carries the ORDER id as work_unit_id, which this context never
//     planned. The event is still marked processed so redelivery is cheap.
//   - any other error: returned with nothing committed, so handleMessage
//     retries it and, once retries are exhausted, dead-letters it.
func (c *Consumer) handleFulfillmentEvent(ctx context.Context, env ce.Event) error {
	if env.Type() != cloudevents.TypeTaskCompleted {
		return nil
	}

	var data taskCompletedData
	if err := env.DataAs(&data); err != nil {
		return err
	}

	outcome, err := c.applyTaskCompleted.Execute(ctx, usecases.ApplyTaskCompletedRequest{
		EventId:    env.ID(),
		WorkUnitId: data.WorkUnitId,
		OccurredAt: env.Time(),
	})
	if err != nil {
		return err
	}
	if outcome == usecases.TaskCompletedUnknownWorkUnit {
		c.logInfo(ctx, "TaskCompleted for a work unit this context never planned; skipping",
			"event_id", env.ID(), "work_unit_id", data.WorkUnitId,
			"task_id", data.TaskId, "task_type", data.TaskType)
	}
	return nil
}

// handleOrderManagementEvent filters for OrderAllocated / OrderPartially-
// Allocated — both event types share an identical payload shape and both
// mean "these lines are ready to enqueue", so this handler does not
// distinguish between them — and feeds the payload into
// ApplyOrderAllocated, which enqueues one WorkUnit per line through the
// existing EnqueueWorkUnit. This is the event-choreography replacement for
// order-management's former synchronous call to
// POST /paths/{pathId}/work-units.
//
// The processed-event mark and every line's enqueue commit atomically
// (ADR-0028): if any line fails, nothing is committed, the error is
// returned, and the event is retried and finally dead-lettered rather than
// silently dropped with its order released but never worked.
func (c *Consumer) handleOrderManagementEvent(ctx context.Context, env ce.Event) error {
	if env.Type() != cloudevents.TypeOrderAllocated && env.Type() != cloudevents.TypeOrderPartiallyAllocated {
		return nil
	}

	var data orderAllocatedData
	if err := env.DataAs(&data); err != nil {
		return err
	}

	lines := make([]usecases.OrderAllocatedLine, 0, len(data.Lines))
	for _, line := range data.Lines {
		lines = append(lines, usecases.OrderAllocatedLine{
			LineNo:   line.LineNo,
			SKU:      line.SKU,
			PathId:   line.PathId,
			GiftWrap: line.GiftWrap,
		})
	}
	_, err := c.applyOrderAllocated.Execute(ctx, usecases.ApplyOrderAllocatedRequest{
		EventId:     env.ID(),
		OccurredAt:  env.Time(),
		OrderId:     data.OrderId,
		PromiseDate: data.PromiseDate,
		Lines:       lines,
	})
	return err
}

// handleNetworkDemandEvent filters for WorkDemandReleased and feeds the
// payload into ApplyWorkDemandReleased, which enqueues one transfer work
// unit through the existing EnqueueWorkUnit under the deterministic id
// demand_id (ADR-0033) — the transfer-demand sibling of
// handleOrderManagementEvent.
//
// The processed-event mark and the enqueue commit atomically (ADR-0028):
// a failure commits nothing, the error is returned, and the event is
// retried and finally dead-lettered (e.g. an unknown path_id or an
// undeclared work_kind — deterministic poison) rather than silently
// dropped with its transfer demand released but never worked. A
// redelivery of the same CloudEvents id, or a different event carrying a
// demand_id already in the pool, is a benign no-op inside the use case.
func (c *Consumer) handleNetworkDemandEvent(ctx context.Context, env ce.Event) error {
	if env.Type() != cloudevents.TypeWorkDemandReleased {
		return nil
	}

	var data workDemandReleasedData
	if err := env.DataAs(&data); err != nil {
		return err
	}

	_, err := c.applyWorkDemandReleased.Execute(ctx, usecases.ApplyWorkDemandReleasedRequest{
		EventId:     env.ID(),
		OccurredAt:  env.Time(),
		DemandId:    data.DemandId,
		WorkKind:    workunit.WorkKind(data.WorkKind),
		TransferRef: data.TransferRef,
		PathId:      data.PathId,
		SiteId:      data.SiteId,
		SKU:         data.SKU,
		Quantity:    data.Quantity,
		CPT:         data.CPT,
	})
	return err
}

// handleSlottingEvent filters for SlotPlanApproved and feeds it into
// ApplySlotPlanApproved (ADR-0040), which plans one RELOCATE work unit per
// move with the CloudEvents id marked processed in the SAME atomic scope.
// SlotPlanGenerated, SlotPlanRejected and unknown types are ignored. A
// broken plan, an unknown RELOCATE path or an intake without
// SLOT_RELOCATION is returned as an error: dispatch retries it and then
// dead-letters it (deterministic poison); a message only shutdown
// cancellation interrupted is never dead-lettered (dispatch's ctx guard).
func (c *Consumer) handleSlottingEvent(ctx context.Context, env ce.Event) error {
	if env.Type() != cloudevents.TypeSlotPlanApproved {
		return nil
	}

	var data slotPlanApprovedData
	if err := env.DataAs(&data); err != nil {
		return err
	}
	moves := make([]usecases.SlotPlanMove, len(data.Moves))
	for i, m := range data.Moves {
		moves[i] = usecases.SlotPlanMove{SKU: m.SKU, Kind: m.Kind, FromSlot: m.FromSlot, ToSlot: m.ToSlot}
	}
	outcome, err := c.applySlotPlanApproved.Execute(ctx, usecases.ApplySlotPlanApprovedRequest{
		EventId:          env.ID(),
		OccurredAt:       env.Time(),
		PlanId:           data.PlanId,
		SiteId:           data.SiteId,
		ApprovedAt:       data.ApprovedAt,
		SupersedesPlanId: data.SupersedesPlanId,
		Moves:            moves,
	})
	if err != nil {
		return err
	}
	if outcome == usecases.SlotPlanStale {
		c.logInfo(ctx, "SlotPlanApproved older than the site's current plan; recorded, nothing planned",
			"event_id", env.ID(), "plan_id", data.PlanId, "site_id", data.SiteId)
	}
	return nil
}
