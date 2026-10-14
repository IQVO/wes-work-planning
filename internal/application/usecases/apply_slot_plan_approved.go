package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/relocation"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// RelocatePathId is the process path every slot-plan move is released on
// (ADR-0040): fulfillment-execution derives its RELOCATE task type from it.
const RelocatePathId = "RELOCATE"

// DefaultRelocationCPTHorizon is how far after approval a slot-plan move's
// CPT lies when RELOCATION_CPT_HORIZON is not set (ADR-0040).
const DefaultRelocationCPTHorizon = 24 * time.Hour

// ErrPathRefusesSlotRelocation: the RELOCATE catalogue entry declares an
// intake without SLOT_RELOCATION (process-path-management ADR 0034, fleet
// decision E5). Deterministic: the plan is dead-lettered.
var ErrPathRefusesSlotRelocation = errors.New("process path intake does not admit SLOT_RELOCATION")

// SlotPlanOutcome reports what ApplySlotPlanApproved did with one event.
type SlotPlanOutcome int

const (
	// SlotPlanApplied: the plan's moves were planned (and the site's older
	// plans' unreleased moves cancelled).
	SlotPlanApplied SlotPlanOutcome = iota
	// SlotPlanAlreadyProcessed: a redelivery of the same CloudEvents id.
	SlotPlanAlreadyProcessed
	// SlotPlanAlreadyApplied: the same plan under another event id.
	SlotPlanAlreadyApplied
	// SlotPlanStale: the site already has a newer plan; recorded, nothing
	// planned.
	SlotPlanStale
)

// SlotPlanMove is one move of SlotPlanApproved, as published.
type SlotPlanMove struct {
	SKU      string
	Kind     string
	FromSlot string
	ToSlot   string
}

// ApplySlotPlanApprovedRequest is one SlotPlanApproved occurrence.
type ApplySlotPlanApprovedRequest struct {
	EventId          string
	OccurredAt       time.Time
	PlanId           string
	SiteId           string
	ApprovedAt       time.Time
	SupersedesPlanId string
	Moves            []SlotPlanMove
}

// ApplySlotPlanApproved is the inbound-event use case behind
// slotting-optimization's SlotPlanApproved (ADR-0040): one RELOCATE work
// unit per move, sequenced and superseded per site, with the processed-event
// mark and every write in ONE atomic scope (ADR-0028).
type ApplySlotPlanApproved struct {
	workUnits   ports.WorkUnitRepo
	relocations ports.RelocationWorkRepo
	pools       ports.WorkPoolRepo
	plans       ports.SlotPlanRepo
	processed   ports.ProcessedEventRepo
	catalogue   ports.PathCatalogue
	publisher   ports.EventPublisher
	clock       ports.Clock
	uow         ports.UnitOfWork
	horizon     time.Duration
}

func NewApplySlotPlanApproved(workUnits ports.WorkUnitRepo, relocations ports.RelocationWorkRepo, pools ports.WorkPoolRepo, plans ports.SlotPlanRepo, processed ports.ProcessedEventRepo, catalogue ports.PathCatalogue, publisher ports.EventPublisher, clock ports.Clock) *ApplySlotPlanApproved {
	return &ApplySlotPlanApproved{
		workUnits:   workUnits,
		relocations: relocations,
		pools:       pools,
		plans:       plans,
		processed:   processed,
		catalogue:   catalogue,
		publisher:   publisher,
		clock:       clock,
		horizon:     DefaultRelocationCPTHorizon,
	}
}

// WithUnitOfWork brackets the processed-event mark and every write in one
// atomic scope (ADR-0014).
func (uc *ApplySlotPlanApproved) WithUnitOfWork(u ports.UnitOfWork) *ApplySlotPlanApproved {
	uc.uow = u
	return uc
}

// WithCPTHorizon sets how far after approval a move's CPT lies
// (RELOCATION_CPT_HORIZON). A non-positive value keeps the current one.
func (uc *ApplySlotPlanApproved) WithCPTHorizon(h time.Duration) *ApplySlotPlanApproved {
	if h > 0 {
		uc.horizon = h
	}
	return uc
}

// Execute applies one SlotPlanApproved. A broken plan, an unknown RELOCATE
// path or an intake without SLOT_RELOCATION is returned as an error with
// nothing committed (the consumer retries, then dead-letters it).
func (uc *ApplySlotPlanApproved) Execute(ctx context.Context, req ApplySlotPlanApprovedRequest) (SlotPlanOutcome, error) {
	outcome := SlotPlanApplied
	already, err := onceAtomically(ctx, uc.uow, uc.processed, req.EventId, req.OccurredAt, func(ctx context.Context) error {
		plan, err := relocation.NewPlan(req.PlanId, req.SiteId, req.ApprovedAt, req.SupersedesPlanId, moveInputs(req.Moves))
		if err != nil {
			return err
		}
		pathId, err := uc.relocatePath()
		if err != nil {
			return err
		}
		return retryOnPoolConflict(ctx, func(ctx context.Context) error {
			var err error
			outcome, err = uc.applyOnce(ctx, plan, pathId)
			return err
		})
	})
	if err != nil {
		return SlotPlanApplied, err
	}
	if already {
		return SlotPlanAlreadyProcessed, nil
	}
	return outcome, nil
}

func moveInputs(moves []SlotPlanMove) []relocation.MoveInput {
	out := make([]relocation.MoveInput, len(moves))
	for i, m := range moves {
		out[i] = relocation.MoveInput{SKU: m.SKU, Kind: m.Kind, FromSlot: m.FromSlot, ToSlot: m.ToSlot}
	}
	return out
}

// relocatePath validates RELOCATE against the catalogue (ADR-0012) and its
// declared intake (E5).
func (uc *ApplySlotPlanApproved) relocatePath() (shared.PathId, error) {
	def, err := uc.catalogue.Lookup(RelocatePathId)
	if err != nil {
		return shared.PathId{}, err
	}
	if !def.Admits(pathcatalog.IntakeSlotRelocation) {
		return shared.PathId{}, fmt.Errorf("%w: path %s intake %v", ErrPathRefusesSlotRelocation, def.Id, def.Intake)
	}
	return shared.NewPathId(RelocatePathId)
}

// applyOnce is one optimistic attempt: decide against the site's latest
// plan, then cancel the older plans' unreleased moves and plan this one.
// The pool is saved FIRST so a lost race fails before anything else is
// written.
func (uc *ApplySlotPlanApproved) applyOnce(ctx context.Context, plan *relocation.Plan, pathId shared.PathId) (SlotPlanOutcome, error) {
	receipt := plan.Receipt()
	latest, err := uc.latestAtSite(ctx, plan.SiteId())
	if err != nil {
		return SlotPlanApplied, err
	}
	decision := relocation.Decide(receipt, latest)
	if decision == relocation.AlreadyApplied {
		return SlotPlanAlreadyApplied, nil
	}
	if decision == relocation.Stale {
		return SlotPlanStale, uc.plans.Save(ctx, receipt)
	}
	pool, err := loadOrNewPool(ctx, uc.pools, pathId)
	if err != nil {
		return SlotPlanApplied, err
	}
	now := uc.clock.Now()
	cancelled, err := uc.cancelSuperseded(ctx, pool, plan, now)
	if err != nil {
		return SlotPlanApplied, err
	}
	units, err := uc.planUnits(pool, plan, pathId)
	if err != nil {
		return SlotPlanApplied, err
	}
	return SlotPlanApplied, uc.save(ctx, pool, append(cancelled, units...), units, receipt, now)
}

func (uc *ApplySlotPlanApproved) latestAtSite(ctx context.Context, siteId string) (*relocation.Receipt, error) {
	latest, err := uc.plans.LatestAtSite(ctx, siteId)
	if errors.Is(err, ports.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &latest, nil
}

func loadOrNewPool(ctx context.Context, pools ports.WorkPoolRepo, pathId shared.PathId) (*release.WorkPool, error) {
	pool, err := pools.FindByPathId(ctx, pathId)
	if errors.Is(err, ports.ErrNotFound) {
		return release.NewWorkPool(pathId, release.ReleaseFed, defaultWIPLimit, defaultAlarmThreshold), nil
	}
	return pool, err
}

// cancelSuperseded cancels every Pending RELOCATE unit of the plan's site
// that belongs to another plan, withdrawing its pool entry (a blocked unit
// has none). Released and Completed units are never returned by the
// repository query, so they run to completion.
func (uc *ApplySlotPlanApproved) cancelSuperseded(ctx context.Context, pool *release.WorkPool, plan *relocation.Plan, now time.Time) ([]*workunit.WorkUnit, error) {
	pending, err := uc.relocations.PendingRelocationsAtSite(ctx, plan.SiteId())
	if err != nil {
		return nil, err
	}
	var cancelled []*workunit.WorkUnit
	for _, u := range pending {
		if m, ok := u.Relocation(); ok && m.PlanId == plan.Id() {
			continue
		}
		if err := u.Cancel(now); err != nil {
			return nil, err
		}
		if err := pool.Withdraw(u.Id()); err != nil && !errors.Is(err, release.ErrUnknownEntry) {
			return nil, err
		}
		cancelled = append(cancelled, u)
	}
	return cancelled, nil
}

// planUnits builds one RELOCATE unit per move; a move with a predecessor is
// blocked and stays out of the pool, every other one is enqueued.
func (uc *ApplySlotPlanApproved) planUnits(pool *release.WorkPool, plan *relocation.Plan, pathId shared.PathId) ([]*workunit.WorkUnit, error) {
	cpt := shared.NewCPT(plan.ApprovedAt().Add(uc.horizon))
	pred := plan.Predecessors()
	units := make([]*workunit.WorkUnit, 0, len(plan.Moves()))
	for _, m := range plan.Moves() {
		unit, err := workunit.NewWorkUnit(relocation.WorkUnitId(plan.Id(), m.Move.MoveSeq), pathId, cpt, plan.Id())
		if err != nil {
			return nil, err
		}
		unit.SetWorkKind(workunit.WorkKindRelocate)
		unit.SetSiteId(plan.SiteId())
		unit.SetSKU(m.SKU)
		unit.SetRelocation(m.Move)
		if a, waits := pred[m.Move.MoveSeq]; waits {
			unit.SetBlockedBy(relocation.WorkUnitId(plan.Id(), a))
		} else if err := pool.Enqueue(unit.Id(), cpt); err != nil {
			return nil, err
		}
		units = append(units, unit)
	}
	return units, nil
}

// save writes the pool first, then every touched unit, the plan receipt and
// one WorkUnitCreated per planned unit, all in one atomic scope.
func (uc *ApplySlotPlanApproved) save(ctx context.Context, pool *release.WorkPool, touched, created []*workunit.WorkUnit, receipt relocation.Receipt, now time.Time) error {
	events := make([]shared.DomainEvent, 0, len(created))
	for _, u := range created {
		events = append(events, shared.NewWorkUnitCreated(u.Id(), u.PathId(), now))
	}
	return atomically(ctx, uc.uow, func(ctx context.Context) error {
		if err := uc.pools.Save(ctx, pool); err != nil {
			return err
		}
		for _, u := range touched {
			if err := uc.workUnits.Save(ctx, u); err != nil {
				return err
			}
		}
		if err := uc.plans.Save(ctx, receipt); err != nil {
			return err
		}
		return uc.publisher.Publish(ctx, events...)
	})
}
