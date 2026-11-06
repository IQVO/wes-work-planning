package usecases

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/pickload"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// defaultWIPLimit and defaultAlarmThreshold seed a path's WorkPool the first
// time work is enqueued against it, if no pool has been provisioned yet.
const (
	defaultWIPLimit       = 1000
	defaultAlarmThreshold = 1000
)

// EnqueueWorkUnit adds a releasable unit of work to a path's work pool.
type EnqueueWorkUnit struct {
	workUnits ports.WorkUnitRepo
	pools     ports.WorkPoolRepo
	publisher ports.EventPublisher
	clock     ports.Clock
	uow       ports.UnitOfWork
	packs     ports.ProductClassificationLookup
	catalogue ports.PathCatalogue
	pickFaces ports.ForwardPickFaceLookup
}

func NewEnqueueWorkUnit(workUnits ports.WorkUnitRepo, pools ports.WorkPoolRepo, publisher ports.EventPublisher, clock ports.Clock) *EnqueueWorkUnit {
	return &EnqueueWorkUnit{workUnits: workUnits, pools: pools, publisher: publisher, clock: clock}
}

// WithUnitOfWork brackets both Saves + Publish in one atomic scope
// (ADR-0014). Optional: nil keeps the calls running back to back.
func (uc *EnqueueWorkUnit) WithUnitOfWork(u ports.UnitOfWork) *EnqueueWorkUnit {
	uc.uow = u
	return uc
}

// WithPickFaces wires the local slot-plan copy's forward-pick faces
// (ADR-0048). Optional: without it, a pick plan whose caller stated no
// FromBin is enqueued as-is (omit-when-unknown, fail-open).
func (uc *EnqueueWorkUnit) WithPickFaces(f ports.ForwardPickFaceLookup) *EnqueueWorkUnit {
	uc.pickFaces = f
	return uc
}

// WithPackLookup wires the local pack copy (ADR-0045) used to verify a CASE
// pick against the SKU's active CASE level. Optional: without it a CASE pick
// is refused (it cannot be verified), every other request is unaffected.
func (uc *EnqueueWorkUnit) WithPackLookup(l ports.ProductClassificationLookup) *EnqueueWorkUnit {
	uc.packs = l
	return uc
}

// WithCatalogue wires the process-path catalogue so a pick plan is accepted
// only on a path whose canonical id is PICK, and a case break only (and
// always) on BREAK_CASE: fulfillment-execution derives a task's type from
// the released path and dead-letters a mismatched pairing (its ADR 0054).
// Optional: nil skips the check.
func (uc *EnqueueWorkUnit) WithCatalogue(c ports.PathCatalogue) *EnqueueWorkUnit {
	uc.catalogue = c
	return uc
}

// Canonical process-path ids fulfillment-execution maps to task types.
const (
	pickCanonicalPathId      = "PICK"
	breakCaseCanonicalPathId = workunit.BreakCasePathId
)

type EnqueueWorkUnitRequest struct {
	WorkUnitId string
	PathId     shared.PathId
	CPT        shared.CPT
	Reference  string
	// SKU is optional: the inventory SKU this order line corresponds to,
	// if known. Threaded through to the WorkUnit so the outbound
	// WorkReleased publisher can look up its ProductClassification once,
	// at release time, and stamp derived hazmat/fragile hints onto the
	// published event (see ADR-0009). Empty is a valid value — not every
	// caller knows a SKU.
	SKU string
	// GiftWrap is optional: whether the requester asked the warehouse to
	// produce a gift package for this work unit, stated at enqueue time.
	// Threaded through to the WorkUnit so the outbound WorkReleased
	// publisher can stamp it directly onto the published event's data
	// payload (see ADR-0010) — unlike SKU, this is never looked up from
	// another service; it is exactly what the caller supplied. False by
	// default — not every caller requests gift wrap.
	GiftWrap bool
	// TransferRef, WorkKind and Quantity are the optional transfer
	// metadata of a network-inventory-planning work demand (ADR-0033),
	// threaded through to the WorkUnit so the outbound WorkReleased
	// publisher can stamp them onto the published event at release time.
	// Zero values mean "not transfer work" — the fields are additive,
	// exactly like SKU/GiftWrap, never new invariants.
	TransferRef string
	WorkKind    workunit.WorkKind
	// SiteId is the optional site of the work (ADR-0048): a transfer
	// demand's anchor site (ADR-0033), an order line's fulfillment site,
	// a slot plan's site (ADR-0040) or a replenishment request's site
	// (ADR-0042). Threaded through to the WorkUnit so the outbound
	// WorkReleased publisher can stamp it at release time; "" means none
	// known — additive, never a new invariant.
	SiteId   string
	Quantity int
	// LineNo is optional: the 1-based order line this work unit is for
	// (ADR-0036). 0 means "unknown"; a negative value or one above
	// workunit.MaxLineNo (2147483647) is rejected with
	// workunit.ErrInvalidLineNo. Threaded through to the WorkUnit so the
	// outbound WorkReleased publisher can stamp line_no at release time. It
	// never alters WorkUnitId.
	LineNo int
	// Pick is the optional pick plan of a CASE_PICK / EACH_PICK unit
	// (ADR-0045), built by workunit.NewPickPlan. Its quantity is in eaches.
	// It replaces SKU and Quantity on the unit.
	Pick *workunit.PickPlan
	// CaseBreak is the optional sealed-case break of a BREAK_CASE unit
	// (ADR-0045), built by workunit.NewCaseBreak. Mutually exclusive with
	// Pick.
	CaseBreak *workunit.CaseBreak
}

func (uc *EnqueueWorkUnit) Execute(ctx context.Context, req EnqueueWorkUnitRequest) (*workunit.WorkUnit, error) {
	var unit *workunit.WorkUnit
	err := retryOnPoolConflict(ctx, func(ctx context.Context) error {
		var err error
		unit, err = uc.enqueueOnce(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return unit, nil
}

// enqueueOnce is one optimistic attempt: load (or create) the pool, add
// the entry, and save the POOL FIRST so a lost race fails before anything
// else is written -- the retry then starts clean even without a
// transactional UnitOfWork.
func (uc *EnqueueWorkUnit) enqueueOnce(ctx context.Context, req EnqueueWorkUnitRequest) (*workunit.WorkUnit, error) {
	packOpts, err := uc.packAwareOptions(ctx, req)
	if err != nil {
		return nil, err
	}
	// The unit is complete and valid at construction: every optional
	// attribute is a functional option, validated on the way in.
	opts := []workunit.Option{
		workunit.WithLineNo(req.LineNo),
		workunit.WithSKU(req.SKU),
		workunit.WithGiftWrap(req.GiftWrap),
	}
	// The optional site (ADR-0048) applies to EVERY work kind whose
	// request carries one — order units (from OrderAllocated's additive
	// site_id) exactly like transfer legs, RELOCATE and REPLENISH.
	if req.SiteId != "" {
		opts = append(opts, workunit.WithSiteId(req.SiteId))
	}
	if req.WorkKind != "" {
		opts = append(opts, workunit.WithTransferLeg(req.TransferRef, req.WorkKind, req.SiteId, req.Quantity))
	}
	opts = append(opts, packOpts...)
	unit, err := workunit.NewWorkUnit(req.WorkUnitId, req.PathId, req.CPT, req.Reference, opts...)
	if err != nil {
		return nil, err
	}

	pool, err := uc.pools.FindByPathId(ctx, req.PathId)
	if errors.Is(err, ports.ErrNotFound) {
		pool = release.NewWorkPool(req.PathId, release.ReleaseFed, defaultWIPLimit, defaultAlarmThreshold)
	} else if err != nil {
		return nil, err
	}
	if err := pool.Enqueue(unit.Id(), unit.CPT()); err != nil {
		return nil, err
	}

	event := shared.NewWorkUnitCreated(unit.Id(), req.PathId, uc.clock.Now()).WithOrigin(unit.Reference(), unit.LineNo())
	err = atomically(ctx, uc.uow, func(ctx context.Context) error {
		if err := uc.pools.Save(ctx, pool); err != nil {
			return err
		}
		if err := uc.workUnits.Save(ctx, unit); err != nil {
			return err
		}
		return uc.publisher.Publish(ctx, event)
	})
	if err != nil {
		return nil, err
	}
	return unit, nil
}

// deriveForwardPickFace fills a pick plan's FromBin with the SKU's
// forward-pick face from the local slot-plan copy (ADR-0048) at ENQUEUE
// time — the unit is immutable after construction (PR #261), so the face
// is resolved before NewWorkUnit. It never overrides a caller-stated bin
// and treats every miss, unwired port or lookup error as "no face"
// (omit-when-unknown, fail-open): the enqueue proceeds exactly as it
// would have without it.
func (uc *EnqueueWorkUnit) deriveForwardPickFace(ctx context.Context, siteId string, plan workunit.PickPlan) workunit.PickPlan {
	if plan.FromBin != "" || plan.SKU == "" || uc.pickFaces == nil {
		return plan
	}
	face, err := uc.pickFaces.ForwardPickFaceBySKU(ctx, siteId, plan.SKU)
	if err != nil || face == "" {
		return plan
	}
	plan.FromBin = face
	return plan
}

// packAwareOptions validates the pack-aware parts of req (ADR-0045) and
// returns them as construction options: a pick plan makes the unit
// CASE_PICK / EACH_PICK, a case break makes it BREAK_CASE. A request with
// neither carries no pack option, except that a BREAK_CASE path with no
// case break is refused (it could never be executed).
func (uc *EnqueueWorkUnit) packAwareOptions(ctx context.Context, req EnqueueWorkUnitRequest) ([]workunit.Option, error) {
	if req.Pick != nil && req.CaseBreak != nil {
		return nil, fmt.Errorf("%w: a work unit is either a pick or a case break", workunit.ErrInvalidPickPlan)
	}
	canonical, err := uc.canonicalPathId(req.PathId)
	if err != nil {
		// A plain request keeps its pre-ADR-0045 behaviour (the callers
		// validate the path themselves); only pack-aware work needs it.
		if req.Pick == nil && req.CaseBreak == nil {
			return nil, nil
		}
		return nil, err
	}
	onBreakCase := strings.EqualFold(canonical, breakCaseCanonicalPathId)
	switch {
	case req.CaseBreak != nil:
		if uc.catalogue != nil && !onBreakCase {
			return nil, fmt.Errorf("%w: a case break is released on the %s path, not %q", workunit.ErrInvalidCaseBreak, breakCaseCanonicalPathId, req.PathId.String())
		}
		return []workunit.Option{workunit.WithCaseBreak(*req.CaseBreak)}, nil
	case req.Pick != nil:
		if uc.catalogue != nil && !strings.EqualFold(canonical, pickCanonicalPathId) {
			return nil, fmt.Errorf("%w: a pick plan is released on the %s path, not %q", workunit.ErrInvalidPickPlan, pickCanonicalPathId, req.PathId.String())
		}
		if req.Pick.Unit == workunit.PickUnitCase {
			if err := uc.verifyCasePick(ctx, *req.Pick); err != nil {
				return nil, err
			}
		}
		plan := uc.deriveForwardPickFace(ctx, req.SiteId, *req.Pick)
		return []workunit.Option{workunit.WithPick(plan)}, nil
	case onBreakCase:
		return nil, fmt.Errorf("%w: work on the %s path carries a case break", workunit.ErrInvalidCaseBreak, breakCaseCanonicalPathId)
	}
	return nil, nil
}

// canonicalPathId resolves the catalogue's canonical id of pathId; "" when no
// catalogue is wired. An unknown path is the catalogue's own error.
func (uc *EnqueueWorkUnit) canonicalPathId(pathId shared.PathId) (string, error) {
	if uc.catalogue == nil {
		return "", nil
	}
	def, err := uc.catalogue.Lookup(pathId.String())
	if err != nil {
		return "", err
	}
	return def.Id, nil
}

// verifyCasePick refuses a CASE pick the local pack copy cannot back: no
// active CASE level for the SKU, or a quantity that is not a whole number of
// cases. Both are the caller's to fix (422); an EACH pick needs no pack data.
func (uc *EnqueueWorkUnit) verifyCasePick(ctx context.Context, plan workunit.PickPlan) error {
	if uc.packs == nil {
		return fmt.Errorf("%w: %s (no pack lookup wired)", pickload.ErrNoActiveCaseLevel, plan.SKU)
	}
	view, err := uc.packs.GetClassification(ctx, plan.SKU)
	if err != nil {
		return err
	}
	_, err = pickload.Estimate(workunit.PickUnitCase, plan.Quantity, view)
	return err
}
