package http

import (
	"fmt"

	"github.com/claudioed/wes-work-planning/internal/domain/charge"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/plan"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

func toBucketDTOs(buckets []charge.CPTBucket) []cptBucketDTO {
	out := make([]cptBucketDTO, len(buckets))
	for i, b := range buckets {
		cpt := b.CPT.Time()
		qty := b.Quantity.Value()
		out[i] = cptBucketDTO{CPT: &cpt, Quantity: &qty}
		if b.Load != nil {
			handles := b.Load.Handles
			out[i].SKU, out[i].PickUnit, out[i].Handles = b.SKU, b.Load.Unit.String(), &handles
			if b.Load.CubeKnown {
				cube := b.Load.CubeMm3
				out[i].CubeMm3 = &cube
			}
			if b.Load.WeightKnown {
				weight := b.Load.WeightG
				out[i].WeightG = &weight
			}
		}
	}
	return out
}

func toWorkUnitResponseDTO(unit *workunit.WorkUnit) workUnitResponseDTO {
	return workUnitResponseDTO{
		Id:          unit.Id(),
		PathId:      unit.PathId().String(),
		CPT:         unit.CPT().Time(),
		Reference:   unit.Reference(),
		State:       unit.State().String(),
		GiftWrap:    unit.GiftWrap(),
		SKU:         unit.SKU(),
		LineNo:      unit.LineNo(),
		ReleasedAt:  unit.ReleasedAt(),
		CompletedAt: unit.CompletedAt(),
		CancelledAt: unit.CancelledAt(),
		Relocation:  toRelocationDTO(unit),
		WorkKind:    packAwareKind(unit),
		Pick:        toPickDTO(unit),
		CaseBreak:   toCaseBreakDTO(unit),
	}
}

// packAwareKind is the unit's work kind when it is one of the pack-aware kinds
// (ADR-0045), "" for every other unit so existing responses are unchanged.
func packAwareKind(unit *workunit.WorkUnit) string {
	if k := unit.WorkKind(); k.IsPickKind() || k == workunit.WorkKindBreakCase {
		return k.String()
	}
	return ""
}

// toPickDTO is the pick plan of unit, nil for any unit that is not a pick.
func toPickDTO(unit *workunit.WorkUnit) *pickDTO {
	plan, ok := unit.Pick()
	if !ok {
		return nil
	}
	return &pickDTO{Quantity: plan.Quantity, Unit: plan.Unit.String(), FromBin: plan.FromBin}
}

// toCaseBreakDTO is the case break of unit, nil for any unit that is not a
// BREAK_CASE.
func toCaseBreakDTO(unit *workunit.WorkUnit) *caseBreakDTO {
	brk, ok := unit.CaseBreak()
	if !ok {
		return nil
	}
	return &caseBreakDTO{LPN: brk.LPN, LooseBin: brk.LooseBin, PickTaskId: brk.PickTaskId}
}

// pickUnitOf parses an optional wire pick unit: "" is "not stated".
func pickUnitOf(raw string) (workunit.PickUnit, error) {
	if raw == "" {
		return "", nil
	}
	return workunit.ParsePickUnit(raw)
}

// packAwareOf builds the validated pick plan or case break of an enqueue
// body, nil/nil for a plain one. Quantity, pickUnit and fromBin belong to a
// pick, caseBreak to a break; the two never mix and both need a sku.
func packAwareOf(body enqueueWorkUnitRequestDTO) (*workunit.PickPlan, *workunit.CaseBreak, error) {
	pickGiven := body.Quantity != nil || body.PickUnit != "" || body.FromBin != ""
	switch {
	case body.CaseBreak != nil && pickGiven:
		return nil, nil, fmt.Errorf("%w: caseBreak excludes quantity, pickUnit and fromBin", workunit.ErrInvalidCaseBreak)
	case body.CaseBreak != nil:
		brk, err := workunit.NewCaseBreak(body.CaseBreak.LPN, body.SKU, body.CaseBreak.LooseBin, body.CaseBreak.PickTaskId)
		if err != nil {
			return nil, nil, err
		}
		return nil, &brk, nil
	case pickGiven:
		if body.Quantity == nil {
			return nil, nil, fmt.Errorf("%w: quantity (eaches) is required with pickUnit or fromBin", workunit.ErrInvalidPickPlan)
		}
		unit := body.PickUnit
		if unit == "" {
			unit = workunit.PickUnitEach.String()
		}
		plan, err := workunit.NewPickPlan(body.SKU, *body.Quantity, unit, body.FromBin)
		if err != nil {
			return nil, nil, err
		}
		return &plan, nil, nil
	default:
		return nil, nil, nil
	}
}

// toRelocationDTO is the RELOCATE move of unit, nil for any other unit.
func toRelocationDTO(unit *workunit.WorkUnit) *relocationDTO {
	move, ok := unit.Relocation()
	if !ok {
		return nil
	}
	return &relocationDTO{
		PlanId:    move.PlanId,
		MoveSeq:   move.MoveSeq,
		Kind:      move.Kind.String(),
		SiteId:    unit.SiteId(),
		FromSlot:  move.FromSlot,
		ToSlot:    move.ToSlot,
		BlockedBy: unit.BlockedBy(),
	}
}

func toShiftPlanResponseDTO(p plan.PathPlan) shiftPlanResponseDTO {
	dto := shiftPlanResponseDTO{
		PathId:            p.PathId().String(),
		PlannedHeads:      p.PlannedHeads().Value(),
		InstalledStations: p.InstalledStations().Value(),
		RateUnitsPerHour:  p.Rate().UnitsPerHour(),
		Hours:             p.Hours(),
		PlannedThroughput: p.PlannedThroughput(),
	}
	if p.TravelDistanceKnown() {
		metresM := p.TravelDistanceM()
		estimated := p.TravelDistanceEstimated()
		dto.TravelDistanceM = &metresM
		dto.TravelDistanceEstimated = &estimated
	}
	if load, ok := p.HandleLoad(); ok {
		handle := &handleLoadDTO{PickUnit: load.Unit.String()}
		if load.WeightKnown {
			weight := load.WeightG
			handle.WeightG = &weight
		}
		if load.CubeKnown {
			cube := load.CubeMm3
			handle.CubeMm3 = &cube
		}
		if kgM, known := p.CarryKgM(); known {
			handle.CarryKgM = &kgM
		}
		dto.Handle = handle
	}
	return dto
}

func toLaborPlanViewDTO(view laborview.LaborPlanObserved) *laborPlanViewDTO {
	return &laborPlanViewDTO{
		PathId:       view.PathId.String(),
		PlannedHeads: view.PlannedHeads,
		PlannedRate:  view.PlannedRate,
		PlannedHours: view.PlannedHours,
		ObservedAt:   view.ObservedAt,

		DriftHeads:      view.DriftHeads,
		DriftDetectedAt: view.DriftDetectedAt,
	}
}
