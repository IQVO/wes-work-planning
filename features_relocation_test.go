package main_test

// BDD steps for features/slot_plan_relocation.feature (ADR-0040). The
// SlotPlanApproved steps call ApplySlotPlanApproved — the use case the
// warehouse.slotting-optimization.events consumer calls — over the same
// in-memory repositories the REST router reads, so every Then is checked
// through the real REST surface.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/relocation"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

type slotPlanSteps struct {
	w        *world
	last     usecases.ApplySlotPlanApprovedRequest
	lastErr  error
	eventSeq int
}

func (s *slotPlanSteps) reset() {
	s.last = usecases.ApplySlotPlanApprovedRequest{}
	s.lastErr = nil
	s.eventSeq = 0
}

func (s *slotPlanSteps) approve(planId, siteId, at string, table *godog.Table) error {
	approvedAt, err := time.Parse(time.RFC3339, at)
	if err != nil {
		return fmt.Errorf("approved_at %q: %w", at, err)
	}
	moves := make([]usecases.SlotPlanMove, 0, len(table.Rows)-1)
	for _, row := range table.Rows[1:] {
		moves = append(moves, usecases.SlotPlanMove{
			SKU: row.Cells[0].Value, Kind: row.Cells[1].Value, FromSlot: row.Cells[2].Value, ToSlot: row.Cells[3].Value,
		})
	}
	s.eventSeq++
	s.last = usecases.ApplySlotPlanApprovedRequest{
		EventId: fmt.Sprintf("evt-slot-%d-%s", s.eventSeq, planId), OccurredAt: approvedAt,
		PlanId: planId, SiteId: siteId, ApprovedAt: approvedAt, Moves: moves,
	}
	return s.apply()
}

func (s *slotPlanSteps) apply() error {
	_, s.lastErr = s.w.h.inbound.slotPlan.Execute(context.Background(), s.last)
	if s.lastErr != nil && !isSlotPlanPoison(s.lastErr) {
		return s.lastErr
	}
	return nil
}

func (s *slotPlanSteps) redeliver() error { return s.apply() }

func (s *slotPlanSteps) refusedAsPoison() error {
	if s.lastErr == nil || !isSlotPlanPoison(s.lastErr) {
		return fmt.Errorf("want a deterministic refusal (dead-lettered by the consumer), got %v", s.lastErr)
	}
	return nil
}

func (s *slotPlanSteps) unitInState(id, state string) error {
	if err := s.w.requestWorkUnitById(id); err != nil {
		return err
	}
	return s.w.responseWorkUnitState(state)
}

func (s *slotPlanSteps) relocation() (map[string]any, error) {
	obj, err := s.w.decodeLast()
	if err != nil {
		return nil, err
	}
	rel, ok := obj["relocation"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("no relocation in %s", string(s.w.lastBody))
	}
	return rel, nil
}

func (s *slotPlanSteps) relocates(sku, from, to string, seq int, planId string) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	rel, err := s.relocation()
	if err != nil {
		return err
	}
	if obj["sku"] != sku || obj["pathId"] != usecases.RelocatePathId || rel["fromSlot"] != from || rel["toSlot"] != to ||
		rel["moveSeq"] != float64(seq) || rel["planId"] != planId {
		return fmt.Errorf("got %s", string(s.w.lastBody))
	}
	return nil
}

func (s *slotPlanSteps) waitsOn(blocker string) error {
	rel, err := s.relocation()
	if err != nil {
		return err
	}
	if rel["blockedBy"] != blocker {
		return fmt.Errorf("blockedBy = %v, want %s", rel["blockedBy"], blocker)
	}
	return nil
}

func registerSlotPlanSteps(sc *godog.ScenarioContext, w *world) {
	s := &slotPlanSteps{w: w}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		s.reset()
		return ctx, nil
	})
	sc.Step(`^slotting-optimization approves slot plan "([^"]*)" for site "([^"]*)" at "([^"]*)" with moves:$`, s.approve)
	sc.Step(`^the SlotPlanApproved event is redelivered$`, s.redeliver)
	sc.Step(`^the slot plan is refused as poison$`, s.refusedAsPoison)
	sc.Step(`^the RELOCATE WorkUnit "([^"]*)" is in state "([^"]*)"$`, s.unitInState)
	sc.Step(`^the WorkUnit in the response relocates SKU "([^"]*)" from "([^"]*)" to "([^"]*)" as move (\d+) of plan "([^"]*)"$`, s.relocates)
	sc.Step(`^the WorkUnit in the response waits on "([^"]*)"$`, s.waitsOn)
}

// isSlotPlanPoison is the deterministic refusals the consumer retries and
// then dead-letters: a malformed plan, an unknown move kind or move shape,
// or a RELOCATE path whose intake refuses SLOT_RELOCATION.
func isSlotPlanPoison(err error) bool {
	return errors.Is(err, workunit.ErrUnknownRelocationKind) ||
		errors.Is(err, workunit.ErrInvalidRelocationMove) ||
		errors.Is(err, relocation.ErrInvalidPlan) ||
		errors.Is(err, usecases.ErrPathRefusesSlotRelocation)
}
