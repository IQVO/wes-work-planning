package relocation

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

var approvedAt = time.Date(2026, 10, 15, 8, 0, 0, 0, time.UTC)

func mustPlan(t *testing.T, id string, moves ...MoveInput) *Plan {
	t.Helper()
	p, err := NewPlan(id, "SITE-1", approvedAt, "", moves)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	return p
}

func relocate(sku, from, to string) MoveInput {
	return MoveInput{SKU: sku, Kind: "Relocate", FromSlot: from, ToSlot: to}
}

func assign(sku, to string) MoveInput { return MoveInput{SKU: sku, Kind: "Assign", ToSlot: to} }

func vacate(sku, from string) MoveInput { return MoveInput{SKU: sku, Kind: "Vacate", FromSlot: from} }

func TestNewPlan_BuildsOneMovePerInputInOrder(t *testing.T) {
	p, err := NewPlan(" plan-1 ", " SITE-1 ", approvedAt, "plan-0", []MoveInput{
		assign("SKU-1", "S1"), relocate("SKU-2", "S9", "S2"), vacate("SKU-3", "S3"),
	})
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	if p.Id() != "plan-1" || p.SiteId() != "SITE-1" || !p.ApprovedAt().Equal(approvedAt) || p.SupersedesPlanId() != "plan-0" {
		t.Fatalf("plan = %s %s %v %s", p.Id(), p.SiteId(), p.ApprovedAt(), p.SupersedesPlanId())
	}
	assertMovesNumbered(t, p.Moves(), "plan-1", 3)
	assertMove(t, p.Moves()[1], "SKU-2", workunit.RelocationKindRelocate, "S9", "S2")
	assertReceipt(t, p.Receipt(), Receipt{PlanId: "plan-1", SiteId: "SITE-1", ApprovedAt: approvedAt, SupersedesPlanId: "plan-0", MoveCount: 3})
}

func assertMovesNumbered(t *testing.T, moves []Move, planId string, n int) {
	t.Helper()
	if len(moves) != n {
		t.Fatalf("moves = %d, want %d", len(moves), n)
	}
	for i, m := range moves {
		if m.Move.MoveSeq != i || m.Move.PlanId != planId {
			t.Fatalf("move %d = %+v", i, m)
		}
	}
}

func assertMove(t *testing.T, m Move, sku string, kind workunit.RelocationKind, from, to string) {
	t.Helper()
	if m.SKU != sku || m.Move.Kind != kind || m.Move.FromSlot != from || m.Move.ToSlot != to {
		t.Fatalf("move = %+v", m)
	}
}

func assertReceipt(t *testing.T, got, want Receipt) {
	t.Helper()
	if got.PlanId != want.PlanId || got.SiteId != want.SiteId || !got.ApprovedAt.Equal(want.ApprovedAt) ||
		got.SupersedesPlanId != want.SupersedesPlanId || got.MoveCount != want.MoveCount {
		t.Fatalf("receipt = %+v, want %+v", got, want)
	}
}

func TestNewPlan_RejectsBrokenPlans(t *testing.T) {
	cases := []struct {
		name     string
		id, site string
		at       time.Time
		moves    []MoveInput
		want     error
	}{
		{"no plan id", " ", "S", approvedAt, nil, ErrInvalidPlan},
		{"no site", "p", "", approvedAt, nil, ErrInvalidPlan},
		{"no approval time", "p", "S", time.Time{}, nil, ErrInvalidPlan},
		{"move without sku", "p", "S", approvedAt, []MoveInput{{SKU: " ", Kind: "Assign", ToSlot: "A"}}, ErrInvalidPlan},
		{"unknown kind", "p", "S", approvedAt, []MoveInput{{SKU: "x", Kind: "Swap", ToSlot: "A"}}, workunit.ErrUnknownRelocationKind},
		{"vacate without slot", "p", "S", approvedAt, []MoveInput{vacate("x", "")}, workunit.ErrInvalidRelocationMove},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPlan(tc.id, tc.site, tc.at, "", tc.moves); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestNewPlan_EmptyMovesIsValid(t *testing.T) {
	p := mustPlan(t, "plan-empty")
	if len(p.Moves()) != 0 || len(p.Predecessors()) != 0 {
		t.Fatal("a plan with no moves plans nothing")
	}
}

func TestWorkUnitId(t *testing.T) {
	if got := WorkUnitId("plan-1", 0); got != "plan-1-move-0" {
		t.Fatalf("WorkUnitId = %q", got)
	}
	if got := WorkUnitId("plan-1", 12); got != "plan-1-move-12" {
		t.Fatalf("WorkUnitId = %q", got)
	}
}

func TestPredecessors_MoveIntoAVacatedSlotWaits(t *testing.T) {
	// move 0 vacates S1; move 1 relocates SKU-2 from S2 into S1; move 2
	// assigns SKU-3 into S2 (vacated by move 1); move 3 is independent.
	p := mustPlan(t, "plan-1",
		vacate("SKU-1", "S1"),
		relocate("SKU-2", "S2", "S1"),
		assign("SKU-3", "S2"),
		assign("SKU-4", "S7"),
	)
	got := p.Predecessors()
	want := map[int]int{1: 0, 2: 1}
	assertPredecessors(t, got, want)
}

func TestPredecessors_DeclaredOrderDoesNotMatter(t *testing.T) {
	// The vacating move comes AFTER the move that needs its slot.
	p := mustPlan(t, "plan-1",
		assign("SKU-3", "S2"),
		relocate("SKU-2", "S2x", "S9"),
		vacate("SKU-9", "S2"),
	)
	assertPredecessors(t, p.Predecessors(), map[int]int{0: 2})
}

func TestPredecessors_LowestSeqVacaterWins(t *testing.T) {
	p := mustPlan(t, "plan-1",
		assign("SKU-1", "S1"),
		vacate("SKU-8", "S1"),
		vacate("SKU-9", "S1"),
	)
	assertPredecessors(t, p.Predecessors(), map[int]int{0: 1})
}

func TestPredecessors_SwapCycleIsBrokenAtLowestSeq(t *testing.T) {
	p := mustPlan(t, "plan-1",
		assign("SKU-0", "S5"),
		relocate("SKU-1", "S1", "S2"),
		relocate("SKU-2", "S2", "S1"),
	)
	// 1 waits on 2 and 2 waits on 1: the cycle breaks at 1.
	assertPredecessors(t, p.Predecessors(), map[int]int{2: 1})
}

func TestPredecessors_ThreeCycleWithATail(t *testing.T) {
	p := mustPlan(t, "plan-1",
		assign("SKU-T", "S3"),         // 0 waits on whoever vacates S3 (move 3)
		relocate("SKU-A", "S1", "S2"), // 1 waits on 2 (vacates S2)
		relocate("SKU-B", "S2", "S3"), // 2 waits on 3 (vacates S3)
		relocate("SKU-C", "S3", "S1"), // 3 waits on 1 (vacates S1)
	)
	// Cycle 1 -> 2 -> 3 -> 1 breaks at 1; the tail 0 -> 3 stays.
	assertPredecessors(t, p.Predecessors(), map[int]int{0: 3, 2: 3, 3: 1})
}

func TestPredecessors_ReturnsAFreshMap(t *testing.T) {
	p := mustPlan(t, "plan-1", vacate("SKU-1", "S1"), assign("SKU-2", "S1"))
	first := p.Predecessors()
	first[0] = 99
	assertPredecessors(t, p.Predecessors(), map[int]int{1: 0})
}

func assertPredecessors(t *testing.T, got, want map[int]int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("predecessors = %v, want %v", got, want)
	}
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			t.Fatalf("predecessors = %v, want %v", got, want)
		}
	}
}

func receipt(id string, at time.Time) Receipt {
	return Receipt{PlanId: id, SiteId: "SITE-1", ApprovedAt: at}
}

func TestDecide(t *testing.T) {
	later := approvedAt.Add(time.Hour)
	cases := []struct {
		name     string
		incoming Receipt
		latest   *Receipt
		want     Decision
	}{
		{"first plan of the site", receipt("p1", approvedAt), nil, Apply},
		{"same plan again", receipt("p1", approvedAt), ptr(receipt("p1", approvedAt)), AlreadyApplied},
		{"newer plan supersedes", receipt("p2", later), ptr(receipt("p1", approvedAt)), Apply},
		{"older plan is stale", receipt("p1", approvedAt), ptr(receipt("p2", later)), Stale},
		{"tie: greater plan id wins", receipt("p2", approvedAt), ptr(receipt("p1", approvedAt)), Apply},
		{"tie: smaller plan id is stale", receipt("p1", approvedAt), ptr(receipt("p2", approvedAt)), Stale},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Decide(tc.incoming, tc.latest); got != tc.want {
				t.Fatalf("Decide = %v, want %v", got, tc.want)
			}
		})
	}
}

func ptr(r Receipt) *Receipt { return &r }

func TestSupersedes_SameReceiptDoesNotSupersedeItself(t *testing.T) {
	r := receipt("p1", approvedAt)
	if r.Supersedes(r) {
		t.Fatal("a plan never supersedes itself")
	}
}

func TestPredecessors_TailIntoAnAlreadyWalkedChain(t *testing.T) {
	// 0 waits on 1, 1 waits on 2; walking from 0 finishes the chain, and
	// move 3 (also into S2's vacater's slot chain) reaches a finished node.
	p := mustPlan(t, "plan-1",
		relocate("SKU-0", "S0", "S1"),
		relocate("SKU-1", "S1", "S2"),
		vacate("SKU-2", "S2"),
		assign("SKU-3", "S0"),
	)
	assertPredecessors(t, p.Predecessors(), map[int]int{0: 1, 1: 2, 3: 0})
}
