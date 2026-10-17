package workunit_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0042: a REPLENISH work unit carries one validated pick-face
// replenishment; the move is the single source of the unit's SKU and
// quantity.

func TestParseReplenishmentPriority(t *testing.T) {
	for _, want := range []workunit.ReplenishmentPriority{workunit.ReplenishmentPriorityNormal, workunit.ReplenishmentPriorityUrgent} {
		got, err := workunit.ParseReplenishmentPriority(want.String())
		if err != nil || got != want {
			t.Fatalf("ParseReplenishmentPriority(%q) = %q, %v", want, got, err)
		}
	}
	for _, bad := range []string{"", "normal", "Urgent", "HIGH", " URGENT"} {
		if _, err := workunit.ParseReplenishmentPriority(bad); !errors.Is(err, workunit.ErrUnknownReplenishmentPriority) {
			t.Fatalf("ParseReplenishmentPriority(%q) error = %v, want ErrUnknownReplenishmentPriority", bad, err)
		}
	}
}

func TestNewReplenishmentMove_BuildsATrimmedMove(t *testing.T) {
	m, err := workunit.NewReplenishmentMove(" repl-1 ", " SKU-1 ", " RSV-1 ", " FWD-1 ", 24, " URGENT ")
	if err != nil {
		t.Fatalf("NewReplenishmentMove: %v", err)
	}
	want := workunit.ReplenishmentMove{RequestId: "repl-1", SKU: "SKU-1", FromBin: "RSV-1", ToBin: "FWD-1", Quantity: 24, Priority: workunit.ReplenishmentPriorityUrgent}
	if m != want {
		t.Fatalf("move = %+v, want %+v", m, want)
	}
}

func TestNewReplenishmentMove_RejectsEveryBrokenInvariant(t *testing.T) {
	long := strings.Repeat("r", workunit.MaxReplenishmentRequestIdLen+1)
	cases := []struct {
		name                         string
		request, sku, from, to, prio string
		qty                          int
	}{
		{"no request id", "", "S", "A", "B", "NORMAL", 1},
		{"blank request id", "  ", "S", "A", "B", "NORMAL", 1},
		{"request id over 200 characters", long, "S", "A", "B", "NORMAL", 1},
		{"no sku", "r", " ", "A", "B", "NORMAL", 1},
		{"no source bin", "r", "S", "", "B", "NORMAL", 1},
		{"no destination bin", "r", "S", "A", "", "NORMAL", 1},
		{"same bin", "r", "S", "A", " A ", "NORMAL", 1},
		{"zero quantity", "r", "S", "A", "B", "NORMAL", 0},
		{"negative quantity", "r", "S", "A", "B", "NORMAL", -3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := workunit.NewReplenishmentMove(tc.request, tc.sku, tc.from, tc.to, tc.qty, tc.prio)
			if !errors.Is(err, workunit.ErrInvalidReplenishmentMove) {
				t.Fatalf("error = %v, want ErrInvalidReplenishmentMove", err)
			}
		})
	}
}

func TestNewReplenishmentMove_RequestIdOfExactly200CharactersIsValid(t *testing.T) {
	id := strings.Repeat("r", workunit.MaxReplenishmentRequestIdLen)
	if _, err := workunit.NewReplenishmentMove(id, "S", "A", "B", 1, "NORMAL"); err != nil {
		t.Fatalf("a %d-character request id must be valid: %v", len(id), err)
	}
}

func TestNewReplenishmentMove_RejectsAnUnknownPriority(t *testing.T) {
	if _, err := workunit.NewReplenishmentMove("r", "S", "A", "B", 1, "HIGH"); !errors.Is(err, workunit.ErrUnknownReplenishmentPriority) {
		t.Fatalf("error = %v, want ErrUnknownReplenishmentPriority", err)
	}
}

func TestReplenishmentWorkUnitId_IsDeterministicPerRequest(t *testing.T) {
	if got := workunit.ReplenishmentWorkUnitId(" repl-1 "); got != "replenish-repl-1" {
		t.Fatalf("ReplenishmentWorkUnitId = %q", got)
	}
}

func TestWorkUnit_SetReplenishmentIsTheSourceOfSKUAndQuantity(t *testing.T) {
	pathId, _ := shared.NewPathId("REPLENISH")
	unit, err := workunit.NewWorkUnit("replenish-r", pathId, shared.NewCPT(time.Date(2026, 10, 17, 18, 0, 0, 0, time.UTC)), "r")
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	if _, ok := unit.Replenishment(); ok {
		t.Fatal("a fresh unit must carry no replenishment")
	}
	move, err := workunit.NewReplenishmentMove("r", "SKU-9", "RSV", "FWD", 12, "NORMAL")
	if err != nil {
		t.Fatalf("NewReplenishmentMove: %v", err)
	}
	unit.SetReplenishment(move)
	got, ok := unit.Replenishment()
	if !ok || got != move {
		t.Fatalf("Replenishment() = %+v, %v", got, ok)
	}
	if unit.SKU() != "SKU-9" || unit.Quantity() != 12 {
		t.Fatalf("sku=%q quantity=%d, want the move's", unit.SKU(), unit.Quantity())
	}
	if unit.WorkKind().IsTransfer() {
		t.Fatal("replenishment is not transfer work")
	}
}

func TestParseWorkKind_AcceptsReplenish(t *testing.T) {
	k, err := workunit.ParseWorkKind("REPLENISH")
	if err != nil || k != workunit.WorkKindReplenish {
		t.Fatalf("ParseWorkKind(REPLENISH) = %q, %v", k, err)
	}
	if k.IsTransfer() {
		t.Fatal("REPLENISH must not be a transfer leg")
	}
}
