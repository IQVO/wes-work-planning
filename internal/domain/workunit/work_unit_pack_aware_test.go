package workunit_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

func TestNewPickPlan(t *testing.T) {
	cases := []struct {
		name     string
		sku      string
		quantity int
		unit     string
		fromBin  string
		wantErr  error
	}{
		{"each pick", "SKU-1", 3, "EACH", "", nil},
		{"case pick with source bin, trimmed", " SKU-1 ", 24, " CASE ", " FWD-01 ", nil},
		{"no sku", "", 3, "EACH", "", workunit.ErrInvalidPickPlan},
		{"blank sku", "  ", 3, "EACH", "", workunit.ErrInvalidPickPlan},
		{"zero quantity", "SKU-1", 0, "EACH", "", workunit.ErrInvalidPickPlan},
		{"negative quantity", "SKU-1", -4, "CASE", "", workunit.ErrInvalidPickPlan},
		{"unknown unit", "SKU-1", 1, "PALLET", "", workunit.ErrUnknownPickUnit},
		{"lower-case unit is not a wire form", "SKU-1", 1, "case", "", workunit.ErrUnknownPickUnit},
		{"empty unit is not defaulted here", "SKU-1", 1, "", "", workunit.ErrUnknownPickUnit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plan, err := workunit.NewPickPlan(c.sku, c.quantity, c.unit, c.fromBin)
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("err = %v, want %v", err, c.wantErr)
			}
			if c.wantErr == nil && (plan.SKU != "SKU-1" || plan.Quantity != c.quantity || plan.Unit.String() != strings.TrimSpace(c.unit) || plan.FromBin != strings.TrimSpace(c.fromBin)) {
				t.Fatalf("plan = %+v", plan)
			}
		})
	}
}

func TestNewCaseBreak(t *testing.T) {
	long := strings.Repeat("A", 65)
	cases := []struct {
		name                       string
		lpn, sku, looseBin, taskId string
		wantErr                    bool
	}{
		{"valid", "LPN-0001.A_b", "SKU-1", "FWD-01", "task-1", false},
		{"lpn with a space", "LPN 1", "SKU-1", "FWD-01", "task-1", true},
		{"empty lpn", "", "SKU-1", "FWD-01", "task-1", true},
		{"lpn longer than 64", long, "SKU-1", "FWD-01", "task-1", true},
		{"no sku", "L1", "", "FWD-01", "task-1", true},
		{"no loose bin", "L1", "SKU-1", " ", "task-1", true},
		{"no pick task id", "L1", "SKU-1", "FWD-01", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := workunit.NewCaseBreak(c.lpn, c.sku, c.looseBin, c.taskId)
			if (err != nil) != c.wantErr || (err != nil && !errors.Is(err, workunit.ErrInvalidCaseBreak)) {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
		})
	}
}

func newUnit(t *testing.T) *workunit.WorkUnit {
	t.Helper()
	path, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatal(err)
	}
	u, err := workunit.NewWorkUnit("wu-1", path, shared.NewCPT(time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)), "order-1")
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// SetPick is the single source of the unit's sku, quantity (eaches) and kind.
func TestSetPick_SetsSkuQuantityAndPackAwareKind(t *testing.T) {
	for unit, kind := range map[workunit.PickUnit]workunit.WorkKind{
		workunit.PickUnitCase: workunit.WorkKindCasePick,
		workunit.PickUnitEach: workunit.WorkKindEachPick,
	} {
		u := newUnit(t)
		u.SetSKU("stale")
		u.SetQuantity(1)
		plan, err := workunit.NewPickPlan("SKU-9", 36, unit.String(), "FWD-02")
		if err != nil {
			t.Fatal(err)
		}
		u.SetPick(plan)
		got, ok := u.Pick()
		if !ok || got != plan || u.SKU() != "SKU-9" || u.Quantity() != 36 || u.WorkKind() != kind || !kind.IsPickKind() {
			t.Fatalf("%s: pick %+v ok=%v sku=%q qty=%d kind=%q", unit, got, ok, u.SKU(), u.Quantity(), u.WorkKind())
		}
		if _, isBreak := u.CaseBreak(); isBreak {
			t.Fatal("a pick must not look like a case break")
		}
	}
}

func TestSetCaseBreak_SetsSkuAndBreakCaseKind(t *testing.T) {
	u := newUnit(t)
	brk, err := workunit.NewCaseBreak("LPN-1", "SKU-1", "FWD-01", "task-9")
	if err != nil {
		t.Fatal(err)
	}
	u.SetCaseBreak(brk)
	got, ok := u.CaseBreak()
	if !ok || got != brk || u.SKU() != "SKU-1" || u.WorkKind() != workunit.WorkKindBreakCase || u.WorkKind().IsPickKind() {
		t.Fatalf("case break %+v ok=%v sku=%q kind=%q", got, ok, u.SKU(), u.WorkKind())
	}
	if _, isPick := u.Pick(); isPick {
		t.Fatal("a case break must not look like a pick")
	}
}

func TestParseWorkKind_AcceptsThePackAwareKinds(t *testing.T) {
	for _, k := range []string{"CASE_PICK", "EACH_PICK", "BREAK_CASE", "REPLENISH", "RELOCATE", "TRANSFER_PICK"} {
		if _, err := workunit.ParseWorkKind(k); err != nil {
			t.Fatalf("%s: %v", k, err)
		}
	}
	if _, err := workunit.ParseWorkKind("PICK"); !errors.Is(err, workunit.ErrUnknownWorkKind) {
		t.Fatalf("PICK is a path, not a work kind: err = %v", err)
	}
	if workunit.WorkKindCasePick.IsTransfer() || workunit.WorkKindBreakCase.IsTransfer() {
		t.Fatal("pack-aware kinds must never be emitted as transfer work")
	}
}
