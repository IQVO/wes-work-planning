package http

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0040: GET /work-units/{id} and GET /work-units?reference=<plan_id>
// show a RELOCATE unit's move, what it waits on, and a Cancelled state with
// its timestamp. Order and transfer units keep their exact previous shape.

func relocationDTOUnit(t *testing.T) *workunit.WorkUnit {
	t.Helper()
	pathId, _ := shared.NewPathId("RELOCATE")
	unit, err := workunit.NewWorkUnit("plan-1-move-1", pathId, shared.NewCPT(time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)), "plan-1")
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	move, err := workunit.NewRelocationMove("plan-1", 1, "Relocate", "S2", "S3")
	if err != nil {
		t.Fatalf("NewRelocationMove: %v", err)
	}
	unit.SetWorkKind(workunit.WorkKindRelocate)
	unit.SetSiteId("SITE-1")
	unit.SetSKU("SKU-2")
	unit.SetRelocation(move)
	unit.SetBlockedBy("plan-1-move-2")
	return unit
}

func dtoJSON(t *testing.T, unit *workunit.WorkUnit) map[string]any {
	t.Helper()
	raw, err := json.Marshal(toWorkUnitResponseDTO(unit))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func TestWorkUnitResponse_RelocationUnit(t *testing.T) {
	got := dtoJSON(t, relocationDTOUnit(t))
	rel, ok := got["relocation"].(map[string]any)
	if !ok {
		t.Fatalf("relocation missing: %v", got)
	}
	want := map[string]any{"planId": "plan-1", "moveSeq": float64(1), "kind": "Relocate", "siteId": "SITE-1", "fromSlot": "S2", "toSlot": "S3", "blockedBy": "plan-1-move-2"}
	for k, v := range want {
		if rel[k] != v {
			t.Fatalf("relocation.%s = %v, want %v (%v)", k, rel[k], v, rel)
		}
	}
	if got["state"] != "Pending" || got["sku"] != "SKU-2" {
		t.Fatalf("state=%v sku=%v", got["state"], got["sku"])
	}
	if _, ok := got["cancelledAt"]; ok {
		t.Fatal("cancelledAt must be omitted until cancelled")
	}
}

func TestWorkUnitResponse_CancelledUnit(t *testing.T) {
	unit := relocationDTOUnit(t)
	unit.Unblock()
	at := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	if err := unit.Cancel(at); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	got := dtoJSON(t, unit)
	if got["state"] != "Cancelled" || got["cancelledAt"] != "2026-10-15T09:00:00Z" {
		t.Fatalf("state=%v cancelledAt=%v", got["state"], got["cancelledAt"])
	}
	rel := got["relocation"].(map[string]any)
	if _, ok := rel["blockedBy"]; ok {
		t.Fatalf("blockedBy must be omitted when not blocked: %v", rel)
	}
}

func TestWorkUnitResponse_OrderUnitHasNoRelocation(t *testing.T) {
	pathId, _ := shared.NewPathId("pick-a")
	unit, _ := workunit.NewWorkUnit("wu-1", pathId, shared.NewCPT(time.Now()), "order-1")
	got := dtoJSON(t, unit)
	if _, ok := got["relocation"]; ok {
		t.Fatalf("an order unit must not carry relocation: %v", got)
	}
}
