package workunit

import (
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func newTransferHostUnit(t *testing.T) *WorkUnit {
	t.Helper()
	cpt := shared.NewCPT(time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC))
	unit, err := NewWorkUnit("wu-xfer-1", mustTransferPath(t), cpt, "demand-1")
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	return unit
}

func mustTransferPath(t *testing.T) shared.PathId {
	t.Helper()
	pathId, err := shared.NewPathId("pick-transfer-a")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	return pathId
}

func TestParseWorkKind(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want WorkKind
		err  error
	}{
		{"TRANSFER_PICK", WorkKindTransferPick, nil},
		{"TRANSFER_DISPATCH", WorkKindTransferDispatch, nil},
		{"TRANSFER_ARRIVAL", WorkKindTransferArrival, nil},
		{"TRANSFER_PICK ", "", ErrUnknownWorkKind},
		{"transfer_pick", "", ErrUnknownWorkKind},
		{"SOMETHING_ELSE", "", ErrUnknownWorkKind},
		{"", "", ErrUnknownWorkKind},
	} {
		got, err := ParseWorkKind(tc.in)
		if tc.err == nil && err != nil {
			t.Fatalf("ParseWorkKind(%q): %v", tc.in, err)
		}
		if tc.err != nil && err != tc.err {
			t.Fatalf("ParseWorkKind(%q) err = %v, want %v", tc.in, err, tc.err)
		}
		if tc.err == nil && got != tc.want {
			t.Fatalf("ParseWorkKind(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestWorkUnit_TransferMetadataDefaultsToZeroValues(t *testing.T) {
	unit := newTransferHostUnit(t)
	if unit.TransferRef() != "" || unit.WorkKind() != "" || unit.SiteId() != "" || unit.Quantity() != 0 {
		t.Fatalf("expected zero-valued transfer metadata on a fresh unit, got ref=%q kind=%q site=%q qty=%d",
			unit.TransferRef(), unit.WorkKind(), unit.SiteId(), unit.Quantity())
	}
}

func TestWorkUnit_SetTransferMetadata(t *testing.T) {
	unit := newTransferHostUnit(t)
	unit.SetTransferRef("TRF-2026-001")
	unit.SetWorkKind(WorkKindTransferPick)
	unit.SetSiteId("site-north-1")
	unit.SetQuantity(42)

	if unit.TransferRef() != "TRF-2026-001" {
		t.Fatalf("TransferRef = %q", unit.TransferRef())
	}
	if unit.WorkKind() != WorkKindTransferPick {
		t.Fatalf("WorkKind = %q", unit.WorkKind())
	}
	if unit.SiteId() != "site-north-1" {
		t.Fatalf("SiteId = %q", unit.SiteId())
	}
	if unit.Quantity() != 42 {
		t.Fatalf("Quantity = %d", unit.Quantity())
	}
}
