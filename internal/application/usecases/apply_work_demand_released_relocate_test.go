package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0040: RELOCATE is a valid stored work kind, but never a transfer leg:
// a WorkDemandReleased carrying it is poison, exactly like an unknown kind.
func TestApplyWorkDemandReleased_RejectsRelocateWorkKind(t *testing.T) {
	f := newDemandFixture()
	req := demandRequest("evt-relocate", "demand-relocate")
	req.WorkKind = workunit.WorkKindRelocate
	if _, err := f.uc.Execute(context.Background(), req); !errors.Is(err, workunit.ErrUnknownWorkKind) {
		t.Fatalf("err = %v, want ErrUnknownWorkKind", err)
	}
	if _, err := f.workUnits.FindById(context.Background(), "demand-relocate"); err == nil {
		t.Fatal("a RELOCATE work demand must not be enqueued")
	}
}
