package usecases

import (
	"context"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// GetWorkUnitsByReference is a read-only query use case exposing every
// WorkUnit ever enqueued for an external reference (e.g. an order line),
// so a cross-service caller can answer "what work did this service release
// for order X, line N". A reference can plausibly have more than one
// WorkUnit across retries/history, so this returns a slice rather than a
// single unit.
type GetWorkUnitsByReference struct {
	workUnits ports.WorkUnitRepo
}

func NewGetWorkUnitsByReference(workUnits ports.WorkUnitRepo) *GetWorkUnitsByReference {
	return &GetWorkUnitsByReference{workUnits: workUnits}
}

type GetWorkUnitsByReferenceRequest struct {
	Reference string
	// After resumes strictly after this (cpt, id) keyset position; nil is
	// the first page. Limit caps the rows returned (0 = unbounded); the
	// HTTP adapter asks for limit+1 to learn whether a next page exists.
	After *workunit.Cursor
	Limit int
}

func (uc *GetWorkUnitsByReference) Execute(ctx context.Context, req GetWorkUnitsByReferenceRequest) ([]*workunit.WorkUnit, error) {
	return uc.workUnits.FindByReferencePage(ctx, req.Reference, req.After, req.Limit)
}
