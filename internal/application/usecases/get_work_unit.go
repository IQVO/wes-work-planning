package usecases

import (
	"context"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// GetWorkUnit is a read-only query use case that resolves ONE WorkUnit by
// its own identity (the WorkUnitId), as opposed to GetWorkUnitsByReference,
// which looks units up by their upstream order-line reference. It lets a
// client holding only a WorkUnitId (e.g. an RF gun resolving the orderRef
// of a PICK task claimed from fulfillment-execution) read the unit's sku,
// reference, pathId, cpt and state without parsing the id string.
// An unknown id surfaces as ports.ErrNotFound.
type GetWorkUnit struct {
	workUnits ports.WorkUnitRepo
}

func NewGetWorkUnit(workUnits ports.WorkUnitRepo) *GetWorkUnit {
	return &GetWorkUnit{workUnits: workUnits}
}

type GetWorkUnitRequest struct {
	WorkUnitId string
}

func (uc *GetWorkUnit) Execute(ctx context.Context, req GetWorkUnitRequest) (*workunit.WorkUnit, error) {
	return uc.workUnits.FindById(ctx, req.WorkUnitId)
}
