package http

import (
	"fmt"
	"net/http"

	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

func (h *Handlers) postChargeForecast(w http.ResponseWriter, r *http.Request) {
	pathId, err := pathIdParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := h.validatePathId(pathId); err != nil {
		writeError(w, r, err)
		return
	}

	var body receiveChargeForecastRequestDTO
	if !decodeJSON(w, r, &body) {
		return
	}

	buckets := make([]usecases.CPTBucketInput, len(body.Buckets))
	for i, b := range body.Buckets {
		if b.CPT == nil {
			writeError(w, r, fmt.Errorf("%w: buckets[%d].cpt is required", errMalformedBody, i))
			return
		}
		if b.Quantity == nil {
			writeError(w, r, fmt.Errorf("%w: buckets[%d].quantity is required", errMalformedBody, i))
			return
		}
		qty, err := shared.NewQuantity(*b.Quantity)
		if err != nil {
			writeError(w, r, err)
			return
		}
		pickUnit, err := pickUnitOf(b.PickUnit)
		if err != nil {
			writeError(w, r, err)
			return
		}
		buckets[i] = usecases.CPTBucketInput{CPT: shared.NewCPT(*b.CPT), Quantity: qty, SKU: b.SKU, PickUnit: pickUnit}
	}

	forecast, err := h.ReceiveChargeForecast.Execute(r.Context(), usecases.ReceiveChargeForecastRequest{
		PathId:  pathId,
		Buckets: buckets,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Location", "/paths/"+forecast.PathId().String()+"/charge")
	writeJSON(w, http.StatusCreated, chargeForecastResponseDTO{
		PathId:        forecast.PathId().String(),
		TotalQuantity: forecast.TotalQuantity().Value(),
		Buckets:       toBucketDTOs(forecast.Buckets()),
		ReceivedAt:    forecast.ReceivedAt(),
	})
}

func (h *Handlers) postShiftPlan(w http.ResponseWriter, r *http.Request) {
	pathId, err := pathIdParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := h.validatePathId(pathId); err != nil {
		writeError(w, r, err)
		return
	}

	var body commitShiftPlanRequestDTO
	if !decodeJSON(w, r, &body) {
		return
	}

	if body.PlannedHeads == nil {
		writeError(w, r, fmt.Errorf("%w: plannedHeads is required", errMalformedBody))
		return
	}
	if body.InstalledStations == nil {
		writeError(w, r, fmt.Errorf("%w: installedStations is required", errMalformedBody))
		return
	}

	heads, err := shared.NewStationCount(*body.PlannedHeads)
	if err != nil {
		writeError(w, r, err)
		return
	}
	installed, err := shared.NewStationCount(*body.InstalledStations)
	if err != nil {
		writeError(w, r, err)
		return
	}
	rate, err := shared.NewRate(body.RateUnitsPerHour)
	if err != nil {
		writeError(w, r, err)
		return
	}

	pickUnit, err := pickUnitOf(body.PickUnit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	shiftPlan, err := h.CommitShiftPlan.Execute(r.Context(), usecases.CommitShiftPlanRequest{
		PathId:            pathId,
		PlannedHeads:      heads,
		InstalledStations: installed,
		Rate:              rate,
		Hours:             body.Hours,
		FromLocationCode:  body.FromLocationCode,
		ToLocationCode:    body.ToLocationCode,
		SKU:               body.SKU,
		PickUnit:          pickUnit,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	pathPlan, _ := shiftPlan.PathPlan(pathId)
	w.Header().Set("Location", "/paths/"+pathId.String()+"/plan")
	writeJSON(w, http.StatusCreated, toShiftPlanResponseDTO(pathPlan))
}
