package http

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

func (h *Handlers) getTelemetry(w http.ResponseWriter, r *http.Request) {
	pathId, err := pathIdParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	req := usecases.SampleBacklogRequest{PathId: pathId}
	// cutoffAt is optional (ADR-0018): when supplied, this same read
	// also reports — and publishes — the path's current remaining
	// admission capacity correlated against that CPT cutoff. Omitted
	// entirely (the default) keeps this endpoint's existing behavior
	// unchanged. Present-but-empty is NOT "omitted": the parameter is
	// documented as an RFC3339 date-time, and an empty string is not
	// one, so it is a 400 rather than silently skipping capacity
	// reporting (Query().Get conflates absent and empty; Has tells them
	// apart).
	if r.URL.Query().Has("cutoffAt") {
		raw := r.URL.Query().Get("cutoffAt")
		if raw == "" {
			writeError(w, r, fmt.Errorf("%w: cutoffAt is present but empty", errMalformedBody))
			return
		}
		cutoffAt, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeError(w, r, fmt.Errorf("%w: cutoffAt: %v", errMalformedBody, err))
			return
		}
		req.CutoffAt = cutoffAt
	}

	snapshot, err := h.SampleBacklog.Execute(r.Context(), req)
	if err != nil {
		writeError(w, r, err)
		return
	}

	resp := backlogSnapshotResponseDTO{
		PathId:             snapshot.PathId.String(),
		BacklogDepth:       snapshot.BacklogDepth,
		WIP:                snapshot.WIP,
		Mode:               snapshot.Mode,
		OverAlarmThreshold: snapshot.OverAlarmThreshold,
	}
	if !req.CutoffAt.IsZero() {
		known := snapshot.RemainingCapacityKnown
		resp.RemainingCapacityKnown = &known
		if known {
			units := snapshot.RemainingCapacityUnits
			resp.RemainingCapacityUnits = &units
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handlers) getRebalance(w http.ResponseWriter, r *http.Request) {
	pathId, err := pathIdParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	rec, err := h.RebalanceDecision.Execute(r.Context(), usecases.RebalanceDecisionRequest{PathId: pathId})
	if err != nil {
		writeError(w, r, err)
		return
	}

	resp := rebalanceResponseDTO{
		PathId:       rec.PathId.String(),
		Action:       rec.Action.String(),
		BacklogDepth: rec.BacklogDepth,
		WIP:          rec.WIP,
	}
	if h.LaborPlanView != nil {
		if view, err := h.LaborPlanView.Execute(r.Context(), usecases.LaborPlanViewRequest{PathId: pathId}); err == nil {
			resp.LaborPlan = toLaborPlanViewDTO(view)
		} else if !errors.Is(err, ports.ErrNotFound) {
			writeError(w, r, err)
			return
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handlers) getLaborPlanView(w http.ResponseWriter, r *http.Request) {
	pathId, err := pathIdParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	view, err := h.LaborPlanView.Execute(r.Context(), usecases.LaborPlanViewRequest{PathId: pathId})
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toLaborPlanViewDTO(view))
}

func (h *Handlers) getInventoryView(w http.ResponseWriter, r *http.Request) {
	sku := chi.URLParam(r, "sku")

	view, err := h.InventoryView.Execute(r.Context(), usecases.InventoryViewRequest{SKU: sku})
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, inventoryViewResponseDTO{
		SKU:            view.SKU,
		UsableQuantity: view.UsableQuantity,
		ObservedAt:     view.ObservedAt,
	})
}
