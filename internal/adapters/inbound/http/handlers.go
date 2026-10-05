package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/charge"
	"github.com/claudioed/wes-work-planning/internal/domain/laborview"
	"github.com/claudioed/wes-work-planning/internal/domain/plan"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// Handlers holds every use case the inbound HTTP adapter depends on.
type Handlers struct {
	ReceiveChargeForecast *usecases.ReceiveChargeForecast
	CommitShiftPlan       *usecases.CommitShiftPlan
	EnqueueWorkUnit       *usecases.EnqueueWorkUnit
	ReleaseNextWork       *usecases.ReleaseNextWork
	RecordCompletion      *usecases.RecordCompletion
	SampleBacklog         *usecases.SampleBacklog
	RebalanceDecision     *usecases.RebalanceDecision

	// Catalogue validates every caller-supplied path_id against the
	// fleet's declared process-path catalogue before it is allowed to
	// seed a new WorkPool/ChargeForecast/ShiftPlan — see
	// fulfillment-execution's ADR-0017. A nil Catalogue (only ever the
	// case in older tests not yet updated) skips validation rather than
	// panicking, so this is additive, not a required wiring change for
	// every caller. ports.PathCatalogue (not the concrete
	// *pathcatalog.Catalogue) so an alternative source (e.g. a
	// Kafka-fed catalogue) can be wired in without touching this
	// struct.
	Catalogue ports.PathCatalogue

	// Additive: cross-service integration read models (Task 7).
	LaborPlanView *usecases.LaborPlanView
	InventoryView *usecases.InventoryView

	// Additive: read-only lookup of work units by their order-line
	// reference, for cross-service console screens.
	GetWorkUnitsByReference *usecases.GetWorkUnitsByReference

	// Additive: read-only lookup of ONE work unit by its own identity
	// (GET /work-units/{id}), so a client holding only a WorkUnitId never
	// has to parse the id string to recover its sku/reference/path.
	GetWorkUnit *usecases.GetWorkUnit

	// IdempotencyPool, when non-nil, wires RequireIdempotencyKey onto
	// POST /paths/{pathId}/work-units (see router.go and idempotency.go).
	// A nil pool means "no transactional Postgres backing wired"
	// (in-memory dev/test configuration) — the idempotency middleware
	// needs a real pgxpool.Pool to begin its own transaction, so it is
	// simply not applied in that case, mirroring this codebase's
	// existing convention for every other optional Postgres-backed
	// capability (UnitOfWork, the outbox relay).
	IdempotencyPool *pgxpool.Pool

	// Readiness backs GET /readyz (graceful shutdown hardening):
	// flipped to not-ready as the FIRST step of shutdown, in cmd/wes,
	// before anything else stops. A nil Readiness (the Handlers zero
	// value, every existing test) always reports ready.
	Readiness *Readiness
}

func pathIdParam(r *http.Request) (shared.PathId, error) {
	return shared.NewPathId(chi.URLParam(r, "pathId"))
}

// validatePathId checks pathId against h.Catalogue when one is wired in.
// Call this from every handler that seeds a NEW aggregate keyed by a
// caller-supplied path_id (ChargeForecast, ShiftPlan, WorkPool via
// EnqueueWorkUnit) — read-only lookups do not need it, since a
// nonexistent path already surfaces as ports.ErrNotFound on its own.
func (h *Handlers) validatePathId(pathId shared.PathId) error {
	if h.Catalogue == nil {
		return nil
	}
	_, err := h.Catalogue.Lookup(pathId.String())
	return err
}

// decodeJSON decodes the request body into dst, writing an RFC-mapped 400
// response and returning false on malformed JSON so callers can bail out.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, r, fmt.Errorf("%w: %v", errMalformedBody, err))
		return false
	}
	return true
}

func toBucketDTOs(buckets []charge.CPTBucket) []cptBucketDTO {
	out := make([]cptBucketDTO, len(buckets))
	for i, b := range buckets {
		cpt := b.CPT.Time()
		qty := b.Quantity.Value()
		out[i] = cptBucketDTO{CPT: &cpt, Quantity: &qty}
	}
	return out
}

func toWorkUnitResponseDTO(unit *workunit.WorkUnit) workUnitResponseDTO {
	return workUnitResponseDTO{
		Id:          unit.Id(),
		PathId:      unit.PathId().String(),
		CPT:         unit.CPT().Time(),
		Reference:   unit.Reference(),
		State:       unit.State().String(),
		GiftWrap:    unit.GiftWrap(),
		SKU:         unit.SKU(),
		ReleasedAt:  unit.ReleasedAt(),
		CompletedAt: unit.CompletedAt(),
	}
}

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
		buckets[i] = usecases.CPTBucketInput{CPT: shared.NewCPT(*b.CPT), Quantity: qty}
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

	shiftPlan, err := h.CommitShiftPlan.Execute(r.Context(), usecases.CommitShiftPlanRequest{
		PathId:            pathId,
		PlannedHeads:      heads,
		InstalledStations: installed,
		Rate:              rate,
		Hours:             body.Hours,
		FromLocationCode:  body.FromLocationCode,
		ToLocationCode:    body.ToLocationCode,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	pathPlan, _ := shiftPlan.PathPlan(pathId)
	w.Header().Set("Location", "/paths/"+pathId.String()+"/plan")
	writeJSON(w, http.StatusCreated, toShiftPlanResponseDTO(pathPlan))
}

func toShiftPlanResponseDTO(p plan.PathPlan) shiftPlanResponseDTO {
	dto := shiftPlanResponseDTO{
		PathId:            p.PathId().String(),
		PlannedHeads:      p.PlannedHeads().Value(),
		InstalledStations: p.InstalledStations().Value(),
		RateUnitsPerHour:  p.Rate().UnitsPerHour(),
		Hours:             p.Hours(),
		PlannedThroughput: p.PlannedThroughput(),
	}
	if p.TravelDistanceKnown() {
		metresM := p.TravelDistanceM()
		estimated := p.TravelDistanceEstimated()
		dto.TravelDistanceM = &metresM
		dto.TravelDistanceEstimated = &estimated
	}
	return dto
}

func (h *Handlers) postWorkUnit(w http.ResponseWriter, r *http.Request) {
	pathId, err := pathIdParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := h.validatePathId(pathId); err != nil {
		writeError(w, r, err)
		return
	}

	var body enqueueWorkUnitRequestDTO
	if !decodeJSON(w, r, &body) {
		return
	}
	if body.CPT == nil {
		writeError(w, r, fmt.Errorf("%w: cpt is required", errMalformedBody))
		return
	}

	unit, err := h.EnqueueWorkUnit.Execute(r.Context(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: body.WorkUnitId,
		PathId:     pathId,
		CPT:        shared.NewCPT(*body.CPT),
		Reference:  body.Reference,
		SKU:        body.SKU,
		GiftWrap:   body.GiftWrap,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Location", "/work-units/"+unit.Id())
	writeJSON(w, http.StatusCreated, toWorkUnitResponseDTO(unit))
}

func (h *Handlers) postRelease(w http.ResponseWriter, r *http.Request) {
	pathId, err := pathIdParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	unit, err := h.ReleaseNextWork.Execute(r.Context(), usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toWorkUnitResponseDTO(unit))
}

func (h *Handlers) postComplete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	unit, err := h.RecordCompletion.Execute(r.Context(), usecases.RecordCompletionRequest{WorkUnitId: id})
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toWorkUnitResponseDTO(unit))
}

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

func toLaborPlanViewDTO(view laborview.LaborPlanObserved) *laborPlanViewDTO {
	return &laborPlanViewDTO{
		PathId:       view.PathId.String(),
		PlannedHeads: view.PlannedHeads,
		PlannedRate:  view.PlannedRate,
		PlannedHours: view.PlannedHours,
		ObservedAt:   view.ObservedAt,

		DriftHeads:      view.DriftHeads,
		DriftDetectedAt: view.DriftDetectedAt,
	}
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

func healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handlers) getWorkUnit(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	unit, err := h.GetWorkUnit.Execute(r.Context(), usecases.GetWorkUnitRequest{WorkUnitId: id})
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, toWorkUnitResponseDTO(unit))
}

func (h *Handlers) getWorkUnitsByReference(w http.ResponseWriter, r *http.Request) {
	reference := r.URL.Query().Get("reference")
	if reference == "" {
		writeError(w, r, errMissingReference)
		return
	}

	units, err := h.GetWorkUnitsByReference.Execute(r.Context(), usecases.GetWorkUnitsByReferenceRequest{Reference: reference})
	if err != nil {
		writeError(w, r, err)
		return
	}

	out := make([]workUnitResponseDTO, len(units))
	for i, unit := range units {
		out[i] = toWorkUnitResponseDTO(unit)
	}

	writeJSON(w, http.StatusOK, out)
}
