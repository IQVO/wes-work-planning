package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/adapters/inbound/http/page"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
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

	// ConfigurePool backs PUT /paths/{pathId}/pool (ADR-0034): the explicit
	// operator command that sets a path's pool mode and WIP limit.
	ConfigurePool *usecases.ConfigurePool

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
	// lineNo is optional (nil = unknown) but, when given, must be in
	// 1..2147483647 (the 32-bit column limit): an explicit 0, negative or
	// too-large value is rejected, never coerced to "unknown".
	lineNo := 0
	if body.LineNo != nil {
		if *body.LineNo < 1 || *body.LineNo > workunit.MaxLineNo {
			writeError(w, r, fmt.Errorf("%w: got %d", workunit.ErrInvalidLineNo, *body.LineNo))
			return
		}
		lineNo = *body.LineNo
	}

	pick, caseBreak, err := packAwareOf(body)
	if err != nil {
		writeError(w, r, err)
		return
	}

	unit, err := h.EnqueueWorkUnit.Execute(r.Context(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: body.WorkUnitId,
		PathId:     pathId,
		CPT:        shared.NewCPT(*body.CPT),
		Reference:  body.Reference,
		SKU:        body.SKU,
		GiftWrap:   body.GiftWrap,
		LineNo:     lineNo,
		Pick:       pick,
		CaseBreak:  caseBreak,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Location", "/work-units/"+unit.Id())
	writeJSON(w, http.StatusCreated, toWorkUnitResponseDTO(unit))
}

// putPool is the ConfigurePool command (ADR-0034): PUT is idempotent by
// construction -- the body fully describes the desired mode and WIP limit.
// It validates pathId against the catalogue because it can seed a new pool.
func (h *Handlers) putPool(w http.ResponseWriter, r *http.Request) {
	pathId, err := pathIdParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := h.validatePathId(pathId); err != nil {
		writeError(w, r, err)
		return
	}

	var body configurePoolRequestDTO
	if !decodeJSON(w, r, &body) {
		return
	}
	if body.WIPLimit == nil {
		writeError(w, r, fmt.Errorf("%w: wipLimit is required", errMalformedBody))
		return
	}
	mode, err := release.ParseFeedMode(body.Mode)
	if err != nil {
		writeError(w, r, fmt.Errorf("%w: mode %q (want ReleaseFed or FlowFed)", err, body.Mode))
		return
	}

	pool, err := h.ConfigurePool.Execute(r.Context(), usecases.ConfigurePoolRequest{
		PathId:   pathId,
		Mode:     mode,
		WIPLimit: *body.WIPLimit,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, poolConfigResponseDTO{
		PathId:       pool.PathId().String(),
		Mode:         pool.Mode().String(),
		WIPLimit:     pool.WIPLimit(),
		WIP:          pool.WIP(),
		BacklogDepth: pool.BacklogDepth(),
	})
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
	if name := page.Unknown(r, "reference"); name != "" {
		writeError(w, r, fmt.Errorf("%w: %q is not a parameter of this collection (declared: reference, limit, cursor)", errUnknownQueryParameter, name))
		return
	}
	limit, err := page.Limit(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	req := usecases.GetWorkUnitsByReferenceRequest{Limit: page.FetchSize(limit)}
	pos, err := page.After(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if pos != nil {
		cpt, perr := time.Parse(time.RFC3339Nano, pos.Sort)
		if perr != nil {
			writeError(w, r, page.ErrInvalidCursor)
			return
		}
		req.After = &workunit.Cursor{CPT: cpt, ID: pos.ID}
	}
	req.Reference = r.URL.Query().Get("reference")
	if req.Reference == "" {
		writeError(w, r, errMissingReference)
		return
	}

	units, err := h.GetWorkUnitsByReference.Execute(r.Context(), req)
	if err != nil {
		writeError(w, r, err)
		return
	}

	out := make([]workUnitResponseDTO, len(units))
	for i, unit := range units {
		out[i] = toWorkUnitResponseDTO(unit)
	}
	writeJSON(w, http.StatusOK, page.Of(out, limit, func(d workUnitResponseDTO) page.Position {
		return page.Position{Sort: d.CPT.UTC().Format(time.RFC3339Nano), ID: d.Id}
	}))
}
