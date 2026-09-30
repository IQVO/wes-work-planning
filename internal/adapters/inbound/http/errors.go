package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/charge"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/plan"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// errMalformedBody is a sentinel wrapped around request-body decode failures
// (invalid JSON) so statusFor routes them to 400 instead of falling through
// to the default 500 case — malformed input is a client error, never a
// server error.
var errMalformedBody = errors.New("malformed request body")

// errMissingReference is a sentinel wrapped around a missing/empty
// `reference` query parameter on GET /work-units, mapped to 400 the same
// way errMalformedBody routes decode failures.
var errMissingReference = errors.New("reference query parameter is required")

// statusFor maps a domain/application error to an HTTP status code.
func statusFor(err error) int {
	switch {
	case errors.Is(err, errMalformedBody), errors.Is(err, errMissingReference):
		return http.StatusBadRequest
	case errors.Is(err, ports.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, release.ErrWIPLimitReached),
		errors.Is(err, release.ErrAlreadyReleased),
		errors.Is(err, release.ErrDuplicateEntry),
		errors.Is(err, release.ErrEmptyPool),
		errors.Is(err, workunit.ErrAlreadyReleased),
		errors.Is(err, workunit.ErrAlreadyCompleted),
		errors.Is(err, workunit.ErrNotReleased):
		return http.StatusConflict
	case errors.Is(err, shared.ErrInvalidQuantity),
		errors.Is(err, shared.ErrInvalidRate),
		errors.Is(err, shared.ErrInvalidStationCount),
		errors.Is(err, shared.ErrInvalidPathId),
		errors.Is(err, shared.ErrInvalidHours),
		errors.Is(err, pathcatalog.ErrUnknownPath),
		errors.Is(err, charge.ErrNoBuckets),
		errors.Is(err, charge.ErrUnknownCPT),
		errors.Is(err, plan.ErrHeadsExceedStations),
		errors.Is(err, plan.ErrNoPathPlans),
		errors.Is(err, plan.ErrThroughputNotFinite),
		errors.Is(err, workunit.ErrEmptyId),
		errors.Is(err, workunit.ErrEmptyReference),
		errors.Is(err, release.ErrUnknownEntry):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

// problemBaseURI is the type-URI namespace for this service's RFC 7807
// problem categories. It does not need to resolve — it's an identifier, not
// a fetchable page — but is namespaced per-service so type URIs never
// collide across the warehouse-systems services.
const problemBaseURI = "https://errors.wes-work-planning.warehouse-systems.dev/"

// problemCategory pairs one sentinel error with the RFC 7807 type-URI
// slug and human title problemFor answers for it. The catalog is walked in
// order and the first errors.Is match wins, so the entry order below IS
// the matching order — keep it stable when appending.
type problemCategory struct {
	err   error
	slug  string
	title string
}

// problemCatalog lists every error category problemFor recognizes.
// Categories mirror statusFor's grouping exactly — every sentinel error
// statusFor recognizes has a corresponding entry here.
var problemCatalog = []problemCategory{
	{errMalformedBody, "malformed-request-body", "Malformed request body"},
	{errMissingReference, "reference-required", "Reference query parameter is required"},
	{ports.ErrNotFound, "not-found", "Resource not found"},
	{release.ErrWIPLimitReached, "wip-limit-reached", "Release-fed pool WIP limit reached"},
	{release.ErrAlreadyReleased, "work-pool-entry-already-released", "Work pool entry already released"},
	{release.ErrDuplicateEntry, "work-unit-already-enqueued", "Work unit already enqueued in this pool"},
	{release.ErrEmptyPool, "work-pool-empty", "Work pool is empty"},
	{workunit.ErrAlreadyReleased, "work-unit-already-released", "Work unit already released"},
	{workunit.ErrAlreadyCompleted, "work-unit-already-completed", "Work unit already completed"},
	{workunit.ErrNotReleased, "work-unit-not-released", "Work unit not released"},
	{shared.ErrInvalidQuantity, "invalid-quantity", "Invalid quantity"},
	{shared.ErrInvalidRate, "invalid-rate", "Invalid rate"},
	{shared.ErrInvalidStationCount, "invalid-station-count", "Invalid station count"},
	{shared.ErrInvalidPathId, "invalid-path-id", "Invalid path id"},
	{pathcatalog.ErrUnknownPath, "unknown-path-id", "Unrecognized process-path id"},
	{shared.ErrInvalidHours, "invalid-hours", "Invalid hours"},
	{charge.ErrNoBuckets, "charge-forecast-requires-buckets", "Charge forecast requires at least one CPT bucket"},
	{charge.ErrUnknownCPT, "unknown-cpt", "No bucket exists for the given CPT"},
	{plan.ErrHeadsExceedStations, "heads-exceed-installed-stations", "Planned heads exceed installed stations"},
	{plan.ErrNoPathPlans, "shift-plan-requires-path-plans", "Shift plan requires at least one path plan"},
	{plan.ErrThroughputNotFinite, "planned-throughput-not-finite", "Planned throughput is not finite"},
	{workunit.ErrEmptyId, "work-unit-id-required", "Work unit id is required"},
	{workunit.ErrEmptyReference, "work-unit-reference-required", "Work unit reference is required"},
	{release.ErrUnknownEntry, "work-pool-entry-not-found", "Work unit not found in this pool"},
}

// problemFor returns the RFC 7807 (type, title) pair for a category of
// error: the first problemCatalog sentinel err matches (errors.Is), or the
// catch-all internal-error category.
func problemFor(err error) (typeURI, title string) {
	for _, category := range problemCatalog {
		if errors.Is(err, category.err) {
			return problemBaseURI + category.slug, category.title
		}
	}
	return problemBaseURI + "internal-error", "Internal server error"
}

// writeError writes a domain/application error as an RFC 7807
// (https://www.rfc-editor.org/rfc/rfc7807) application/problem+json body.
// statusFor's mapping is unchanged; this only shapes what gets written.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	status := statusFor(err)
	typeURI, title := problemFor(err)
	writeProblem(w, status, problemDetails{
		Type:     typeURI,
		Title:    title,
		Status:   status,
		Detail:   err.Error(),
		Instance: r.URL.Path,
	})
}

func writeProblem(w http.ResponseWriter, status int, body problemDetails) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
