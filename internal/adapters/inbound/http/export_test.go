package http

import "net/http"

// ProblemTypeForTest is one problem type of the catalogue, for the external
// test package.
type ProblemTypeForTest struct {
	Slug   string
	Status int
	Class  string
}

// ProblemTypesForTest is the whole catalogue (allProblemTypes), one entry per
// (slug, status).
func ProblemTypesForTest() []ProblemTypeForTest {
	var out []ProblemTypeForTest
	for _, p := range allProblemTypes() {
		out = append(out, ProblemTypeForTest{Slug: p.Slug, Status: p.Status, Class: string(p.Class)})
	}
	return out
}

// RefuseForTest answers the problem type (slug, status) through the
// production error writer: a typed error of the catalogue through writeError,
// any other problem through the same writer its production call site uses, so
// the class marking the middleware reads is the production one.
func RefuseForTest(w http.ResponseWriter, r *http.Request, slug string, status int) {
	for _, e := range problemCatalog {
		if e.slug == slug && statusFor(e.err) == status {
			writeError(w, r, e.err)
			return
		}
	}
	switch slug {
	case "internal-error":
		markReplayClass(w, classRetryable)
		writeProblem(w, http.StatusInternalServerError, internalErrorProblemDetails(r.URL.Path))
	case "not-found":
		markReplayClass(w, classRetryable)
		writeProblem(w, http.StatusNotFound, nulProblem(http.StatusNotFound, "not-found", "Resource not found", "no resource matches the requested path", r.URL.Path))
	case "malformed-request-body":
		markReplayClass(w, classFinal)
		writeProblem(w, http.StatusBadRequest, nulProblem(http.StatusBadRequest, "malformed-request-body", "Malformed request body", "refused for the test", r.URL.Path))
	case "charge-forecast-moved":
		markReplayClass(w, classFinal)
		writeProblem(w, http.StatusGone, problemDetails{Type: problemBaseURI + "charge-forecast-moved", Title: "Charge forecast moved to warehouse-planning", Status: http.StatusGone, Detail: "moved", Instance: r.URL.Path})
	case "shift-plan-moved":
		markReplayClass(w, classFinal)
		writeProblem(w, http.StatusGone, problemDetails{Type: problemBaseURI + "shift-plan-moved", Title: "Shift plan moved to warehouse-planning", Status: http.StatusGone, Detail: "moved", Instance: r.URL.Path})
	case "dependency-unavailable":
		markReplayClass(w, classRetryable)
		writeProblem(w, http.StatusServiceUnavailable, problemDetails{
			Type:     problemBaseURI + "dependency-unavailable",
			Title:    "A runtime dependency is unavailable",
			Status:   http.StatusServiceUnavailable,
			Detail:   "refused for the test",
			Instance: r.URL.Path,
		})
	case "invalid-report-query":
		markReplayClass(w, classFinal)
		writeProblem(w, http.StatusBadRequest, problemDetails{
			Type:     problemBaseURI + "invalid-report-query",
			Title:    "The report query is malformed or missing a required parameter",
			Status:   http.StatusBadRequest,
			Detail:   "refused for the test",
			Instance: r.URL.Path,
		})
	case "report-store-error":
		markReplayClass(w, classRetryable)
		writeProblem(w, http.StatusInternalServerError, problemDetails{
			Type:     problemBaseURI + "report-store-error",
			Title:    "The report could not be served",
			Status:   http.StatusInternalServerError,
			Detail:   "refused for the test",
			Instance: r.URL.Path,
		})
	default:
		panic("no problem type " + slug)
	}
}

// ProbeRefuseForTest answers a refusal of the given status whose replay class
// is class, through the production writer shape. No problem type of this
// service declares OUTCOME (ADR 0050: no keyed use case commits state before
// refusing), so the middleware's OUTCOME branch is proved with this probe.
func ProbeRefuseForTest(w http.ResponseWriter, r *http.Request, status int, class string) {
	markReplayClass(w, replayClass(class))
	writeProblem(w, status, problemDetails{
		Type:     problemBaseURI + "probe-refusal",
		Title:    "Probe",
		Status:   status,
		Detail:   "refused for the test",
		Instance: r.URL.Path,
	})
}
