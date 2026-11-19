package http

import "net/http"

// ADR-0049 stage C: the charge forecast and the shift plan moved to
// warehouse-planning (its ADR 0023). Their two POST routes stay one release
// and answer 410 Gone for ANY body, naming the route to call instead; they
// go in stage E. Nothing is decoded, recorded or published.
const (
	chargeForecastMovedSlug   = "charge-forecast-moved"
	chargeForecastMovedTitle  = "Charge forecast moved to warehouse-planning"
	chargeForecastMovedDetail = "the charge forecast moved to warehouse-planning (ADR-0049): send it as PUT /workload-forecasts/{site_id}/{path_id} on warehouse-planning"

	shiftPlanMovedSlug   = "shift-plan-moved"
	shiftPlanMovedTitle  = "Shift plan moved to warehouse-planning"
	shiftPlanMovedDetail = "the shift plan moved to warehouse-planning (ADR-0049): send it as PUT /labour-plans/{site_id}/{shift_id}/{path_id} on warehouse-planning"
)

// postChargeForecast answers POST /paths/{pathId}/charge: always 410
// charge-forecast-moved.
func postChargeForecast(w http.ResponseWriter, r *http.Request) {
	writeGone(w, r, chargeForecastMovedSlug, chargeForecastMovedTitle, chargeForecastMovedDetail)
}

// postShiftPlan answers POST /paths/{pathId}/plan: always 410
// shift-plan-moved.
func postShiftPlan(w http.ResponseWriter, r *http.Request) {
	writeGone(w, r, shiftPlanMovedSlug, shiftPlanMovedTitle, shiftPlanMovedDetail)
}

// writeGone writes a 410 RFC 7807 problem for a route whose decision moved
// to another context.
func writeGone(w http.ResponseWriter, r *http.Request, slug, title, detail string) {
	writeProblem(w, http.StatusGone, problemDetails{
		Type:     problemBaseURI + slug,
		Title:    title,
		Status:   http.StatusGone,
		Detail:   detail,
		Instance: r.URL.Path,
	})
}
