package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/wes-work-planning/internal/analytics/report"
)

// ReportsHandlers is the inbound HTTP adapter for the "Release Throughput &
// Backlog Health" data product's READER. It depends only on the read-model
// port (report.ReportStore); it never touches the OLTP use cases or the
// writer (ADR-0011).
type ReportsHandlers struct {
	Store report.ReportStore
}

// throughputRowDTO is the wire shape of one report row. It is a dedicated
// DTO so the read-model struct (report.Row) never leaks onto the API.
type throughputRowDTO struct {
	PathId                   string `json:"pathId"`
	HourBucket               string `json:"hourBucket"`
	WorkReleased             int    `json:"workReleased"`
	WorkUnitCompleted        int    `json:"workUnitCompleted"`
	BacklogThresholdBreached int    `json:"backlogThresholdBreached"`
	PathThrottled            int    `json:"pathThrottled"`
	RateDeviationDetected    int    `json:"rateDeviationDetected"`
}

// throughputReportDTO is the wire shape of a throughput report response.
type throughputReportDTO struct {
	Rows []throughputRowDTO `json:"rows"`
}

// freshnessDTO is the wire shape of the freshness-lag response.
type freshnessDTO struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// GetThroughput serves GET /reports/throughput. from and to (RFC3339) are
// required; pathId and granularity are optional (granularity defaults to
// hour).
func (h *ReportsHandlers) GetThroughput(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	from, ok := parseRequiredTime(w, r, q.Get("from"), "from")
	if !ok {
		return
	}
	to, ok := parseRequiredTime(w, r, q.Get("to"), "to")
	if !ok {
		return
	}

	granularity := report.GranularityHour
	if g := q.Get("granularity"); g != "" {
		if g != string(report.GranularityHour) {
			writeReportBadRequest(w, r, "granularity must be 'hour'")
			return
		}
		granularity = report.Granularity(g)
	}

	rep, err := h.Store.Query(r.Context(), report.ReportQuery{
		From:        from,
		To:          to,
		PathId:      q.Get("pathId"),
		Granularity: granularity,
	})
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}

	dto := throughputReportDTO{Rows: make([]throughputRowDTO, 0, len(rep.Rows))}
	for _, row := range rep.Rows {
		dto.Rows = append(dto.Rows, throughputRowDTO{
			PathId:                   row.Key.PathId,
			HourBucket:               row.Key.HourBucket.UTC().Format(time.RFC3339),
			WorkReleased:             row.WorkReleased,
			WorkUnitCompleted:        row.WorkUnitCompleted,
			BacklogThresholdBreached: row.BacklogThresholdBreached,
			PathThrottled:            row.PathThrottled,
			RateDeviationDetected:    row.RateDeviationDetected,
		})
	}
	writeJSON(w, http.StatusOK, dto)
}

// GetFreshness serves GET /reports/throughput/freshness.
func (h *ReportsHandlers) GetFreshness(w http.ResponseWriter, r *http.Request) {
	lag, err := h.Store.FreshnessLag(r.Context())
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, freshnessDTO{LagSeconds: lag.Seconds()})
}

// GetReportsHealthz serves GET /healthz for the reports service.
func (h *ReportsHandlers) GetReportsHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// parseRequiredTime parses an RFC3339 timestamp, writing an RFC 7807 400 and
// returning ok=false when it is missing or malformed.
func parseRequiredTime(w http.ResponseWriter, r *http.Request, raw, name string) (time.Time, bool) {
	if raw == "" {
		writeReportBadRequest(w, r, "query parameter '"+name+"' is required (RFC3339)")
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeReportBadRequest(w, r, "query parameter '"+name+"' must be an RFC3339 timestamp")
		return time.Time{}, false
	}
	return t, true
}

// writeReportBadRequest writes the reports service's RFC 7807 400.
func writeReportBadRequest(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, http.StatusBadRequest, problemDetails{
		Type:     problemBaseURI + "invalid-report-query",
		Title:    "The report query is malformed or missing a required parameter",
		Status:   http.StatusBadRequest,
		Detail:   detail,
		Instance: r.URL.Path,
	})
}

// writeReportInternal writes the reports service's RFC 7807 500.
func writeReportInternal(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, http.StatusInternalServerError, problemDetails{
		Type:     problemBaseURI + "report-store-error",
		Title:    "The report could not be served",
		Status:   http.StatusInternalServerError,
		Detail:   err.Error(),
		Instance: r.URL.Path,
	})
}

// NewReportsRouter builds the chi router for the wes-reports reader service.
// serviceName names the server in the OTel span attributes; a nil logger
// falls back to slog.Default().
func NewReportsRouter(h *ReportsHandlers, serviceName string, logger *slog.Logger) *chi.Mux {
	if logger == nil {
		logger = slog.Default()
	}

	r := chi.NewRouter()
	metricCfg := otelchimetric.NewBaseConfig(serviceName)
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(otelchimetric.NewServerActiveRequests(metricCfg))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", h.GetReportsHealthz)
	r.Get("/reports/throughput", h.GetThroughput)
	r.Get("/reports/throughput/freshness", h.GetFreshness)

	return r
}
