package http

import (
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"
)

// NewRouter wires every REST endpoint to its handler. serviceName names the
// server in the OTel span and metric attributes; logger, when non-nil,
// enables the structured per-request access log. Each request gets a server
// span named after its chi route pattern (not the raw path, which would
// blow up span cardinality on the {pathId}/{id}/{sku} segments) plus the
// semconv http.server.request.duration histogram.
func NewRouter(h *Handlers, serviceName string, logger *slog.Logger) *chi.Mux {
	r := chi.NewRouter()

	metricCfg := otelchimetric.NewBaseConfig(serviceName)

	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(otelchimetric.NewServerActiveRequests(metricCfg))
	if logger != nil {
		r.Use(RequestLogger(logger))
	}
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware())

	r.Get("/healthz", healthz)
	r.Get("/readyz", h.handleReadyz)

	r.Route("/paths/{pathId}", func(r chi.Router) {
		r.Post("/charge", h.postChargeForecast)
		r.Post("/plan", h.postShiftPlan)
		// POST /paths/{pathId}/work-units is route-scoped (r.With, not
		// r.Use) behind RequireIdempotencyKey — it is the one mutating
		// endpoint in this service that creates a NEW resource with a
		// (caller-supplied but server-persisted) id, so a lost response
		// and a client retry would otherwise duplicate the work unit
		// (see docs/docs/adr for the ADR). The other three mutating POST
		// routes are explicitly ruled out: postChargeForecast and
		// postShiftPlan are upserts keyed by the caller-supplied path_id
		// (ON CONFLICT (path_id) DO UPDATE — idempotent by construction,
		// a retry just re-applies the same natural-key upsert), and
		// postRelease acts on whichever existing pending entry the
		// release policy currently picks, not a caller-identified new
		// resource. IdempotencyPool nil (in-memory dev/test
		// configuration, no transactional Postgres backing) skips the
		// middleware entirely, mirroring every other optional
		// Postgres-backed capability's nil convention in this service.
		if h.IdempotencyPool != nil {
			r.With(RequireIdempotencyKey(h.IdempotencyPool)).Post("/work-units", h.postWorkUnit)
		} else {
			r.Post("/work-units", h.postWorkUnit)
		}
		r.Post("/release", h.postRelease)
		r.Get("/telemetry", h.getTelemetry)
		r.Get("/rebalance", h.getRebalance)
		r.Get("/labor-plan-view", h.getLaborPlanView)
	})

	r.Post("/work-units/{id}/complete", h.postComplete)
	r.Get("/work-units", h.getWorkUnitsByReference)

	r.Get("/inventory-view/{sku}", h.getInventoryView)

	return r
}

// corsMiddleware allows the warehouse-console browser SPA (and this
// service's own future MFE remote dev origin) to call this API directly
// from the browser. CORS_ALLOWED_ORIGINS overrides the local-dev default
// (comma-separated) for staging/prod deployments.
func corsMiddleware() func(http.Handler) http.Handler {
	origins := []string{"http://localhost:5173", "http://localhost:5183"}
	if v := os.Getenv("CORS_ALLOWED_ORIGINS"); v != "" {
		origins = strings.Split(v, ",")
	}
	return cors.Handler(cors.Options{
		AllowedOrigins:   origins,
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: false,
		MaxAge:           300,
	})
}
