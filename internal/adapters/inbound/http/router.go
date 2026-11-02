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
	// A NUL (U+0000) in a path, query or body is a client error: Postgres
	// text cannot hold it. Before any route-scoped idempotency middleware.
	r.Use(rejectNUL)
	r.Get("/healthz", healthz)
	r.Get("/readyz", h.handleReadyz)

	// Every POST is route-scoped (r.With, not r.Use) behind
	// RequireIdempotencyKey (REST guideline section 2, ADR-0047). PUT and
	// GET take no middleware. IdempotencyPool nil (in-memory test
	// configuration, no transactional Postgres backing) skips the
	// middleware entirely; production is Postgres-only (ADR-0044).
	keyed := func(rt chi.Router) chi.Router {
		if h.IdempotencyPool == nil {
			return rt
		}
		return rt.With(RequireIdempotencyKey(h.IdempotencyPool))
	}

	r.Route("/paths/{pathId}", func(r chi.Router) {
		keyed(r).Post("/charge", h.postChargeForecast)
		keyed(r).Post("/plan", h.postShiftPlan)
		keyed(r).Post("/work-units", h.postWorkUnit)
		keyed(r).Post("/work-releases", h.postRelease)
		r.Put("/pool", h.putPool)
		r.Get("/telemetry", h.getTelemetry)
		r.Get("/rebalance", h.getRebalance)
		r.Get("/labor-plan-view", h.getLaborPlanView)
	})

	keyed(r).Post("/work-units/{id}/completions", h.postComplete)
	r.Get("/work-units", h.getWorkUnitsByReference)
	r.Get("/work-units/{id}", h.getWorkUnit)

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
		AllowedHeaders:   []string{"Content-Type", "Authorization", IdempotencyKeyHeader},
		AllowCredentials: false,
		MaxAge:           300,
	})
}
