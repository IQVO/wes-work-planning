// Command mcp is the composition root for the wes-work-planning MCP server:
// it wires env config to outbound adapters, adapters to the use cases, and
// those to the inbound MCP adapter, then serves MCP over Streamable HTTP. It
// is a second, independent deployable alongside cmd/wes (the HTTP service),
// per ADR-0008.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	inboundmcp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/bootretry"
)

// serviceName is this server's identity in OTel resource attributes and
// span/metric scopes; OTEL_SERVICE_NAME can override it.
const serviceName = "wes-work-planning-mcp"

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Same non-blocking telemetry setup as the HTTP service: the exporters
	// dial lazily, so an unreachable Collector degrades to dropped telemetry,
	// never a server that won't start.
	otelServiceName := getenv("OTEL_SERVICE_NAME", serviceName)
	shutdownTelemetry, err := telemetry.Setup(
		context.Background(),
		otelServiceName,
		getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion),
		getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint),
	)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(ctx); err != nil {
			logger.Warn("telemetry shutdown did not flush cleanly", "error", err)
		}
	}()

	pools, workUnits, closeRepos, err := wireRepositories(logger, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer closeRepos()

	server := inboundmcp.NewServer(newMCPServerDeps(logger, pools, workUnits))

	srv := &http.Server{
		Addr:              getenv("MCP_ADDR", ":8090"),
		Handler:           newRouter(inboundmcp.Handler(server)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return serveMCP(logger, srv)
}

// wireRepositories selects the in-memory or Postgres work-pool/work-unit
// repositories exactly as cmd/wes does: in-memory unless DATABASE_URL is
// set. The returned close func releases the Postgres pool (a no-op for
// the in-memory adapters) and must be deferred by the caller.
func wireRepositories(logger *slog.Logger, databaseURL string) (pools ports.WorkPoolRepo, workUnits ports.WorkUnitRepo, closeRepos func(), err error) {
	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return memory.NewWorkPoolRepo(), memory.NewWorkUnitRepo(), func() {}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.Connect(ctx, databaseURL)
	if err != nil {
		return nil, nil, nil, err
	}
	// Retried, because in this fleet EVERY injected pod's first
	// outbound TCP dial is reset ~10s after the app starts (Istio
	// native sidecars; see internal/bootretry's package doc
	// comment), and pgxpool.NewWithConfig above does not itself
	// dial. Without this, that reset would surface inside the
	// first real request rather than at boot.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer bootCancel()
	if err := bootretry.Retry(bootCtx, logger, "ping postgres", func() error {
		return pool.Ping(bootCtx)
	}); err != nil {
		pool.Close()
		return nil, nil, nil, err
	}
	return postgres.NewWorkPoolRepo(pool), postgres.NewWorkUnitRepo(pool), pool.Close, nil
}

// newMCPServerDeps wires the MCP adapter's Deps over the SAME use cases
// the HTTP adapter uses: SampleBacklog and RebalanceDecision (read) and
// ReleaseNextWork (write). Those use cases need a publisher and clock;
// the MCP server is not the platform's primary event publisher (cmd/wes
// is), so it logs any event it raises rather than publishing to Kafka.
//
// When the wes-reports REST service is reachable, this also exposes the
// curated, read-only "Release Throughput & Backlog Health" report tool. It
// calls that REST surface rather than opening the analytical database
// directly, so no process touches a datastore it does not own (ADR-0011).
// Absent REPORTS_BASE_URL the tool is simply not registered.
func newMCPServerDeps(logger *slog.Logger, pools ports.WorkPoolRepo, workUnits ports.WorkUnitRepo) inboundmcp.Deps {
	clock := memory.SystemClock{}
	publisher := events.NewLogPublisher(logger)
	deps := inboundmcp.Deps{
		SampleBacklog:     usecases.NewSampleBacklog(pools, publisher, clock),
		RebalanceDecision: usecases.NewRebalanceDecision(pools, publisher, clock),
		ReleaseNextWork:   usecases.NewReleaseNextWork(pools, workUnits, publisher, clock),
	}
	if reportsBaseURL := os.Getenv("REPORTS_BASE_URL"); reportsBaseURL != "" {
		logger.Info("release throughput report tool enabled", "reports_base_url", reportsBaseURL)
		deps.Reports = inboundmcp.NewReportsRESTClient(reportsBaseURL, nil)
	}
	return deps
}

// serveMCP runs srv until SIGINT/SIGTERM or a listen error, returning the
// listen error or the graceful-shutdown result respectively.
func serveMCP(logger *slog.Logger, srv *http.Server) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
		logger.Info("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(ctx)
	}
}

// newRouter wraps the authenticated MCP handler in the process's HTTP surface:
//
//   - GET /healthz answers 200 {"status":"ok"} WITHOUT authentication, so the
//     Kubernetes liveness/readiness probes (which cannot carry a bearer key)
//     can observe the process. It leaks nothing: no tool, resource, or state.
//   - The MCP Streamable HTTP endpoint is mounted at BOTH "/" (the original
//     root mount) and "/mcp" (warehouse-ops-agent's *_MCP_ENDPOINT convention
//     and the docs' examples). Every other method/path on those routes still
//     goes through the bearer check inside the handler.
//
// chi matches the static /healthz route before the "/" handler, so the probe
// never reaches the auth middleware.
func newRouter(mcpHandler http.Handler) http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", healthz)
	r.Handle("/", mcpHandler)
	r.Handle("/mcp", mcpHandler)
	return r
}

// healthz is the unauthenticated liveness/readiness endpoint.
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// newLogger builds the process-wide structured logger: JSON to stdout, at the
// level LOG_LEVEL names (debug|info|warn|error, case-insensitive, default
// info). Records are routed through telemetry.NewTraceHandler so any log
// emitted inside a span carries that span's trace_id and span_id.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(telemetry.NewTraceHandler(handler))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
