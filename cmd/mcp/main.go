// Command mcp is the composition root for the wes-work-planning MCP server:
// it wires env config to outbound adapters, adapters to the use cases, and
// those to the inbound MCP adapter, then serves MCP over Streamable HTTP. It
// is a second, independent deployable alongside cmd/wes (the HTTP service),
// per ADR-0008.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	inboundmcp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassification"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/bootretry"
	"github.com/claudioed/wes-work-planning/internal/resilience"
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

	repos, err := wireRepositories(logger, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer repos.close()

	publisher, stopPublisher, err := wireEventPublisher(logger, getenv("EVENT_PUBLISHER", "log"), os.Getenv("KAFKA_BROKERS"), repos, classificationLookup(logger))
	if err != nil {
		return err
	}
	defer stopPublisher()

	server := inboundmcp.NewServer(newMCPServerDeps(logger, repos, publisher))

	srv := &http.Server{
		Addr:              getenv("MCP_ADDR", ":8090"),
		Handler:           newRouter(inboundmcp.Handler(server), otelServiceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return serveMCP(logger, srv)
}

// repositories groups the environment-selected persistence adapters the MCP
// server needs. pool and uow are nil in the in-memory configuration; a nil
// UnitOfWork makes every use case run Save + Publish back to back (ADR-0014),
// with Postgres they run in ONE transaction together with the outbox rows.
type repositories struct {
	pools     ports.WorkPoolRepo
	workUnits ports.WorkUnitRepo
	pool      *pgxpool.Pool
	uow       ports.UnitOfWork
	// close releases the Postgres pool (a no-op for the in-memory adapters).
	close func()
}

// wireRepositories selects the in-memory or Postgres work-pool/work-unit
// repositories exactly as cmd/wes does: in-memory unless DATABASE_URL is
// set. The returned close func must be deferred by the caller.
func wireRepositories(logger *slog.Logger, databaseURL string) (repositories, error) {
	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return repositories{pools: memory.NewWorkPoolRepo(), workUnits: memory.NewWorkUnitRepo(), close: func() {}}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.Connect(ctx, databaseURL)
	if err != nil {
		return repositories{}, err
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
		return repositories{}, err
	}
	return repositories{
		pools:     postgres.NewWorkPoolRepo(pool),
		workUnits: postgres.NewWorkUnitRepo(pool),
		pool:      pool,
		uow:       postgres.NewUnitOfWork(pool),
		close:     pool.Close,
	}, nil
}

// classificationLookup wires the product-classification ACL exactly as
// cmd/wes does (PRODUCT_CLASSIFICATION_MODE / INVENTORY_STORAGE_BASE_URL),
// so a WorkReleased raised through MCP carries the same ADR-0009
// hazmat/fragile hints as one raised through REST.
func classificationLookup(logger *slog.Logger) ports.ProductClassificationLookup {
	breakerMetrics, err := telemetry.NewCircuitBreakerMetrics()
	if err != nil {
		logger.Warn("circuit breaker metrics not registered; continuing without them", "error", err)
	}
	return buildClassificationLookup(getenv("PRODUCT_CLASSIFICATION_MODE", "permissive"), os.Getenv("INVENTORY_STORAGE_BASE_URL"), breakerMetrics, logger)
}

// buildClassificationLookup mirrors cmd/wes's function of the same name
// (http|permissive, default permissive; ADR-0009, ADR-0023).
func buildClassificationLookup(mode, inventoryStorageBaseURL string, recorder resilience.StateRecorder, logger *slog.Logger) ports.ProductClassificationLookup {
	if !strings.EqualFold(mode, "http") {
		return productclassification.NewPermissiveLookup()
	}
	logger.Info("product classification lookup configured", "mode", "http", "inventory_storage_base_url", inventoryStorageBaseURL)
	return productclassification.NewBreakerClient(productclassification.NewClient(inventoryStorageBaseURL, nil), recorder)
}

// wireEventPublisher selects the MCP server's event publisher with the SAME
// EVENT_PUBLISHER / DATABASE_URL / KAFKA_BROKERS contract as cmd/wes
// (ADR-0008, ADR-0014): a WorkReleased raised by the release_next_work tool
// must reach fulfillment-execution exactly like one raised over REST.
//
//   - EVENT_PUBLISHER unset or "log": events are only logged (local dev).
//   - "kafka" + Postgres: an OutboxPublisher writes one outbox_events row per
//     event per topic inside the use case's UnitOfWork. The outbox RELAY stays
//     in cmd/wes — it drains every row regardless of which process wrote it —
//     so this process never opens a Kafka writer in that mode.
//   - "kafka" without Postgres: events go straight to both topics.
//
// The returned stop func releases any Kafka writer opened.
func wireEventPublisher(logger *slog.Logger, kind, kafkaBrokers string, repos repositories, classifications ports.ProductClassificationLookup) (ports.EventPublisher, func(), error) {
	if kind != "kafka" {
		logger.Info("event publisher configured", "publisher", "log")
		return events.NewLogPublisher(logger), func() {}, nil
	}
	if kafkaBrokers == "" {
		return nil, func() {}, fmt.Errorf("EVENT_PUBLISHER=kafka requires KAFKA_BROKERS to be set")
	}
	brokers := brokerList(kafkaBrokers)
	integrationPublisher := outboundkafka.NewPublisher(brokers, repos.workUnits, classifications, newEventID)
	analyticsPublisher := outboundkafka.NewAnalyticsPublisher(brokers, newEventID)
	stop := func() {
		_ = integrationPublisher.Close()
		_ = analyticsPublisher.Close()
	}
	if repos.pool != nil {
		logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox", "brokers", kafkaBrokers)
		return postgres.NewOutboxPublisher(repos.pool, integrationPublisher, analyticsPublisher), stop, nil
	}
	logger.Info("event publisher configured", "publisher", "kafka", "mode", "direct", "brokers", kafkaBrokers)
	return events.NewMultiPublisher(integrationPublisher, analyticsPublisher), stop, nil
}

// newMCPServerDeps wires the MCP adapter's Deps over the SAME use cases
// the HTTP adapter uses: SampleBacklog and RebalanceDecision (read) and
// ReleaseNextWork (write), each in the repositories' UnitOfWork (nil for
// in-memory) so the state change and its events commit together (ADR-0014).
// publisher is whatever wireEventPublisher chose — the MCP server publishes
// through the platform's outbox exactly like cmd/wes does.
//
// When the wes-reports REST service is reachable, this also exposes the
// curated, read-only "Release Throughput & Backlog Health" report tool. It
// calls that REST surface rather than opening the analytical database
// directly, so no process touches a datastore it does not own (ADR-0011).
// Absent REPORTS_BASE_URL the tool is simply not registered.
func newMCPServerDeps(logger *slog.Logger, repos repositories, publisher ports.EventPublisher) inboundmcp.Deps {
	clock := memory.SystemClock{}
	deps := inboundmcp.Deps{
		SampleBacklog:     usecases.NewSampleBacklog(repos.pools, publisher, clock).WithUnitOfWork(repos.uow),
		RebalanceDecision: usecases.NewRebalanceDecision(repos.pools, publisher, clock).WithUnitOfWork(repos.uow),
		ReleaseNextWork:   usecases.NewReleaseNextWork(repos.pools, repos.workUnits, publisher, clock).WithUnitOfWork(repos.uow),
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

// newRouter wraps the MCP handler in the process's HTTP surface:
//
//   - GET /healthz answers 200 {"status":"ok"} so the Kubernetes
//     liveness/readiness probes can observe the process. It leaks nothing:
//     no tool, resource, or state.
//   - The MCP Streamable HTTP endpoint is mounted at BOTH "/" (the original
//     root mount) and "/mcp" (warehouse-ops-agent's *_MCP_ENDPOINT convention
//     and the docs' examples).
//
// The MCP surface is unauthenticated by decision (ADR-0016). Every request
// gets a server span named after its chi route pattern plus the semconv
// http.server.request.duration histogram, the same instrumentation the REST
// router applies (ADR-0013). serviceName names the server in those
// attributes.
func newRouter(mcpHandler http.Handler, serviceName string) http.Handler {
	r := chi.NewRouter()
	metricCfg := otelchimetric.NewBaseConfig(serviceName)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(otelchimetric.NewServerActiveRequests(metricCfg))
	r.Get("/healthz", healthz)
	r.Handle("/", mcpHandler)
	r.Handle("/mcp", mcpHandler)
	return r
}

// healthz is the liveness/readiness endpoint.
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

func brokerList(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// newEventID generates a UUID v4 for outbound integration event envelopes.
func newEventID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
