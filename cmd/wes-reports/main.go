// Command wes-reports is the READER composition root of the work-planning
// "Release Throughput & Backlog Health" data product. It opens the analytical
// Postgres database over a read-only pool and serves the throughput report and
// its freshness over REST. It writes nothing: the writer (cmd/wes-projector)
// is a separate deployable and owns the schema (ADR-0011).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/wes-work-planning/internal/bootretry"
)

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is unset.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

// serviceName is this process's identity in OTel resource attributes;
// OTEL_SERVICE_NAME can override it.
const serviceName = "wes-reports"

func main() {
	if err := run(); err != nil {
		slog.Error("wes-reports exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	rootCtx := context.Background()
	otelServiceName := getenv("OTEL_SERVICE_NAME", serviceName)
	shutdownTelemetry, err := telemetry.Setup(
		rootCtx,
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

	httpAddr := getenv("HTTP_ADDR", ":8092")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}

	// Read-only pool: even a bug in the reader cannot mutate the read model,
	// on top of the read-only database role ANALYTICS_DATABASE_URL should use.
	pool, err := analyticsstore.NewReadOnlyPool(rootCtx, analyticsURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Retried, because in this fleet EVERY injected pod's first outbound
	// TCP dial is reset ~10s after the app starts (Istio native
	// sidecars; see internal/bootretry's package doc comment).
	// analyticsstore.NewReadOnlyPool does not itself dial, so without
	// this retried Ping the reset would surface inside the first real
	// request rather than at boot.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer bootCancel()
	if err := bootretry.Retry(bootCtx, logger, "ping analytics postgres", func() error {
		return pool.Ping(bootCtx)
	}); err != nil {
		return err
	}
	if err := analyticsstore.RecordPoolStats(pool); err != nil {
		logger.Error("analytics pgxpool metrics unavailable", "error", err)
	}

	handlers := &inboundhttp.ReportsHandlers{Store: analyticsstore.NewPostgresReport(pool)}
	router := inboundhttp.NewReportsRouter(handlers, otelServiceName, logger)

	srv := &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("reports server listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("reports server failed", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

// newLogger builds the process-wide structured logger: JSON to stdout, at the
// level LOG_LEVEL names, routed through telemetry.NewTraceHandler so a log
// emitted inside a span carries that span's trace_id and span_id.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
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
