// Command wes-projector is the WRITER composition root of the work-planning
// "Release Throughput & Backlog Health" data product. It consumes the
// analytics Kafka topic, projects each event into the analytical Postgres
// database via the idempotent PostgresProjection, and serves only a health
// endpoint on an admin port. It is the single writer of the analytical
// database and serves no reports; the reader (cmd/wes-reports) is a separate
// deployable (ADR-0011).
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

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/wes-work-planning/internal/bootretry"
)

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is unset:
// the projector is the writer of the analytical database and cannot start
// without it.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

// serviceName is this process's identity in OTel resource attributes;
// OTEL_SERVICE_NAME can override it.
const serviceName = "wes-projector"

func main() {
	if err := run(); err != nil {
		slog.Error("wes-projector exited with error", "error", err)
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

	adminAddr := getenv("ADMIN_ADDR", ":8091")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}
	kafkaBrokers := brokerList(getenv("KAFKA_BROKERS", "localhost:9092"))
	migrationsPath := getenv("ANALYTICS_MIGRATIONS_PATH", "migrations/analytics")

	// The projector owns the analytical schema: run its migrations on start.
	// Retried, because in this fleet EVERY injected pod's first outbound
	// TCP dial is reset ~10s after the app starts (Istio native
	// sidecars; see internal/bootretry's package doc comment). A single
	// attempt turns that known, transient condition into
	// CrashLoopBackOff before this process ever gets far enough to
	// serve its own /healthz.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer bootCancel()
	if err := bootretry.Retry(bootCtx, logger, "run analytics migrations", func() error {
		return analyticsstore.Migrate(analyticsURL, migrationsPath)
	}); err != nil {
		return err
	}

	pool, err := analyticsstore.NewPool(rootCtx, analyticsURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	// analyticsstore.NewPool does not itself dial; without this retried
	// Ping the same first-dial reset would surface inside the first
	// real query instead of at boot.
	if err := bootretry.Retry(bootCtx, logger, "ping analytics postgres", func() error {
		return pool.Ping(bootCtx)
	}); err != nil {
		return err
	}
	if err := analyticsstore.RecordPoolStats(pool); err != nil {
		logger.Error("analytics pgxpool metrics unavailable", "error", err)
	}

	projection := analyticsstore.NewPostgresProjection(pool)
	consumed := analyticsstore.NewConsumedEventsRepo(pool)
	consumer := inboundkafka.NewAnalyticsConsumer(kafkaBrokers, outboundkafka.AnalyticsTopic, projection, consumed, logger)
	defer func() { _ = consumer.Close() }()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{Addr: adminAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("projector admin server listening", "addr", adminAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("projector admin server failed", "error", err)
		}
	}()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	go func() {
		logger.Info("analytics consumer starting", "topic", outboundkafka.AnalyticsTopic, "group", inboundkafka.AnalyticsConsumerGroup, "brokers", kafkaBrokers)
		if err := consumer.Run(consumerCtx); err != nil {
			logger.Error("analytics consumer stopped", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	cancelConsumer()

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

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
