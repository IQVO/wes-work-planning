package main

import (
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
)

// appConfig is the process-level configuration read from the environment
// once at boot. Component-specific knobs (housekeeping, circuit breakers,
// catalogue source, ...) are still read by the builder that owns them, so
// every env var keeps exactly the semantics it had before the split.
type appConfig struct {
	httpAddr    string
	databaseURL string
	// migrationsDatabaseURL, when set via MIGRATIONS_DATABASE_URL, is a
	// DIRECT (non-pooled, session-mode) Postgres connection string used
	// ONLY for the golang-migrate startup step in wireRepositories -- the
	// pgxpool opened right after migrations complete still uses
	// databaseURL unchanged, so every request this service serves keeps
	// going through PgBouncer exactly as before. See wireRepositories'
	// doc comment for the full "why": golang-migrate's postgres driver
	// takes a session-scoped `SELECT pg_advisory_lock($1)` to serialize
	// concurrent migration runs, which PgBouncer's transaction-pooling
	// mode does not support (warehouse-infra's PgBouncer rollout, PR
	// #43; this fallback closes the fleet-wide bug that rollout
	// introduced -- see ADR
	// 0026-migrations-direct-postgres-connection.md, mirroring
	// order-management ADR-0029). Falls back to databaseURL when unset,
	// which is every environment that doesn't provision the split
	// (local dev, CI integration tests, and any cluster whose Terraform
	// predates this fix) -- byte-identical to this service's behavior
	// before this change in that case.
	migrationsDatabaseURL string
	migrationsPath        string
	kafkaBrokers          string
	eventPublisherKind    string
	otelServiceName       string
}

// loadConfig reads the process-level environment.
func loadConfig() appConfig {
	databaseURL := os.Getenv("DATABASE_URL")
	return appConfig{
		httpAddr:              getenv("HTTP_ADDR", ":8080"),
		databaseURL:           databaseURL,
		migrationsDatabaseURL: getenv("MIGRATIONS_DATABASE_URL", databaseURL),
		migrationsPath:        getenv("MIGRATIONS_PATH", "migrations"),
		kafkaBrokers:          os.Getenv("KAFKA_BROKERS"),
		eventPublisherKind:    getenv("EVENT_PUBLISHER", "log"),
		otelServiceName:       getenv("OTEL_SERVICE_NAME", serviceName),
	}
}

// newLogger builds the process-wide structured logger: JSON to stdout, at
// the level LOG_LEVEL names (debug|info|warn|error, case-insensitive,
// default info). Records are routed through telemetry.TraceHandler so any
// log emitted inside a span carries that span's trace_id and span_id.
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

// defaultConsumerGroup is the group id every deployed instance of this
// service shares, so they cooperatively split the partitions of the topics
// below -- the normal, intended behaviour for a horizontally scaled service.
const defaultConsumerGroup = "wes-work-planning"

// consumerGroupID resolves the Kafka consumer group id, allowing
// KAFKA_CONSUMER_GROUP to override the default.
//
// This override exists for a specific, real failure: consumer-group offsets
// are shared infrastructure state, not per-process state. This fleet runs ONE
// Kafka broker platform-wide, so a second process started against it -- the
// e2e-tests harness's local binary, or a developer's `go run` -- joins the
// SAME group as the deployed Deployment when the id is fixed. With one
// partition per topic, Kafka's rebalance protocol awards that partition to
// exactly one member and the other silently consumes nothing, having been
// told it is healthy.
//
// Setting a unique id (e.g. wes-work-planning-e2e-$$) isolates such a process
// so it replays the topics itself instead of competing for them. Leaving it
// unset preserves the shared-group behaviour deployments rely on.
func consumerGroupID(override string) string {
	if strings.TrimSpace(override) != "" {
		return override
	}
	return defaultConsumerGroup
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// retentionEnv is durationEnv except that an explicit zero ("0", "0s")
// returns 0 — meaning "disabled" — instead of the fallback. An unset, invalid
// or negative value still yields the fallback.
func retentionEnv(key string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil && d == 0 && os.Getenv(key) != "" {
		return 0
	}
	return durationEnv(key, fallback)
}

// durationEnv parses key as a time.Duration, falling back on absence or a
// malformed/non-positive value: the relay interval is a tuning knob, not
// a contract, so it must never fail the boot.
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
