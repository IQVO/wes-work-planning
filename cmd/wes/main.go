// Command wes is the composition root: it wires config from env into
// adapters, use cases, and the HTTP router, then serves.
//
// The package is split by concern: config.go (env + logger helpers),
// catalogue.go, repositories.go, publisher.go, handlers.go, housekeeper.go
// and serving.go (server lifecycle + graceful shutdown). main.go only
// sequences them -- startup order here is also the reverse of teardown.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
)

// serviceName is this service's identity in OTel resource attributes and
// span/metric scopes; OTEL_SERVICE_NAME can override it.
const serviceName = "wes-work-planning"

func main() {
	if err := run(); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	cfg := loadConfig()

	// catalogueConsumerCtx/cancelCatalogueConsumer are declared here
	// because the Kafka catalogue source needs its own Run goroutine
	// started BEFORE WaitReady is called inside wireCatalogue --
	// otherwise nothing would ever be consuming messages while this
	// process waits, guaranteeing a deadlock until WaitReadyTimeout.
	catalogueConsumerCtx, cancelCatalogueConsumer := context.WithCancel(context.Background())
	defer cancelCatalogueConsumer()

	catalogue, kafkaCatalogue, err := wireCatalogue(cfg.kafkaBrokers, catalogueConsumerCtx, logger)
	if err != nil {
		return err
	}

	stopTelemetry, err := setupTelemetry(cfg.otelServiceName, logger)
	if err != nil {
		return err
	}
	defer stopTelemetry()

	repos, err := wireRepositories(cfg.databaseURL, cfg.migrationsDatabaseURL, cfg.migrationsPath, logger)
	if err != nil {
		return err
	}
	if repos.pgPool != nil {
		defer repos.pgPool.Close()
	}

	publisher, relay, stopPublisher, err := wireEventPublisher(logger, cfg.eventPublisherKind, cfg.kafkaBrokers, repos.workUnits, repos.classificationLookup(logger), repos.pgPool)
	if err != nil {
		return err
	}
	defer stopPublisher()

	return newServing(servingDeps{
		cfg:                     cfg,
		logger:                  logger,
		repos:                   repos,
		publisher:               publisher,
		relay:                   relay,
		catalogue:               catalogue,
		kafkaCatalogue:          kafkaCatalogue,
		cancelCatalogueConsumer: cancelCatalogueConsumer,
	}).run()
}

// setupTelemetry starts the OTel pipeline and returns the func that
// flushes it at exit.
func setupTelemetry(otelServiceName string, logger *slog.Logger) (stop func(), err error) {
	shutdown, err := telemetry.Setup(
		context.Background(),
		otelServiceName,
		getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion),
		getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint),
	)
	if err != nil {
		return nil, err
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// A failed final flush means the Collector was unreachable, not that
		// the service failed — log it and let the process exit cleanly.
		if err := shutdown(ctx); err != nil {
			logger.Warn("telemetry shutdown did not flush cleanly", "error", err)
		}
	}, nil
}
