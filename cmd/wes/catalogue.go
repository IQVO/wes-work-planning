package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/filecatalog"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafkacatalog"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/bootretry"
)

// wireCatalogue resolves the process-path catalogue from its selectable
// SOURCE, defaulting to the existing boot-time file read ("file") --
// zero behavior change for any existing deployment unless
// PATH_CATALOGUE_SOURCE=kafka is explicitly set, matching this fleet's
// EVENT_PUBLISHER convention. See internal/adapters/outbound/kafkacatalog's
// package doc comment for the full rationale and the readiness-gate
// design, mirrored byte-for-byte from fulfillment-execution's identical
// wiring. catalogueConsumerCtx must already be live for the kafka mode's
// consumer goroutine.
func wireCatalogue(kafkaBrokers string, catalogueConsumerCtx context.Context, logger *slog.Logger) (ports.PathCatalogue, *kafkacatalog.Consumer, error) {
	switch getenv("PATH_CATALOGUE_SOURCE", "file") {
	case "kafka":
		return wireKafkaCatalogue(kafkaBrokers, catalogueConsumerCtx, logger)
	default:
		return wireFileCatalogue(logger)
	}
}

// wireKafkaCatalogue dials the catalogue topic, starts its consumer
// goroutine, and blocks until the initial history has been replayed.
func wireKafkaCatalogue(kafkaBrokers string, catalogueConsumerCtx context.Context, logger *slog.Logger) (ports.PathCatalogue, *kafkacatalog.Consumer, error) {
	if kafkaBrokers == "" {
		return nil, nil, fmt.Errorf("PATH_CATALOGUE_SOURCE=kafka requires KAFKA_BROKERS to be set")
	}
	// Retried: this fleet's Istio native sidecars reset EVERY
	// injected pod's first outbound TCP dial ~10s after the app
	// starts, and NewConsumer's newTargetOffsets dials the broker
	// directly before anything else runs. A single attempt turns
	// that known, transient reset into CrashLoopBackOff exactly
	// like the equivalent, unretried Postgres dial did (see
	// internal/bootretry's package doc comment).
	var kafkaCatalogue *kafkacatalog.Consumer
	if err := bootretry.Retry(context.Background(), logger, "connect to the process-path catalogue topic", func() error {
		var dialErr error
		kafkaCatalogue, dialErr = kafkacatalog.NewConsumer(context.Background(), brokerList(kafkaBrokers), logger)
		return dialErr
	}); err != nil {
		return nil, nil, fmt.Errorf("failed to start the Kafka-sourced process-path catalogue: %w", err)
	}
	logger.Info("process-path catalogue source configured", "source", "kafka", "topic", kafkacatalog.Topic)
	go func() {
		logger.Info("process-path catalogue consumer running", "topic", kafkacatalog.Topic)
		if err := kafkaCatalogue.Run(catalogueConsumerCtx); err != nil {
			logger.Error("process-path catalogue consumer stopped", "error", err)
		}
	}()

	logger.Info("waiting for the process-path catalogue to replay its initial history before accepting traffic")
	waitCtx, waitCancel := context.WithTimeout(context.Background(), kafkacatalog.WaitReadyTimeout)
	err := kafkaCatalogue.WaitReady(waitCtx)
	waitCancel()
	if err != nil {
		return nil, nil, fmt.Errorf("process-path catalogue did not become ready within %s: %w", kafkacatalog.WaitReadyTimeout, err)
	}
	logger.Info("process-path catalogue is ready", "paths", kafkaCatalogue.Ids())
	return kafkaCatalogue, kafkaCatalogue, nil
}

// wireFileCatalogue loads the catalogue from PATH_CATALOGUE_FILE.
//
// The process-path catalogue is loaded and validated once at boot, before
// anything else stands up — a missing or malformed catalogue file must
// stop this service from starting at all, never fall back to a
// partial/empty catalogue (mirrors fulfillment-execution's identical
// boot-time contract; see ADR-0017 there and this service's own
// ADR-0012).
func wireFileCatalogue(logger *slog.Logger) (ports.PathCatalogue, *kafkacatalog.Consumer, error) {
	fileCatalogue, err := filecatalog.Load(getenv("PATH_CATALOGUE_FILE", "/etc/wes-work-planning/process-paths.yaml"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load the process-path catalogue: %w", err)
	}
	logger.Info("process-path catalogue loaded", "paths", fileCatalogue.Ids())
	return fileCatalogue, nil, nil
}
