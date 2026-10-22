package main

import (
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
)

// wireEventPublisher builds the outbound event publisher: the integration
// publisher (warehouse.work-planning.events) is untouched while a SEPARATE
// analytics publisher fans every domain event onto the dedicated analytics
// topic (warehouse.wes.analytics) that feeds the "Release Throughput & Backlog
// Health" data product (ADR-0011). Both act only as ENCODERS inside the use
// case's transaction -- one outbox row per event per topic (ADR-0014) -- and
// the returned relay (always built) drains those rows onto Kafka through a
// single topic-less writer, so the store and the topics cannot diverge.
//
// There is no log sink and no direct-to-Kafka path (ADR-0044): a nil pool or an
// empty broker list is an error. The returned stop func releases every Kafka
// writer opened; the caller defers it.
func wireEventPublisher(logger *slog.Logger, kafkaBrokers string, workUnits ports.WorkUnitRepo, classifications ports.ProductClassificationLookup, pgPool *pgxpool.Pool) (ports.EventPublisher, *postgres.OutboxRelay, func(), error) {
	if pgPool == nil {
		return nil, nil, func() {}, errors.New("event publisher: a Postgres pool is required (DATABASE_URL): events leave only through the transactional outbox")
	}
	brokers := brokerList(kafkaBrokers)
	if len(brokers) == 0 {
		return nil, nil, func() {}, errors.New("event publisher: KAFKA_BROKERS is required")
	}
	integrationPublisher := outboundkafka.NewPublisher(brokers, workUnits, classifications, newEventID)
	analyticsPublisher := outboundkafka.NewAnalyticsPublisher(brokers, newEventID).WithWorkUnits(workUnits)
	closers := []func(){func() { _ = integrationPublisher.Close() }, func() { _ = analyticsPublisher.Close() }}

	relay, relayClosers := wireOutboxRelay(logger, brokers, pgPool)
	closers = append(closers, relayClosers...)
	publisher := postgres.NewOutboxPublisher(pgPool, integrationPublisher, analyticsPublisher)
	logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox", "brokers", kafkaBrokers)
	return publisher, relay, func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}, nil
}

// wireOutboxRelay builds the relay that drains outbox_events onto Kafka
// through a single topic-less writer, plus the outbox-lag gauge. The relay
// is capped by OUTBOX_MAX_ATTEMPTS (default 20, 0 = unlimited; ADR-0039).
// The returned closers are in registration order (the caller runs them in
// reverse).
func wireOutboxRelay(logger *slog.Logger, brokers []string, pgPool *pgxpool.Pool) (*postgres.OutboxRelay, []func()) {
	sink := outboundkafka.NewRelaySink(brokers)
	closers := []func(){func() { _ = sink.Close() }}
	maxAttempts := nonNegativeIntEnv(outboxMaxAttemptsEnv, defaultOutboxMaxAttempts)
	relay := postgres.NewOutboxRelay(pgPool, sink, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)),
		postgres.WithMaxAttempts(maxAttempts))
	logger.Info("outbox relay configured", "outbox_max_attempts", maxAttempts)
	// Outbox lag (ADR-0014 follow-up): age of the oldest unpublished row.
	// Unregistered before the pool is closed so the callback never
	// touches a closed pool.
	if lagReg, lagErr := postgres.RegisterOutboxLagGauge(pgPool); lagErr != nil {
		logger.Warn("outbox lag gauge unavailable", "error", lagErr)
	} else {
		closers = append(closers, func() { _ = lagReg.Unregister() })
	}
	return relay, closers
}
