package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
)

// wireEventPublisher selects the outbound event publisher. The default
// is the log publisher; with EVENT_PUBLISHER=kafka the integration
// publisher (warehouse.work-planning.events) is untouched while a
// SEPARATE analytics publisher fans every domain event onto the
// dedicated analytics topic (warehouse.wes.analytics) that feeds the
// "Release Throughput & Backlog Health" data product (ADR-0011). With
// Postgres configured both act only as ENCODERS inside the use case's
// transaction — one outbox row per event per topic (ADR-0014) — and the
// returned relay drains those rows onto Kafka through a single
// topic-less writer, so the store and the topics can no longer diverge;
// without Postgres a MultiPublisher emits each event to BOTH topics
// directly. The returned stop func releases every Kafka writer opened;
// the caller defers it.
func wireEventPublisher(logger *slog.Logger, eventPublisherKind, kafkaBrokers string, workUnits ports.WorkUnitRepo, classifications ports.ProductClassificationLookup, pgPool *pgxpool.Pool) (ports.EventPublisher, *postgres.OutboxRelay, func(), error) {
	if eventPublisherKind != "kafka" {
		logger.Info("event publisher configured", "publisher", "log")
		return events.NewLogPublisher(logger), nil, func() {}, nil
	}
	if kafkaBrokers == "" {
		return nil, nil, func() {}, fmt.Errorf("EVENT_PUBLISHER=kafka requires KAFKA_BROKERS to be set")
	}
	brokers := brokerList(kafkaBrokers)
	integrationPublisher := outboundkafka.NewPublisher(brokers, workUnits, classifications, newEventID)
	analyticsPublisher := outboundkafka.NewAnalyticsPublisher(brokers, newEventID)
	closers := []func(){func() { _ = integrationPublisher.Close() }, func() { _ = analyticsPublisher.Close() }}

	var relay *postgres.OutboxRelay
	var publisher ports.EventPublisher
	if pgPool != nil {
		var relayClosers []func()
		relay, relayClosers = wireOutboxRelay(logger, brokers, pgPool)
		closers = append(closers, relayClosers...)
		publisher = postgres.NewOutboxPublisher(pgPool, integrationPublisher, analyticsPublisher)
		logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox", "brokers", kafkaBrokers)
	} else {
		// No Postgres, no transaction to bind to: publish directly. A
		// MultiPublisher emits each event to BOTH topics, exactly once
		// each, without either publisher knowing about the other.
		publisher = events.NewMultiPublisher(integrationPublisher, analyticsPublisher)
		logger.Info("event publisher configured", "publisher", "kafka", "mode", "direct", "brokers", kafkaBrokers)
	}
	return publisher, relay, func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}, nil
}

// wireOutboxRelay builds the relay that drains outbox_events onto Kafka
// through a single topic-less writer, plus the outbox-lag gauge. The
// returned closers are in registration order (the caller runs them in
// reverse).
func wireOutboxRelay(logger *slog.Logger, brokers []string, pgPool *pgxpool.Pool) (*postgres.OutboxRelay, []func()) {
	sink := outboundkafka.NewRelaySink(brokers)
	closers := []func(){func() { _ = sink.Close() }}
	relay := postgres.NewOutboxRelay(pgPool, sink, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)))
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
