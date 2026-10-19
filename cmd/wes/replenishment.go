package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// Environment of the pick-face replenishment stream (ADR-0042, ADR-0044).
const (
	replenishmentModeEnv        = "REPLENISHMENT_MODE"
	replenishmentGroupEnv       = "REPLENISHMENT_CONSUMER_GROUP"
	replenishmentCPTHorizonEnv  = "REPLENISHMENT_CPT_HORIZON"
	replenishmentUrgentCPTHzEnv = "REPLENISHMENT_URGENT_CPT_HORIZON"
)

// replenishmentModeKafka is the only REPLENISHMENT_MODE a binary runs in: the
// consumer of warehouse.pick-face-replenishment.events is always on (ADR-0044).
// The variable keeps its name because infra sets it; unset means kafka, and
// permissive/off are boot errors.
const replenishmentModeKafka = "kafka"

// replenishmentConfig is the environment of the REPLENISH-work consumer.
type replenishmentConfig struct {
	// mode is REPLENISHMENT_MODE: unset or kafka.
	mode string
	// groupID is REPLENISHMENT_CONSUMER_GROUP: the STABLE consumer group
	// every replica shares (the planned work is shared state in Postgres).
	// Mandatory.
	groupID      string
	kafkaBrokers string
}

func loadReplenishmentConfig(kafkaBrokers string) replenishmentConfig {
	return replenishmentConfig{
		mode:         os.Getenv(replenishmentModeEnv),
		groupID:      os.Getenv(replenishmentGroupEnv),
		kafkaBrokers: kafkaBrokers,
	}
}

// replenishmentWiring is what the config selected; enabled is always true for
// a validated config and false only for a zero value (a unit-test fixture).
type replenishmentWiring struct {
	enabled bool
	groupID string
}

// validate is the boot check of REPLENISHMENT_MODE / REPLENISHMENT_CONSUMER_GROUP
// (ADR-0044); every error names the variable. run() calls it before any dial.
func (c replenishmentConfig) validate() error {
	mode := strings.ToLower(strings.TrimSpace(c.mode))
	if mode == "permissive" || mode == "off" {
		return fmt.Errorf("%s=%s is not supported: this context runs on real Kafka (ADR-0044); unset it or set %s", replenishmentModeEnv, mode, replenishmentModeKafka)
	}
	if mode != "" && mode != replenishmentModeKafka {
		return fmt.Errorf("%s=%q is not supported: this context runs on real Kafka, the only value is %s (ADR-0044)", replenishmentModeEnv, c.mode, replenishmentModeKafka)
	}
	if strings.TrimSpace(c.groupID) == "" {
		return errors.New("REPLENISHMENT_CONSUMER_GROUP is required: the replenishment consumer is always on (a stable consumer group shared by every replica)")
	}
	if strings.TrimSpace(c.kafkaBrokers) == "" {
		return errors.New("KAFKA_BROKERS is required by the replenishment consumer")
	}
	return nil
}

// buildReplenishment resolves REPLENISHMENT_MODE (ADR-0042, ADR-0044): the
// consumer always runs. A config that cannot run on real Kafka is a boot error.
func buildReplenishment(cfg replenishmentConfig, logger *slog.Logger) (replenishmentWiring, error) {
	if err := cfg.validate(); err != nil {
		return replenishmentWiring{}, err
	}
	logger.Info("replenishment planning configured", "mode", replenishmentModeKafka, "topic", cloudevents.TopicPickFaceReplenishmentEvents, "group_id", cfg.groupID)
	return replenishmentWiring{enabled: true, groupID: cfg.groupID}, nil
}

// replenishmentCPTHorizons reads REPLENISHMENT_CPT_HORIZON (default 8h) and
// REPLENISHMENT_URGENT_CPT_HORIZON (default 1h). Unset, malformed or
// non-positive values fall back and never fail the boot; the use case caps
// the urgent horizon at the normal one.
func replenishmentCPTHorizons() (normal, urgent time.Duration) {
	return durationEnv(replenishmentCPTHorizonEnv, usecases.DefaultReplenishmentCPTHorizon),
		durationEnv(replenishmentUrgentCPTHzEnv, usecases.DefaultReplenishmentUrgentCPTHorizon)
}

// startReplenishmentConsumer starts the ReplenishmentRequested /
// ReplenishmentCancelled consumer and records how to stop it. A no-op only for
// a zero-value wiring (a unit-test fixture): run() always enables it.
func (s *serving) startReplenishmentConsumer() {
	if !s.replenishment.enabled {
		return
	}
	normal, urgent := replenishmentCPTHorizons()
	requested := usecases.NewApplyReplenishmentRequested(s.repos.workUnits, s.repos.pools, s.repos.processedEvts, s.catalogue, s.publisher, s.clock).
		WithUnitOfWork(s.repos.uow).WithCPTHorizons(normal, urgent)
	cancelled := usecases.NewApplyReplenishmentCancelled(s.repos.workUnits, s.repos.pools, s.repos.processedEvts, s.clock).WithUnitOfWork(s.repos.uow)
	consumer := inboundkafka.NewReplenishmentConsumer(brokerList(s.kafkaBrokers), s.replenishment.groupID, requested, cancelled, s.logger)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.consumerStopped(ctx, "replenishment consumer", consumer.Run(ctx))
	}()
	s.logger.Info("consuming replenishment requests", "topic", cloudevents.TopicPickFaceReplenishmentEvents, "group_id", s.replenishment.groupID)
	s.stopReplenishment = func(waitCtx context.Context) {
		cancel()
		waitDone(waitCtx, s.logger, "replenishment consumer", done)
		_ = consumer.Close()
	}
}

// stopReplenishmentConsumer cancels the consumer, waits (bounded by ctx) for
// in-flight handling to finish, and closes it. Safe to call when it was never
// started or was already stopped.
func (s *serving) stopReplenishmentConsumer(ctx context.Context) {
	if s.stopReplenishment == nil {
		return
	}
	stop := s.stopReplenishment
	s.stopReplenishment = nil
	stop(ctx)
}

// stopReplenishmentConsumerBounded is stopReplenishmentConsumer with its own
// deadline, for the error exit path of run.
func (s *serving) stopReplenishmentConsumerBounded() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.stopReplenishmentConsumer(ctx)
}
