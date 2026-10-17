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

// Environment of the pick-face replenishment stream (ADR-0042).
const (
	replenishmentModeEnv        = "REPLENISHMENT_MODE"
	replenishmentGroupEnv       = "REPLENISHMENT_CONSUMER_GROUP"
	replenishmentCPTHorizonEnv  = "REPLENISHMENT_CPT_HORIZON"
	replenishmentUrgentCPTHzEnv = "REPLENISHMENT_URGENT_CPT_HORIZON"
)

// Replenishment modes: permissive (the default) consumes nothing, so a
// deployment that has not opted in plans no REPLENISH work; kafka consumes
// warehouse.pick-face-replenishment.events.
const (
	replenishmentModePermissive = "permissive"
	replenishmentModeKafka      = "kafka"
)

// replenishmentConfig is the environment that opts this process into
// planning REPLENISH work.
type replenishmentConfig struct {
	// mode is REPLENISHMENT_MODE: kafka | permissive (default).
	mode string
	// groupID is REPLENISHMENT_CONSUMER_GROUP: the STABLE consumer group
	// every replica shares (the planned work is shared state in Postgres).
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

// replenishmentWiring is what the mode selected; enabled is false in
// permissive mode.
type replenishmentWiring struct {
	enabled bool
	groupID string
}

// buildReplenishment resolves REPLENISHMENT_MODE (ADR-0042):
//
//   - permissive (or unset): no consumer.
//   - kafka: the consumer runs. Requires REPLENISHMENT_CONSUMER_GROUP and
//     KAFKA_BROKERS; without either it is a boot error.
//   - anything else: a boot error.
func buildReplenishment(cfg replenishmentConfig, logger *slog.Logger) (replenishmentWiring, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.mode))
	if mode == "" || mode == replenishmentModePermissive {
		logger.Info("replenishment planning configured", "mode", replenishmentModePermissive)
		return replenishmentWiring{}, nil
	}
	if mode != replenishmentModeKafka {
		return replenishmentWiring{}, fmt.Errorf("%s=%q is not one of %s, %s", replenishmentModeEnv, cfg.mode, replenishmentModeKafka, replenishmentModePermissive)
	}
	if strings.TrimSpace(cfg.groupID) == "" {
		return replenishmentWiring{}, errors.New("REPLENISHMENT_MODE=kafka requires REPLENISHMENT_CONSUMER_GROUP (a stable consumer group shared by every replica)")
	}
	if strings.TrimSpace(cfg.kafkaBrokers) == "" {
		return replenishmentWiring{}, errors.New("REPLENISHMENT_MODE=kafka requires KAFKA_BROKERS to be set")
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
// ReplenishmentCancelled consumer when REPLENISHMENT_MODE=kafka, and records
// how to stop it. A no-op otherwise.
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
		if err := consumer.Run(ctx); err != nil {
			s.logger.Error("replenishment consumer stopped", "error", err)
		}
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
