package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// packViewGroupEnv is the STABLE consumer group of the pack-level consumer
// (ADR-0045). It must differ from PRODUCT_CLASSIFICATION_CONSUMER_GROUP: both
// read warehouse.product-master.events, and a group that already committed
// past the pack events could never replay them.
const packViewGroupEnv = "PACK_VIEW_CONSUMER_GROUP"

// packViewConfig is the environment of the pack-level consumer.
type packViewConfig struct {
	// groupID is PACK_VIEW_CONSUMER_GROUP: the STABLE consumer group every
	// replica shares (the pack copy is shared state in Postgres). Mandatory.
	groupID string
	// classificationGroupID is PRODUCT_CLASSIFICATION_CONSUMER_GROUP, only to
	// refuse a collision with it.
	classificationGroupID string
	kafkaBrokers          string
}

func loadPackViewConfig(kafkaBrokers string) packViewConfig {
	return packViewConfig{
		groupID:               os.Getenv(packViewGroupEnv),
		classificationGroupID: os.Getenv("PRODUCT_CLASSIFICATION_CONSUMER_GROUP"),
		kafkaBrokers:          kafkaBrokers,
	}
}

// validate is the boot check of PACK_VIEW_CONSUMER_GROUP (ADR-0044,
// ADR-0045); every error names the variable. run() calls it before any dial.
func (c packViewConfig) validate() error {
	group := strings.TrimSpace(c.groupID)
	if group == "" {
		return errors.New(packViewGroupEnv + " is required: the pack-level consumer is always on (a stable consumer group shared by every replica, distinct from PRODUCT_CLASSIFICATION_CONSUMER_GROUP)")
	}
	if group == strings.TrimSpace(c.classificationGroupID) {
		return errors.New(packViewGroupEnv + " must differ from PRODUCT_CLASSIFICATION_CONSUMER_GROUP: both read warehouse.product-master.events and the classification group has already committed past the pack-level events")
	}
	if strings.TrimSpace(c.kafkaBrokers) == "" {
		return errors.New("KAFKA_BROKERS is required by the pack-level consumer")
	}
	return nil
}

// startPackLevelConsumer starts the ProductPackLevelDeclared / Retired /
// Measured consumer and records how to stop it. A no-op only for a zero-value
// wiring (a unit-test fixture): run() always wires a store.
func (s *serving) startPackLevelConsumer() {
	if s.classification.packs == nil {
		return
	}
	observe := usecases.NewObservePackLevel(s.classification.packs, s.repos.processedEvts).WithUnitOfWork(s.repos.uow)
	consumer := inboundkafka.NewPackLevelConsumer(brokerList(s.kafkaBrokers), s.packViewGroupID, observe, s.logger)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.consumerStopped(ctx, "pack level consumer", consumer.Run(ctx))
	}()
	s.logger.Info("consuming product pack levels", "topic", cloudevents.TopicProductMasterEvents, "group_id", s.packViewGroupID)
	s.stopPackLevel = func(waitCtx context.Context) {
		cancel()
		waitDone(waitCtx, s.logger, "pack level consumer", done)
		_ = consumer.Close()
	}
}

// stopPackLevelConsumer cancels the consumer, waits (bounded by ctx) for
// in-flight handling to finish, and closes it. Safe to call when it was never
// started or was already stopped.
func (s *serving) stopPackLevelConsumer(ctx context.Context) {
	if s.stopPackLevel == nil {
		return
	}
	stop := s.stopPackLevel
	s.stopPackLevel = nil
	stop(ctx)
}

// stopPackLevelConsumerBounded is stopPackLevelConsumer with its own deadline,
// for the error exit path of run.
func (s *serving) stopPackLevelConsumerBounded() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.stopPackLevelConsumer(ctx)
}

// logPackView records the wiring at boot.
func logPackView(logger *slog.Logger, groupID string) {
	logger.Info("pack level copy configured", "topic", cloudevents.TopicProductMasterEvents, "group_id", groupID)
}
