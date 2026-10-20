package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// classificationConfig is the environment that selects how this process
// learns product classifications (ADR-0035, ADR-0044).
type classificationConfig struct {
	// mode is PRODUCT_CLASSIFICATION_MODE: unset or kafka; permissive/off are
	// boot errors.
	mode string
	// groupID is PRODUCT_CLASSIFICATION_CONSUMER_GROUP: the STABLE consumer
	// group every replica shares (the copy is shared state in Postgres).
	// Mandatory.
	groupID      string
	kafkaBrokers string
}

func loadClassificationConfig(kafkaBrokers string) classificationConfig {
	return classificationConfig{
		mode:         os.Getenv("PRODUCT_CLASSIFICATION_MODE"),
		groupID:      os.Getenv("PRODUCT_CLASSIFICATION_CONSUMER_GROUP"),
		kafkaBrokers: kafkaBrokers,
	}
}

// validate rejects every configuration that would not run on real Kafka, with
// an error that names the variable. run() calls it before any dial.
func (c classificationConfig) validate() (productclassificationcopy.Mode, error) {
	mode, err := productclassificationcopy.ParseMode(c.mode)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(c.groupID) == "" {
		return "", errors.New("PRODUCT_CLASSIFICATION_CONSUMER_GROUP is required: the ProductClassified consumer is always on (a stable consumer group shared by every replica)")
	}
	if strings.TrimSpace(c.kafkaBrokers) == "" {
		return "", errors.New("KAFKA_BROKERS is required by the ProductClassified consumer (PRODUCT_CLASSIFICATION_MODE=kafka)")
	}
	return mode, nil
}

// classificationWiring is what the mode selected: the lookup the WorkReleased
// encoder reads and the copy the consumer writes (the same Postgres store).
type classificationWiring struct {
	lookup  ports.ProductClassificationLookup
	copies  ports.ProductClassificationCopyRepo
	groupID string
}

// copyStore is an adapter that is both the lookup and the copy.
type copyStore interface {
	ports.ProductClassificationLookup
	ports.ProductClassificationCopyRepo
}

// buildClassification resolves PRODUCT_CLASSIFICATION_MODE (ADR-0035,
// ADR-0044): kafka is the only mode. The local copy is the Postgres store over
// pool (a nil pool is a boot error), read by the WorkReleased encoder and fed
// by the ProductClassified consumer. Requires
// PRODUCT_CLASSIFICATION_CONSUMER_GROUP and KAFKA_BROKERS.
func buildClassification(cfg classificationConfig, pool *pgxpool.Pool, logger *slog.Logger) (classificationWiring, error) {
	mode, err := cfg.validate()
	if err != nil {
		return classificationWiring{}, err
	}
	if pool == nil {
		return classificationWiring{}, errors.New("PRODUCT_CLASSIFICATION_MODE=kafka requires DATABASE_URL (Postgres): the classification copy is shared state and is never kept in memory")
	}
	var store copyStore = productclassificationcopy.NewStore(pool, logger)
	logger.Info("product classification lookup configured", "mode", string(mode), "topic", cloudevents.TopicProductMasterEvents, "group_id", cfg.groupID)
	return classificationWiring{lookup: store, copies: store, groupID: cfg.groupID}, nil
}

// startClassificationConsumer starts the ProductClassified consumer when a
// copy was wired, and records how to stop it. run() always wires one; the no-op
// is only for a zero-value fixture in unit tests.
func (s *serving) startClassificationConsumer() {
	if s.classification.copies == nil {
		return
	}
	observe := usecases.NewObserveProductClassification(s.classification.copies, s.repos.processedEvts).WithUnitOfWork(s.repos.uow)
	consumer := inboundkafka.NewProductClassificationConsumer(brokerList(s.kafkaBrokers), s.classification.groupID, observe, s.logger)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.consumerStopped(ctx, "product classification consumer", consumer.Run(ctx))
	}()
	s.logger.Info("consuming product classifications", "topic", cloudevents.TopicProductMasterEvents, "group_id", s.classification.groupID)
	s.stopClassification = func(waitCtx context.Context) {
		cancel()
		waitDone(waitCtx, s.logger, "product classification consumer", done)
		_ = consumer.Close()
	}
}

// stopClassificationConsumer cancels the consumer, waits (bounded by ctx)
// for in-flight handling to finish, and closes it. Safe to call when it was
// never started or was already stopped.
func (s *serving) stopClassificationConsumer(ctx context.Context) {
	if s.stopClassification == nil {
		return
	}
	stop := s.stopClassification
	s.stopClassification = nil
	stop(ctx)
}

// stopClassificationConsumerBounded is stopClassificationConsumer with its
// own deadline, for the error exit path of run.
func (s *serving) stopClassificationConsumerBounded() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.stopClassificationConsumer(ctx)
}
