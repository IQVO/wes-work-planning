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
// learns product classifications (ADR-0035).
type classificationConfig struct {
	// mode is PRODUCT_CLASSIFICATION_MODE: kafka | permissive (default).
	mode string
	// groupID is PRODUCT_CLASSIFICATION_CONSUMER_GROUP: the STABLE consumer
	// group every replica shares (the copy is shared state in Postgres).
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

// classificationWiring is what the mode selected: the lookup the WorkReleased
// encoder reads and, in kafka mode, the copy the consumer writes.
type classificationWiring struct {
	lookup ports.ProductClassificationLookup
	// copies is nil unless the ProductClassified consumer must run.
	copies  ports.ProductClassificationCopyRepo
	groupID string
}

// copyStore is an adapter that is both the lookup and the copy.
type copyStore interface {
	ports.ProductClassificationLookup
	ports.ProductClassificationCopyRepo
}

// buildClassification resolves PRODUCT_CLASSIFICATION_MODE (ADR-0035):
//
//   - permissive (or unset): the no-op lookup, no consumer.
//   - kafka: the local copy (Postgres over pool, in memory when pool is nil)
//     as the lookup, plus the consumer that feeds it. Requires
//     PRODUCT_CLASSIFICATION_CONSUMER_GROUP and KAFKA_BROKERS.
//   - anything else, including the retired http: a boot error.
func buildClassification(cfg classificationConfig, pool *pgxpool.Pool, logger *slog.Logger) (classificationWiring, error) {
	mode, err := productclassificationcopy.ParseMode(cfg.mode)
	if err != nil {
		return classificationWiring{}, err
	}
	if mode == productclassificationcopy.ModePermissive {
		logger.Info("product classification lookup configured", "mode", string(mode))
		return classificationWiring{lookup: productclassificationcopy.NewPermissiveLookup()}, nil
	}
	if strings.TrimSpace(cfg.groupID) == "" {
		return classificationWiring{}, errors.New("PRODUCT_CLASSIFICATION_MODE=kafka requires PRODUCT_CLASSIFICATION_CONSUMER_GROUP (a stable consumer group shared by every replica)")
	}
	if strings.TrimSpace(cfg.kafkaBrokers) == "" {
		return classificationWiring{}, errors.New("PRODUCT_CLASSIFICATION_MODE=kafka requires KAFKA_BROKERS to be set")
	}
	var store copyStore
	if pool != nil {
		store = productclassificationcopy.NewStore(pool, logger)
	} else {
		logger.Warn("PRODUCT_CLASSIFICATION_MODE=kafka without DATABASE_URL: the classification copy is in memory and lost on restart; use a fresh PRODUCT_CLASSIFICATION_CONSUMER_GROUP per run to replay it")
		store = productclassificationcopy.NewMemoryStore()
	}
	logger.Info("product classification lookup configured", "mode", string(mode), "topic", cloudevents.TopicProductMasterEvents, "group_id", cfg.groupID, "postgres", pool != nil)
	return classificationWiring{lookup: store, copies: store, groupID: cfg.groupID}, nil
}

// startClassificationConsumer starts the ProductClassified consumer when the
// kafka mode wired a copy, and records how to stop it. A no-op otherwise.
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
		if err := consumer.Run(ctx); err != nil {
			s.logger.Error("product classification consumer stopped", "error", err)
		}
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
