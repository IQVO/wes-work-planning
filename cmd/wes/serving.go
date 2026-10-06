package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafkacatalog"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

// serving groups everything the process needs from the moment the HTTP
// server starts listening until it has fully drained.
type serving struct {
	logger                  *slog.Logger
	httpAddr                string
	server                  *http.Server
	handlers                *inboundhttp.Handlers
	relay                   *postgres.OutboxRelay
	repos                   repositories
	catalogue               ports.PathCatalogue
	kafkaBrokers            string
	recordCompletion        *usecases.RecordCompletion
	enqueueWorkUnit         *usecases.EnqueueWorkUnit
	cancelCatalogueConsumer context.CancelFunc
	kafkaCatalogue          *kafkacatalog.Consumer
	housekeeper             *postgres.Housekeeper
	publisher               ports.EventPublisher
	clock                   ports.Clock
}

// servingDeps is everything run() has wired by the time it can build the
// HTTP handlers and the serving group.
type servingDeps struct {
	cfg                     appConfig
	logger                  *slog.Logger
	repos                   repositories
	publisher               ports.EventPublisher
	relay                   *postgres.OutboxRelay
	catalogue               ports.PathCatalogue
	kafkaCatalogue          *kafkacatalog.Consumer
	cancelCatalogueConsumer context.CancelFunc
}

// newServing builds the HTTP handlers, the server, and the serving group
// over already-wired adapters.
func newServing(d servingDeps) *serving {
	clock := memory.SystemClock{}
	handlers := newHandlers(d.repos, d.publisher, clock, d.catalogue, d.repos.travelDistanceLookup(d.logger))

	server := &http.Server{
		Addr:              d.cfg.httpAddr,
		Handler:           inboundhttp.NewRouter(handlers, d.cfg.otelServiceName, d.logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return &serving{
		logger:                  d.logger,
		httpAddr:                d.cfg.httpAddr,
		server:                  server,
		handlers:                handlers,
		relay:                   d.relay,
		housekeeper:             newHousekeeper(d.repos.pgPool, d.logger),
		publisher:               d.publisher,
		clock:                   clock,
		repos:                   d.repos,
		catalogue:               d.catalogue,
		kafkaBrokers:            d.cfg.kafkaBrokers,
		recordCompletion:        usecases.NewRecordCompletion(d.repos.workUnits, d.repos.pools, d.publisher, clock).WithUnitOfWork(d.repos.uow),
		enqueueWorkUnit:         usecases.NewEnqueueWorkUnit(d.repos.workUnits, d.repos.pools, d.publisher, clock).WithUnitOfWork(d.repos.uow),
		cancelCatalogueConsumer: d.cancelCatalogueConsumer,
		kafkaCatalogue:          d.kafkaCatalogue,
	}
}

// startHousekeeper runs the housekeeper (if wired) in the background and
// returns a func that cancels it and waits for it to return.
func (s *serving) startHousekeeper() (stopAndWait func()) {
	if s.housekeeper == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.housekeeper.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error("housekeeper stopped", "error", err)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// run serves until SIGINT/SIGTERM (or a component failure), then drains.
func (s *serving) run() error {
	errCh := make(chan error, 1)
	s.startHTTP(errCh)

	// The outbox relay (ADR-0014) runs alongside the HTTP server in the
	// same process, draining outbox_events onto both Kafka topics. It is
	// only wired when Postgres AND the kafka publisher are configured.
	relayCtx, stopRelay := context.WithCancel(context.Background())
	defer stopRelay()
	relayDone := s.startRelay(relayCtx, errCh)

	// The housekeeper (ADR-0032) bounds idempotency_keys / outbox_events. It
	// is stopped, and awaited, before run returns so the deferred pool close
	// in main never races an in-flight sweep.
	defer s.startHousekeeper()()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	consumer, consumerDone := s.startConsumer(consumerCtx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
		s.logger.Info("shutting down")
		return s.gracefulShutdown(cancelConsumer, consumer, consumerDone, stopRelay, relayDone)
	}
}

// startHTTP serves the HTTP listener in the background; a listener failure
// (other than a clean shutdown) is reported on errCh.
func (s *serving) startHTTP(errCh chan<- error) {
	go func() {
		s.logger.Info("http server listening", "addr", s.httpAddr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
}

// startRelay runs the outbox relay (if wired) and returns a channel that
// closes when it has returned (immediately when no relay is wired).
func (s *serving) startRelay(ctx context.Context, errCh chan<- error) <-chan struct{} {
	relayDone := make(chan struct{})
	if s.relay == nil {
		close(relayDone)
		return relayDone
	}
	go func() {
		defer close(relayDone)
		s.logger.Info("outbox relay running", "topics", []string{cloudevents.TopicWorkPlanningEvents, outboundkafka.AnalyticsTopic})
		if err := s.relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	return relayDone
}

// startConsumer starts the inbound Kafka consumer when brokers are
// configured. The returned channel closes when consumer.Run has returned
// (or immediately if no consumer is wired), so the shutdown can wait for
// in-flight handling to finish BEFORE closing the dependencies it uses
// (ADR-0023 §8.3).
func (s *serving) startConsumer(ctx context.Context) (*inboundkafka.Consumer, <-chan struct{}) {
	consumerDone := make(chan struct{})
	if s.kafkaBrokers == "" {
		close(consumerDone)
		return nil, consumerDone
	}
	s.logger.Info("consuming integration events", "brokers", s.kafkaBrokers)
	groupID := consumerGroupID(os.Getenv("KAFKA_CONSUMER_GROUP"))
	s.logger.Info("kafka consumer group", "group_id", groupID)
	consumer := s.newInboundConsumer(groupID)
	go func() {
		defer close(consumerDone)
		if err := consumer.Run(ctx); err != nil {
			s.logger.Error("kafka consumer stopped", "error", err)
		}
	}()
	return consumer, consumerDone
}

// newInboundConsumer wires the inbound-event use cases into the Kafka
// consumer. Every one of them records the CloudEvents id as processed in
// the SAME UnitOfWork as its effect, so a failed attempt leaves no mark
// and is retried, never swallowed (ADR-0028).
func (s *serving) newInboundConsumer(groupID string) *inboundkafka.Consumer {
	observeLabor := usecases.NewObserveLaborPlan(s.repos.laborPlanViews, s.repos.processedEvts).WithUnitOfWork(s.repos.uow).WithDriftReconciliation(s.repos.plans, s.publisher, s.clock)
	observeInventory := usecases.NewObserveInventoryChange(s.repos.inventoryViews, s.repos.processedEvts).WithUnitOfWork(s.repos.uow)
	applyTaskCompleted := usecases.NewApplyTaskCompleted(s.recordCompletion, s.repos.processedEvts).WithUnitOfWork(s.repos.uow)
	applyOrderAllocated := usecases.NewApplyOrderAllocated(s.enqueueWorkUnit, s.repos.processedEvts, s.catalogue).WithUnitOfWork(s.repos.uow)
	applyWorkDemandReleased := usecases.NewApplyWorkDemandReleased(s.enqueueWorkUnit, s.repos.processedEvts, s.catalogue).WithUnitOfWork(s.repos.uow)
	return inboundkafka.NewConsumer(brokerList(s.kafkaBrokers), groupID, observeLabor, observeInventory, applyTaskCompleted, applyOrderAllocated, applyWorkDemandReleased, s.catalogue, s.logger)
}

// gracefulShutdown drains the process (ADR-0023 §graceful shutdown):
// readiness flips first, consumers and the catalogue consumer stop and
// close, the HTTP server drains within 5s, and the outbox relay is
// allowed to finish its in-flight pass so an event committed by a
// request that completed just before shutdown is not stranded until the
// next pod boots.
//
// §8.3: the Kafka consumer's Run must RETURN (consumerDone) before the
// consumer is closed and the dependencies it uses are torn down, so a
// message mid-handling is finished (or fails and is redelivered) rather than
// racing a closed pool; the wait is bounded by the shutdown deadline.
func (s *serving) gracefulShutdown(cancelConsumer context.CancelFunc, consumer *inboundkafka.Consumer, consumerDone <-chan struct{}, stopRelay context.CancelFunc, relayDone <-chan struct{}) error {
	// Flip readiness to not-ready FIRST, before anything else stops, so
	// a Kubernetes readinessProbe polling /readyz has a window to
	// observe the flip and stop routing NEW traffic to this pod before
	// the listener is closed below.
	s.handlers.Readiness.SetNotReady()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cancelConsumer()
	waitDone(ctx, s.logger, "kafka consumer", consumerDone)
	if consumer != nil {
		_ = consumer.Close()
	}
	s.cancelCatalogueConsumer()
	if s.kafkaCatalogue != nil {
		_ = s.kafkaCatalogue.Close()
	}
	err := s.server.Shutdown(ctx)
	stopRelay()
	waitDone(ctx, s.logger, "outbox relay", relayDone)
	return err
}

// waitDone blocks until done closes or ctx expires, logging a warning in the
// latter case so a slow drain is visible rather than silent.
func waitDone(ctx context.Context, logger *slog.Logger, what string, done <-chan struct{}) {
	select {
	case <-done:
	case <-ctx.Done():
		logger.Warn(what + " did not stop before the shutdown deadline")
	}
}
