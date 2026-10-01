// Command wes is the composition root: it wires config from env into
// adapters, use cases, and the HTTP router, then serves.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/filecatalog"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafkacatalog"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassification"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/telemetry"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/traveldistance"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/bootretry"
	"github.com/claudioed/wes-work-planning/internal/resilience"
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

	httpAddr := getenv("HTTP_ADDR", ":8080")
	databaseURL := os.Getenv("DATABASE_URL")
	// MIGRATIONS_DATABASE_URL, when set, is a DIRECT (non-pooled,
	// session-mode) Postgres connection string used ONLY for the
	// golang-migrate startup step in wireRepositories below -- the
	// pgxpool opened right after migrations complete still uses
	// databaseURL unchanged, so every request this service serves keeps
	// going through PgBouncer exactly as before. See wireRepositories'
	// doc comment for the full "why": golang-migrate's postgres driver
	// takes a session-scoped `SELECT pg_advisory_lock($1)` to serialize
	// concurrent migration runs, which PgBouncer's transaction-pooling
	// mode does not support (warehouse-infra's PgBouncer rollout, PR
	// #43; this fallback closes the fleet-wide bug that rollout
	// introduced -- see ADR
	// 0026-migrations-direct-postgres-connection.md, mirroring
	// order-management ADR-0029). Falls back to databaseURL when unset,
	// which is every environment that doesn't provision the split
	// (local dev, CI integration tests, and any cluster whose Terraform
	// predates this fix) -- byte-identical to this service's behavior
	// before this change in that case.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	eventPublisherKind := getenv("EVENT_PUBLISHER", "log")
	otelServiceName := getenv("OTEL_SERVICE_NAME", serviceName)

	// catalogueConsumerCtx/cancelCatalogueConsumer are declared here
	// because the Kafka catalogue source needs its own Run goroutine
	// started BEFORE WaitReady is called inside wireCatalogue --
	// otherwise nothing would ever be consuming messages while this
	// process waits, guaranteeing a deadlock until WaitReadyTimeout.
	catalogueConsumerCtx, cancelCatalogueConsumer := context.WithCancel(context.Background())
	defer cancelCatalogueConsumer()

	catalogue, kafkaCatalogue, err := wireCatalogue(kafkaBrokers, catalogueConsumerCtx, logger)
	if err != nil {
		return err
	}

	shutdownTelemetry, err := telemetry.Setup(
		context.Background(),
		otelServiceName,
		getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion),
		getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint),
	)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// A failed final flush means the Collector was unreachable, not that
		// the service failed — log it and let the process exit cleanly.
		if err := shutdownTelemetry(ctx); err != nil {
			logger.Warn("telemetry shutdown did not flush cleanly", "error", err)
		}
	}()

	clock := memory.SystemClock{}

	repos, err := wireRepositories(databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	if repos.pgPool != nil {
		defer repos.pgPool.Close()
	}

	publisher, relay, stopPublisher, err := wireEventPublisher(logger, eventPublisherKind, kafkaBrokers, repos.workUnits, repos.classificationLookup(logger), repos.pgPool)
	if err != nil {
		return err
	}
	defer stopPublisher()

	handlers := newHandlers(repos, publisher, clock, catalogue, repos.travelDistanceLookup(logger))

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(handlers, otelServiceName, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	s := &serving{
		logger:                  logger,
		httpAddr:                httpAddr,
		server:                  server,
		handlers:                handlers,
		relay:                   relay,
		repos:                   repos,
		catalogue:               catalogue,
		kafkaBrokers:            kafkaBrokers,
		recordCompletion:        usecases.NewRecordCompletion(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(repos.uow),
		enqueueWorkUnit:         usecases.NewEnqueueWorkUnit(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(repos.uow),
		cancelCatalogueConsumer: cancelCatalogueConsumer,
		kafkaCatalogue:          kafkaCatalogue,
	}
	return s.run()
}

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
		if kafkaBrokers == "" {
			return nil, nil, fmt.Errorf("PATH_CATALOGUE_SOURCE=kafka requires KAFKA_BROKERS to be set")
		}
		// Retried: this fleet's Istio native sidecars reset EVERY
		// injected pod's first outbound TCP dial ~10s after the app
		// starts, and NewConsumer's newTargetOffsets dials the broker
		// directly before anything else runs. A single attempt turns
		// that known, transient reset into CrashLoopBackOff exactly
		// like the equivalent, unretried Postgres dial below did (see
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
	default:
		// The process-path catalogue is loaded and validated once at
		// boot, before anything else stands up — a missing or
		// malformed catalogue file must stop this service from
		// starting at all, never fall back to a partial/empty
		// catalogue (mirrors fulfillment-execution's identical
		// boot-time contract; see ADR-0017 there and this service's
		// own ADR-0012).
		fileCatalogue, err := filecatalog.Load(getenv("PATH_CATALOGUE_FILE", "/etc/wes-work-planning/process-paths.yaml"))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load the process-path catalogue: %w", err)
		}
		logger.Info("process-path catalogue loaded", "paths", fileCatalogue.Ids())
		return fileCatalogue, nil, nil
	}
}

// repositories groups the environment-selected persistence adapters.
// pgPool and uow are nil in the in-memory configuration. A nil
// UnitOfWork makes every use case run Save + Publish back to back
// (ADR-0014); with Postgres they run in one transaction.
type repositories struct {
	charges        ports.ChargeRepo
	plans          ports.PlanRepo
	pools          ports.WorkPoolRepo
	workUnits      ports.WorkUnitRepo
	laborPlanViews ports.LaborPlanViewRepo
	inventoryViews ports.InventoryViewRepo
	processedEvts  ports.ProcessedEventRepo
	pgPool         *pgxpool.Pool
	uow            ports.UnitOfWork
}

// classificationLookup wires the product-classification ACL behind the
// shared circuit-breaker gauge (ADR-0023).
func (r repositories) classificationLookup(logger *slog.Logger) ports.ProductClassificationLookup {
	breakerMetrics, err := telemetry.NewCircuitBreakerMetrics()
	if err != nil {
		logger.Warn("circuit breaker metrics not registered; continuing without them", "error", err)
	}
	return buildClassificationLookup(getenv("PRODUCT_CLASSIFICATION_MODE", "permissive"), os.Getenv("INVENTORY_STORAGE_BASE_URL"), breakerMetrics, logger)
}

// travelDistanceLookup wires the facility-layout travel-distance ACL
// behind the shared circuit-breaker gauge (ADR-0023).
func (r repositories) travelDistanceLookup(logger *slog.Logger) ports.TravelDistanceLookup {
	breakerMetrics, err := telemetry.NewCircuitBreakerMetrics()
	if err != nil {
		logger.Warn("circuit breaker metrics not registered; continuing without them", "error", err)
	}
	return buildTravelDistanceLookup(getenv("TRAVEL_DISTANCE_MODE", "permissive"), os.Getenv("FACILITY_LAYOUT_BASE_URL"), breakerMetrics, logger)
}

// wireRepositories selects the in-memory or Postgres outbound side.
//
// The OLTP schema is a precondition this process enforces itself, rather
// than assuming an out-of-band golang-migrate CLI step ran. That
// assumption silently did not hold: the service deployed cleanly against
// an empty database and every Postgres-backed endpoint failed at request
// time with `relation "charge_forecasts" does not exist`. Migrating
// before the pool is opened matches what fulfillment-execution's and
// workforce-management's OLTP binaries already do. Retried, because in
// this fleet EVERY injected pod's first outbound TCP dial is reset ~10s
// after the app starts (Istio native sidecars; see internal/bootretry's
// package doc comment). A single attempt turns that known, transient
// condition into CrashLoopBackOff before this service ever gets far
// enough to serve its own health probe.
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below —
// the pgxpool opened just after it (and used for every subsequent
// request) always uses databaseURL. They are deliberately different
// connection strings in a PgBouncer-fronted environment: golang-migrate's
// postgres driver takes a session-scoped `SELECT pg_advisory_lock($1)` to
// serialize concurrent migration runs across replicas starting at the
// same time, and PgBouncer's transaction-pooling mode (this fleet's
// pool_mode for every OLTP DATABASE_URL, warehouse-infra PR #43) does not
// support session-scoped state — each statement in one logical client
// session can land on a different physical backend connection, so the
// advisory lock never behaves as a real mutex. Losing replicas crash-loop
// with `pq: unnamed prepared statement does not exist` / `pq: canceling
// statement due to statement timeout` until one wins the race. See ADR
// 0026-migrations-direct-postgres-connection.md (mirroring order-management
// ADR-0029) for the full incident and fix. Callers pass
// MIGRATIONS_DATABASE_URL when set (warehouse-infra now provisions it as a
// direct, non-pooled DSN alongside DATABASE_URL) or fall back to
// databaseURL itself for any environment that doesn't provision the split
// (local dev, CI integration tests) — byte-identical to this function's
// behavior before this parameter existed in that case.
func wireRepositories(databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (repositories, error) {
	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return repositories{
			charges:        memory.NewChargeRepo(),
			plans:          memory.NewPlanRepo(),
			pools:          memory.NewWorkPoolRepo(),
			workUnits:      memory.NewWorkUnitRepo(),
			laborPlanViews: memory.NewLaborPlanViewRepo(),
			inventoryViews: memory.NewInventoryViewRepo(),
			processedEvts:  memory.NewProcessedEventRepo(),
		}, nil
	}

	bootCtx, bootCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer bootCancel()
	if err := bootretry.Retry(bootCtx, logger, "run migrations", func() error {
		return postgres.Migrate(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return repositories{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.Connect(ctx, databaseURL)
	if err != nil {
		return repositories{}, err
	}
	// pgxpool.NewWithConfig does not itself dial or establish a
	// connection, so without this retried Ping the first-dial reset
	// would surface inside the first real request rather than at boot —
	// turning a transient sidecar warm-up into an intermittent 500
	// instead of a bounded startup retry.
	if err := bootretry.Retry(bootCtx, logger, "ping postgres", func() error {
		return pool.Ping(bootCtx)
	}); err != nil {
		pool.Close()
		return repositories{}, err
	}
	return repositories{
		charges:        postgres.NewChargeRepo(pool),
		plans:          postgres.NewPlanRepo(pool),
		pools:          postgres.NewWorkPoolRepo(pool),
		workUnits:      postgres.NewWorkUnitRepo(pool),
		laborPlanViews: postgres.NewLaborPlanViewRepo(pool),
		inventoryViews: postgres.NewInventoryViewRepo(pool),
		processedEvts:  postgres.NewProcessedEventRepo(pool),
		pgPool:         pool,
		uow:            postgres.NewUnitOfWork(pool),
	}, nil
}

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
		sink := outboundkafka.NewRelaySink(brokers)
		closers = append(closers, func() { _ = sink.Close() })
		relay = postgres.NewOutboxRelay(pgPool, sink, logger,
			postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)))
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

// newHandlers wires every use case into the inbound HTTP handlers,
// including the idempotency pool and readiness gate of their ADRs.
func newHandlers(repos repositories, publisher ports.EventPublisher, clock memory.SystemClock, catalogue ports.PathCatalogue, travelDistances ports.TravelDistanceLookup) *inboundhttp.Handlers {
	uow := repos.uow
	return &inboundhttp.Handlers{
		ReceiveChargeForecast:   usecases.NewReceiveChargeForecast(repos.charges, publisher, clock).WithUnitOfWork(uow),
		CommitShiftPlan:         usecases.NewCommitShiftPlan(repos.plans, publisher, clock).WithUnitOfWork(uow).WithTravelDistanceLookup(travelDistances),
		EnqueueWorkUnit:         usecases.NewEnqueueWorkUnit(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(uow),
		ReleaseNextWork:         usecases.NewReleaseNextWork(repos.pools, repos.workUnits, publisher, clock).WithUnitOfWork(uow),
		RecordCompletion:        usecases.NewRecordCompletion(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(uow),
		SampleBacklog:           usecases.NewSampleBacklog(repos.pools, publisher, clock).WithUnitOfWork(uow),
		RebalanceDecision:       usecases.NewRebalanceDecision(repos.pools, publisher, clock).WithUnitOfWork(uow),
		Catalogue:               catalogue,
		LaborPlanView:           usecases.NewLaborPlanView(repos.laborPlanViews),
		InventoryView:           usecases.NewInventoryView(repos.inventoryViews),
		GetWorkUnitsByReference: usecases.NewGetWorkUnitsByReference(repos.workUnits),
		GetWorkUnit:             usecases.NewGetWorkUnit(repos.workUnits),
		// IdempotencyPool wires RequireIdempotencyKey onto POST
		// /paths/{pathId}/work-units (see idempotency.go's ADR). nil in
		// the in-memory configuration (pgPool nil), matching every other
		// optional Postgres-backed capability's convention here.
		IdempotencyPool: repos.pgPool,
		// Readiness backs GET /readyz (ADR-0023 §graceful shutdown) --
		// flipped to not-ready as the FIRST step of the shutdown
		// sequence below, before anything else stops.
		Readiness: &inboundhttp.Readiness{},
	}
}

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
}

// run serves until SIGINT/SIGTERM (or a component failure), then drains.
func (s *serving) run() error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("http server listening", "addr", s.httpAddr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// The outbox relay (ADR-0014) runs alongside the HTTP server in the
	// same process, draining outbox_events onto both Kafka topics. It is
	// only wired when Postgres AND the kafka publisher are configured.
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(context.Background())
	defer stopRelay()
	if s.relay != nil {
		go func() {
			defer close(relayDone)
			s.logger.Info("outbox relay running", "topics", []string{cloudevents.TopicWorkPlanningEvents, outboundkafka.AnalyticsTopic})
			if err := s.relay.Run(relayCtx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	} else {
		close(relayDone)
	}

	var consumer *inboundkafka.Consumer
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	if s.kafkaBrokers != "" {
		s.logger.Info("consuming integration events", "brokers", s.kafkaBrokers)
		// Every inbound-event use case records the CloudEvents id as
		// processed in the SAME UnitOfWork as its effect, so a failed
		// attempt leaves no mark and is retried, never swallowed (ADR-0028).
		observeLabor := usecases.NewObserveLaborPlan(s.repos.laborPlanViews, s.repos.processedEvts).WithUnitOfWork(s.repos.uow)
		observeInventory := usecases.NewObserveInventoryChange(s.repos.inventoryViews, s.repos.processedEvts).WithUnitOfWork(s.repos.uow)
		applyTaskCompleted := usecases.NewApplyTaskCompleted(s.recordCompletion, s.repos.processedEvts).WithUnitOfWork(s.repos.uow)
		applyOrderAllocated := usecases.NewApplyOrderAllocated(s.enqueueWorkUnit, s.repos.processedEvts, s.catalogue).WithUnitOfWork(s.repos.uow)
		groupID := consumerGroupID(os.Getenv("KAFKA_CONSUMER_GROUP"))
		s.logger.Info("kafka consumer group", "group_id", groupID)
		consumer = inboundkafka.NewConsumer(brokerList(s.kafkaBrokers), groupID, observeLabor, observeInventory, applyTaskCompleted, applyOrderAllocated, s.catalogue, s.logger)
		go func() {
			if err := consumer.Run(consumerCtx); err != nil {
				s.logger.Error("kafka consumer stopped", "error", err)
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
		s.logger.Info("shutting down")
		return s.gracefulShutdown(cancelConsumer, consumer, stopRelay, relayDone)
	}
}

// gracefulShutdown drains the process (ADR-0023 §graceful shutdown):
// readiness flips first, consumers and the catalogue consumer stop and
// close, the HTTP server drains within 5s, and the outbox relay is
// allowed to finish its in-flight pass so an event committed by a
// request that completed just before shutdown is not stranded until the
// next pod boots.
func (s *serving) gracefulShutdown(cancelConsumer context.CancelFunc, consumer *inboundkafka.Consumer, stopRelay context.CancelFunc, relayDone <-chan struct{}) error {
	// Flip readiness to not-ready FIRST, before anything else stops, so
	// a Kubernetes readinessProbe polling /readyz has a window to
	// observe the flip and stop routing NEW traffic to this pod before
	// the listener is closed below.
	s.handlers.Readiness.SetNotReady()
	cancelConsumer()
	if consumer != nil {
		_ = consumer.Close()
	}
	s.cancelCatalogueConsumer()
	if s.kafkaCatalogue != nil {
		_ = s.kafkaCatalogue.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.server.Shutdown(ctx)
	stopRelay()
	select {
	case <-relayDone:
	case <-ctx.Done():
		s.logger.Warn("outbox relay did not stop before the shutdown deadline")
	}
	return err
}

// newLogger builds the process-wide structured logger: JSON to stdout, at
// the level LOG_LEVEL names (debug|info|warn|error, case-insensitive,
// default info). Records are routed through telemetry.TraceHandler so any
// log emitted inside a span carries that span's trace_id and span_id.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(telemetry.NewTraceHandler(handler))
}

func brokerList(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// newEventID generates a UUID v4 for outbound integration event envelopes.
func newEventID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// defaultConsumerGroup is the group id every deployed instance of this
// service shares, so they cooperatively split the partitions of the topics
// below -- the normal, intended behaviour for a horizontally scaled service.
const defaultConsumerGroup = "wes-work-planning"

// consumerGroupID resolves the Kafka consumer group id, allowing
// KAFKA_CONSUMER_GROUP to override the default.
//
// This override exists for a specific, real failure: consumer-group offsets
// are shared infrastructure state, not per-process state. This fleet runs ONE
// Kafka broker platform-wide, so a second process started against it -- the
// e2e-tests harness's local binary, or a developer's `go run` -- joins the
// SAME group as the deployed Deployment when the id is fixed. With one
// partition per topic, Kafka's rebalance protocol awards that partition to
// exactly one member and the other silently consumes nothing, having been
// told it is healthy.
//
// Setting a unique id (e.g. wes-work-planning-e2e-$$) isolates such a process
// so it replays the topics itself instead of competing for them. Leaving it
// unset preserves the shared-group behaviour deployments rely on.
func consumerGroupID(override string) string {
	if strings.TrimSpace(override) != "" {
		return override
	}
	return defaultConsumerGroup
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// durationEnv parses key as a time.Duration, falling back on absence or a
// malformed/non-positive value: the relay interval is a tuning knob, not
// a contract, so it must never fail the boot.
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// buildClassificationLookup selects the outbound
// ports.ProductClassificationLookup adapter via PRODUCT_CLASSIFICATION_MODE
// (http|permissive), defaulting to "permissive" so existing tests, CI and
// deployments that do not set the env var are unaffected — mirroring
// inventory-storage's own LOCATION_LOOKUP_MODE=http|permissive pattern (see
// ADR-0009). "http" requires INVENTORY_STORAGE_BASE_URL. In "http" mode the
// client is wrapped with a per-dependency circuit breaker + jittered retry
// (ADR-0023); recorder may be nil (breaker metrics unavailable), a
// documented no-op.
func buildClassificationLookup(mode, inventoryStorageBaseURL string, recorder resilience.StateRecorder, logger *slog.Logger) ports.ProductClassificationLookup {
	if !strings.EqualFold(mode, "http") {
		return productclassification.NewPermissiveLookup()
	}
	logger.Info("product classification lookup configured", "mode", "http", "inventory_storage_base_url", inventoryStorageBaseURL)
	return productclassification.NewBreakerClient(productclassification.NewClient(inventoryStorageBaseURL, nil), recorder)
}

// buildTravelDistanceLookup selects the outbound ports.TravelDistanceLookup
// adapter via TRAVEL_DISTANCE_MODE (http|permissive), defaulting to
// "permissive" so existing tests, CI and deployments that do not set the
// env var are unaffected — mirroring buildClassificationLookup's own
// PRODUCT_CLASSIFICATION_MODE pattern exactly (see ADR-0017, Phase B3).
// "http" requires FACILITY_LAYOUT_BASE_URL. In "http" mode the client is
// wrapped with a per-dependency circuit breaker + jittered retry
// (ADR-0023); recorder may be nil (breaker metrics unavailable), a
// documented no-op.
func buildTravelDistanceLookup(mode, facilityLayoutBaseURL string, recorder resilience.StateRecorder, logger *slog.Logger) ports.TravelDistanceLookup {
	if !strings.EqualFold(mode, "http") {
		return traveldistance.NewPermissiveLookup()
	}
	logger.Info("travel distance lookup configured", "mode", "http", "facility_layout_base_url", facilityLayoutBaseURL)
	return traveldistance.NewBreakerClient(traveldistance.NewClient(facilityLayoutBaseURL, nil), recorder)
}
