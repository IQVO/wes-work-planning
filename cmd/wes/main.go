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
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/envelope"
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
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	eventPublisherKind := getenv("EVENT_PUBLISHER", "log")
	otelServiceName := getenv("OTEL_SERVICE_NAME", serviceName)

	// The process-path catalogue's SOURCE is selectable, defaulting to
	// the existing boot-time file read ("file") -- zero behavior change
	// for any existing deployment unless PATH_CATALOGUE_SOURCE=kafka is
	// explicitly set, matching this fleet's EVENT_PUBLISHER convention.
	// See internal/adapters/outbound/kafkacatalog's package doc comment
	// for the full rationale and the readiness-gate design, mirrored
	// byte-for-byte from fulfillment-execution's identical wiring.
	catalogueSource := getenv("PATH_CATALOGUE_SOURCE", "file")

	var catalogue ports.PathCatalogue
	var kafkaCatalogue *kafkacatalog.Consumer
	// catalogueConsumerCtx/cancelCatalogueConsumer are declared here
	// (rather than at cancelConsumer's original location further down)
	// because the Kafka catalogue source needs its own Run goroutine
	// started BEFORE WaitReady is called below -- otherwise nothing
	// would ever be consuming messages while this process waits,
	// guaranteeing a deadlock until WaitReadyTimeout.
	catalogueConsumerCtx, cancelCatalogueConsumer := context.WithCancel(context.Background())
	defer cancelCatalogueConsumer()

	switch catalogueSource {
	case "kafka":
		if kafkaBrokers == "" {
			return fmt.Errorf("PATH_CATALOGUE_SOURCE=kafka requires KAFKA_BROKERS to be set")
		}
		// Retried: this fleet's Istio native sidecars reset EVERY
		// injected pod's first outbound TCP dial ~10s after the app
		// starts, and NewConsumer's newTargetOffsets dials the broker
		// directly before anything else runs. A single attempt turns
		// that known, transient reset into CrashLoopBackOff exactly
		// like the equivalent, unretried Postgres dial below did (see
		// internal/bootretry's package doc comment).
		var err error
		if err = bootretry.Retry(context.Background(), logger, "connect to the process-path catalogue topic", func() error {
			var dialErr error
			kafkaCatalogue, dialErr = kafkacatalog.NewConsumer(context.Background(), brokerList(kafkaBrokers), logger)
			return dialErr
		}); err != nil {
			return fmt.Errorf("failed to start the Kafka-sourced process-path catalogue: %w", err)
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
		err = kafkaCatalogue.WaitReady(waitCtx)
		waitCancel()
		if err != nil {
			return fmt.Errorf("process-path catalogue did not become ready within %s: %w", kafkacatalog.WaitReadyTimeout, err)
		}
		logger.Info("process-path catalogue is ready", "paths", kafkaCatalogue.Ids())
		catalogue = kafkaCatalogue
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
			return fmt.Errorf("failed to load the process-path catalogue: %w", err)
		}
		logger.Info("process-path catalogue loaded", "paths", fileCatalogue.Ids())
		catalogue = fileCatalogue
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

	var (
		charges   ports.ChargeRepo
		plans     ports.PlanRepo
		pools     ports.WorkPoolRepo
		workUnits ports.WorkUnitRepo

		laborPlanViews ports.LaborPlanViewRepo
		inventoryViews ports.InventoryViewRepo
		processedEvts  ports.ProcessedEventRepo

		// pgPool and uow are nil in the in-memory configuration. A nil
		// UnitOfWork makes every use case run Save + Publish back to back
		// (ADR-0014); with Postgres they run in one transaction.
		pgPool *pgxpool.Pool
		uow    ports.UnitOfWork
	)

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		charges = memory.NewChargeRepo()
		plans = memory.NewPlanRepo()
		pools = memory.NewWorkPoolRepo()
		workUnits = memory.NewWorkUnitRepo()
		laborPlanViews = memory.NewLaborPlanViewRepo()
		inventoryViews = memory.NewInventoryViewRepo()
		processedEvts = memory.NewProcessedEventRepo()
	} else {
		// The OLTP schema is a precondition this process enforces itself,
		// rather than assuming an out-of-band golang-migrate CLI step ran.
		// That assumption silently did not hold: the service deployed
		// cleanly against an empty database and every Postgres-backed
		// endpoint failed at request time with
		// `relation "charge_forecasts" does not exist`. Migrating before
		// the pool is opened matches what fulfillment-execution's and
		// workforce-management's OLTP binaries already do.
		// Retried, because in this fleet EVERY injected pod's first
		// outbound TCP dial is reset ~10s after the app starts (Istio
		// native sidecars; see internal/bootretry's package doc
		// comment). A single attempt turns that known, transient
		// condition into CrashLoopBackOff before this service ever
		// gets far enough to serve its own health probe.
		bootCtx, bootCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer bootCancel()
		if err := bootretry.Retry(bootCtx, logger, "run migrations", func() error {
			return postgres.Migrate(databaseURL, migrationsPath)
		}); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		pool, err := postgres.Connect(ctx, databaseURL)
		if err != nil {
			return err
		}
		// pgxpool.NewWithConfig does not itself dial or establish a
		// connection, so without this retried Ping the first-dial
		// reset would surface inside the first real request rather
		// than at boot — turning a transient sidecar warm-up into an
		// intermittent 500 instead of a bounded startup retry.
		if err := bootretry.Retry(bootCtx, logger, "ping postgres", func() error {
			return pool.Ping(bootCtx)
		}); err != nil {
			pool.Close()
			return err
		}
		defer pool.Close()
		pgPool = pool
		uow = postgres.NewUnitOfWork(pool)

		charges = postgres.NewChargeRepo(pool)
		plans = postgres.NewPlanRepo(pool)
		pools = postgres.NewWorkPoolRepo(pool)
		workUnits = postgres.NewWorkUnitRepo(pool)
		laborPlanViews = postgres.NewLaborPlanViewRepo(pool)
		inventoryViews = postgres.NewInventoryViewRepo(pool)
		processedEvts = postgres.NewProcessedEventRepo(pool)
	}

	var publisher ports.EventPublisher
	var relay *postgres.OutboxRelay
	classifications := buildClassificationLookup(getenv("PRODUCT_CLASSIFICATION_MODE", "permissive"), os.Getenv("INVENTORY_STORAGE_BASE_URL"), logger)
	travelDistances := buildTravelDistanceLookup(getenv("TRAVEL_DISTANCE_MODE", "permissive"), os.Getenv("FACILITY_LAYOUT_BASE_URL"), logger)
	switch eventPublisherKind {
	case "kafka":
		if kafkaBrokers == "" {
			return fmt.Errorf("EVENT_PUBLISHER=kafka requires KAFKA_BROKERS to be set")
		}
		brokers := brokerList(kafkaBrokers)
		// The integration publisher (warehouse.work-planning.events) is
		// untouched. Alongside it, a SEPARATE analytics publisher fans every
		// domain event onto the dedicated analytics topic
		// (warehouse.wes.analytics) that feeds the "Release Throughput &
		// Backlog Health" data product (ADR-0011).
		integrationPublisher := outboundkafka.NewPublisher(brokers, workUnits, classifications, newEventID)
		defer func() { _ = integrationPublisher.Close() }()
		analyticsPublisher := outboundkafka.NewAnalyticsPublisher(brokers, newEventID)
		defer func() { _ = analyticsPublisher.Close() }()

		if pgPool != nil {
			// Transactional outbox (ADR-0014): both publishers act only as
			// ENCODERS inside the use case's transaction — one outbox row
			// per event per topic — and the relay below drains those rows
			// onto Kafka through a single topic-less writer. The store and
			// the topics can no longer diverge.
			sink := outboundkafka.NewRelaySink(brokers)
			defer func() { _ = sink.Close() }()
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
	default:
		publisher = events.NewLogPublisher(logger)
		logger.Info("event publisher configured", "publisher", "log")
	}

	recordCompletion := usecases.NewRecordCompletion(workUnits, pools, publisher, clock).WithUnitOfWork(uow)
	enqueueWorkUnit := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock).WithUnitOfWork(uow)

	handlers := &inboundhttp.Handlers{
		ReceiveChargeForecast:   usecases.NewReceiveChargeForecast(charges, publisher, clock).WithUnitOfWork(uow),
		CommitShiftPlan:         usecases.NewCommitShiftPlan(plans, publisher, clock).WithUnitOfWork(uow).WithTravelDistanceLookup(travelDistances),
		EnqueueWorkUnit:         enqueueWorkUnit,
		ReleaseNextWork:         usecases.NewReleaseNextWork(pools, workUnits, publisher, clock).WithUnitOfWork(uow),
		RecordCompletion:        recordCompletion,
		SampleBacklog:           usecases.NewSampleBacklog(pools, publisher, clock).WithUnitOfWork(uow),
		RebalanceDecision:       usecases.NewRebalanceDecision(pools, publisher, clock).WithUnitOfWork(uow),
		Catalogue:               catalogue,
		LaborPlanView:           usecases.NewLaborPlanView(laborPlanViews),
		InventoryView:           usecases.NewInventoryView(inventoryViews),
		GetWorkUnitsByReference: usecases.NewGetWorkUnitsByReference(workUnits),
	}

	router := inboundhttp.NewRouter(handlers, otelServiceName, logger)

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// The outbox relay (ADR-0014) runs alongside the HTTP server in the
	// same process, draining outbox_events onto both Kafka topics. It is
	// only wired when Postgres AND the kafka publisher are configured.
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(context.Background())
	defer stopRelay()
	if relay != nil {
		go func() {
			defer close(relayDone)
			logger.Info("outbox relay running", "topics", []string{envelope.TopicWorkPlanningEvents, outboundkafka.AnalyticsTopic})
			if err := relay.Run(relayCtx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	} else {
		close(relayDone)
	}

	var consumer *inboundkafka.Consumer
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	if kafkaBrokers != "" {
		logger.Info("consuming integration events", "brokers", kafkaBrokers)
		observeLabor := usecases.NewObserveLaborPlan(laborPlanViews, processedEvts)
		observeInventory := usecases.NewObserveInventoryChange(inventoryViews, processedEvts)
		groupID := consumerGroupID(os.Getenv("KAFKA_CONSUMER_GROUP"))
		logger.Info("kafka consumer group", "group_id", groupID)
		consumer = inboundkafka.NewConsumer(brokerList(kafkaBrokers), groupID, observeLabor, observeInventory, recordCompletion, enqueueWorkUnit, processedEvts, catalogue, logger)
		go func() {
			if err := consumer.Run(consumerCtx); err != nil {
				logger.Error("kafka consumer stopped", "error", err)
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
		logger.Info("shutting down")
		cancelConsumer()
		if consumer != nil {
			_ = consumer.Close()
		}
		cancelCatalogueConsumer()
		if kafkaCatalogue != nil {
			_ = kafkaCatalogue.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := server.Shutdown(ctx)
		// Let the relay finish its in-flight pass so an event committed by
		// a request that completed just before shutdown is not stranded
		// until the next pod boots.
		stopRelay()
		select {
		case <-relayDone:
		case <-ctx.Done():
			logger.Warn("outbox relay did not stop before the shutdown deadline")
		}
		return err
	}
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
// ADR-0009). "http" requires INVENTORY_STORAGE_BASE_URL.
func buildClassificationLookup(mode, inventoryStorageBaseURL string, logger *slog.Logger) ports.ProductClassificationLookup {
	if !strings.EqualFold(mode, "http") {
		return productclassification.NewPermissiveLookup()
	}
	logger.Info("product classification lookup configured", "mode", "http", "inventory_storage_base_url", inventoryStorageBaseURL)
	return productclassification.NewClient(inventoryStorageBaseURL, nil)
}

// buildTravelDistanceLookup selects the outbound ports.TravelDistanceLookup
// adapter via TRAVEL_DISTANCE_MODE (http|permissive), defaulting to
// "permissive" so existing tests, CI and deployments that do not set the
// env var are unaffected — mirroring buildClassificationLookup's own
// PRODUCT_CLASSIFICATION_MODE pattern exactly (see ADR-0017, Phase B3).
// "http" requires FACILITY_LAYOUT_BASE_URL.
func buildTravelDistanceLookup(mode, facilityLayoutBaseURL string, logger *slog.Logger) ports.TravelDistanceLookup {
	if !strings.EqualFold(mode, "http") {
		return traveldistance.NewPermissiveLookup()
	}
	logger.Info("travel distance lookup configured", "mode", "http", "facility_layout_base_url", facilityLayoutBaseURL)
	return traveldistance.NewClient(facilityLayoutBaseURL, nil)
}
