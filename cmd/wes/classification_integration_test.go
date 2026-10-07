//go:build integration

package main

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ADR-0035 end to end, wired exactly like cmd/wes with
// PRODUCT_CLASSIFICATION_MODE=kafka: product-master's ProductClassified on a
// real Kafka broker -> the ProductClassified consumer -> the Postgres copy ->
// ReleaseNextWork, whose WorkReleased (encoded into the outbox inside the
// release transaction) carries the hazmat/fragile hints read through
// ports.ProductClassificationLookup. Both brokers are testcontainers.
func TestProductClassificationFromKafkaReachesWorkReleased(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dsn := startClassificationPostgres(t, ctx)
	migrations, err := filepath.Abs(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("migrations dir: %v", err)
	}
	repos, err := wireRepositories(dsn, dsn, migrations, logger)
	if err != nil {
		t.Fatalf("wireRepositories: %v", err)
	}
	t.Cleanup(repos.pgPool.Close)

	brokers := startClassificationKafka(t, ctx)
	brokerCSV := strings.Join(brokers, ",")

	suffix := time.Now().UnixNano()
	classification, err := buildClassification(classificationConfig{
		mode: "kafka", groupID: fmt.Sprintf("wes-pc-itest-%d", suffix), kafkaBrokers: brokerCSV,
	}, repos.pgPool, logger)
	if err != nil {
		t.Fatalf("buildClassification: %v", err)
	}
	s := &serving{logger: logger, repos: repos, kafkaBrokers: brokerCSV, classification: classification}
	s.startClassificationConsumer()
	t.Cleanup(s.stopClassificationConsumerBounded)

	hazmatSKU := fmt.Sprintf("SKU-HAZ-%d", suffix)
	newer, stale := fmt.Sprintf("evt-pc-v2-%d", suffix), fmt.Sprintf("evt-pc-v1-%d", suffix)
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicProductMasterEvents, Balancer: &kafkago.Hash{}}
	defer writer.Close()
	if err := writer.WriteMessages(ctx,
		productMasterMessage(t, newer, cloudevents.TypeProductClassified, hazmatSKU, map[string]any{
			"sku": hazmatSKU, "handling_tags": []string{"Hazmat", "Fragile"}, "dot_hazard_class": 3,
			"classification_source": "native", "version": 2,
		}),
		productMasterMessage(t, fmt.Sprintf("evt-pr-%d", suffix), "com.warehouse.wms.product-master.product.ProductRegistered", hazmatSKU, map[string]any{
			"sku": hazmatSKU, "description": "battery", "version": 1,
		}),
		// An older version arriving late must not overwrite the copy.
		productMasterMessage(t, stale, cloudevents.TypeProductClassified, hazmatSKU, map[string]any{
			"sku": hazmatSKU, "handling_tags": []string{"Oversized"}, "classification_source": "legacy-import", "version": 1,
		}),
	); err != nil {
		t.Fatalf("publish ProductClassified: %v", err)
	}

	// The stale event is the last one: once its id is claimed, every message
	// before it on the SKU's partition has been handled.
	eventuallyTrue(t, 90*time.Second, "stale ProductClassified consumed", func() bool { return processed(t, repos.pgPool, stale) })
	view, err := classification.lookup.GetClassification(ctx, hazmatSKU)
	if err != nil || !view.Known || !view.HasTag("Hazmat") || !view.HasTag("Fragile") || view.HasTag("Oversized") {
		t.Fatalf("lookup = %+v, %v; want v2 (Hazmat, Fragile), the stale v1 ignored", view, err)
	}

	// Release work through the real outbox publisher and use cases.
	publisher, _, stopPublisher, err := wireEventPublisher(logger, "kafka", brokerCSV, repos.workUnits, classification.lookup, repos.pgPool)
	if err != nil {
		t.Fatalf("wireEventPublisher: %v", err)
	}
	t.Cleanup(stopPublisher)
	clock := memory.SystemClock{}
	enqueue := usecases.NewEnqueueWorkUnit(repos.workUnits, repos.pools, publisher, clock).WithUnitOfWork(repos.uow)
	release := usecases.NewReleaseNextWork(repos.pools, repos.workUnits, publisher, clock).WithUnitOfWork(repos.uow)

	for _, unit := range []struct{ id, sku string }{
		{fmt.Sprintf("wu-haz-%d", suffix), hazmatSKU},
		{fmt.Sprintf("wu-plain-%d", suffix), fmt.Sprintf("SKU-UNKNOWN-%d", suffix)},
	} {
		pathID, err := shared.NewPathId(fmt.Sprintf("pick-pc-%s", unit.id))
		if err != nil {
			t.Fatalf("path id: %v", err)
		}
		if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
			WorkUnitId: unit.id, PathId: pathID, CPT: shared.NewCPT(time.Now().Add(time.Hour)), Reference: "order-pc", SKU: unit.sku,
		}); err != nil {
			t.Fatalf("enqueue %s: %v", unit.id, err)
		}
		if _, err := release.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathID}); err != nil {
			t.Fatalf("release %s: %v", unit.id, err)
		}
	}

	hazmat := workReleasedPayload(t, repos.pgPool, fmt.Sprintf("wu-haz-%d", suffix))
	if !strings.Contains(hazmat, `"required_capabilities":["hazmat"]`) || !strings.Contains(hazmat, `"fragile":true`) {
		t.Fatalf("WorkReleased for the classified SKU lacks the hints: %s", hazmat)
	}
	plain := workReleasedPayload(t, repos.pgPool, fmt.Sprintf("wu-plain-%d", suffix))
	if strings.Contains(plain, "required_capabilities") || strings.Contains(plain, `"fragile"`) {
		t.Fatalf("WorkReleased for an unknown SKU must carry no hints (fail-open): %s", plain)
	}
}

// testWriter routes slog output to t.Log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func startClassificationPostgres(t *testing.T, ctx context.Context) string {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wes_work_planning"),
		tcpostgres.WithUsername("wes"),
		tcpostgres.WithPassword("wes"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return dsn
}

// startClassificationKafka boots a broker, creates the product-master topic
// explicitly and waits for its partition leader and for the group
// coordinator, so the consumer's first join does not race a cold broker.
func startClassificationKafka(t *testing.T, ctx context.Context) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("wes-pc-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka: %v", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("Kafka controller: %v", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatalf("dial Kafka controller: %v", err)
	}
	defer controllerConn.Close()
	if err := controllerConn.CreateTopics(kafkago.TopicConfig{Topic: cloudevents.TopicProductMasterEvents, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	eventuallyTrue(t, 30*time.Second, "partition leader", func() bool {
		partitions, err := conn.ReadPartitions(cloudevents.TopicProductMasterEvents)
		return err == nil && len(partitions) > 0 && partitions[0].Leader.Host != ""
	})
	client := &kafkago.Client{Addr: kafkago.TCP(brokers...)}
	eventuallyTrue(t, 60*time.Second, "group coordinator", func() bool {
		resp, err := client.FindCoordinator(ctx, &kafkago.FindCoordinatorRequest{Key: "probe", KeyType: kafkago.CoordinatorKeyTypeConsumer})
		return err == nil && resp.Error == nil
	})
	return brokers
}

func productMasterMessage(t *testing.T, id, ceType, sku string, data map[string]any) kafkago.Message {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetType(ceType)
	e.SetSource("/warehouse/product-master")
	e.SetSubject(sku)
	e.SetTime(time.Now().UTC())
	e.SetDataSchema("urn:warehouse:product-master:events:ProductClassified:v1")
	if err := e.SetData(ce.ApplicationJSON, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	body, err := e.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafkago.Message{Key: []byte(sku), Value: body, Headers: []kafkago.Header{cloudevents.ContentTypeHeader()}}
}

func processed(t *testing.T, pool *pgxpool.Pool, eventID string) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM processed_events WHERE event_id = $1`, eventID).Scan(&n); err != nil {
		t.Fatalf("query processed_events: %v", err)
	}
	return n == 1
}

func workReleasedPayload(t *testing.T, pool *pgxpool.Pool, workUnitID string) string {
	t.Helper()
	var value string
	if err := pool.QueryRow(context.Background(),
		`SELECT convert_from(value, 'UTF8') FROM outbox_events
		 WHERE topic = $1 AND event_type = $2 AND convert_from(value, 'UTF8') LIKE '%' || $3 || '%'`,
		cloudevents.TopicWorkPlanningEvents, "com.warehouse.wes.work-planning.workunit.WorkReleased", `"work_unit_id":"`+workUnitID+`"`).Scan(&value); err != nil {
		t.Fatalf("read WorkReleased outbox row for %s: %v", workUnitID, err)
	}
	return value
}

func eventuallyTrue(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
