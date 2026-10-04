//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// TestOutboxRelay_PathCapacityChanged_RoundTripsThroughRealKafka proves
// PathCapacityChanged (ADR-0018) survives the full production path — a
// UnitOfWork-bracketed SampleBacklog call writes outbox rows against a real
// Postgres, the OutboxRelay drains them onto a REAL Kafka broker (via
// testcontainers, never a skip-gated external check per this repo's own
// integration-test convention), and a plain kafka-go consumer reads the
// message back off the wire and decodes it exactly as a real consumer
// would: CloudEvent -> type == com.warehouse.wes.work-planning.workpool.PathCapacityChanged
// -> data fields, and checks the CloudEvents id the relay published is the
// one minted at encode time and persisted with the outbox row.
func TestOutboxRelay_PathCapacityChanged_RoundTripsThroughRealKafka(t *testing.T) {
	ctx := context.Background()

	// Real Postgres for the outbox (mirrors outboxDB(t) in
	// outbox_integration_test.go, inlined here so this test can also spin
	// up Kafka independently without reordering that file's helpers).
	pool := outboxDB(t)

	// Real Kafka broker.
	kafkaContainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("wes-outbox-capacity-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, kafkaContainer)
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	if err := createCapacityTopic(ctx, brokers, cloudevents.TopicWorkPlanningEvents); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}
	if err := createCapacityTopic(ctx, brokers, outboundkafka.AnalyticsTopic); err != nil {
		t.Fatalf("create Kafka analytics topic: %v", err)
	}

	// Production wiring: UnitOfWork -> OutboxPublisher (encodes into the
	// outbox table) -> OutboxRelay -> RelaySink (a real, topic-less Kafka
	// writer) -> the broker.
	integration, analytics := encoders(pool)
	outboxPub := postgres.NewOutboxPublisher(pool, integration, analytics)
	uow := postgres.NewUnitOfWork(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	clock := memory.FixedClock{At: time.Date(2026, 9, 13, 8, 0, 0, 0, time.UTC)}
	pathId := mustPath(t, "pick-capacity-itest")

	// Seed a ReleaseFed pool with 2 of 5 slots occupied, so
	// RemainingCapacity reports a real, non-trivial figure (3 known).
	pool2 := release.NewWorkPool(pathId, release.ReleaseFed, 5, 100)
	cpt := shared.NewCPT(clock.Now().Add(time.Hour))
	for _, id := range []string{"wu-1", "wu-2"} {
		if err := pool2.Enqueue(id, cpt); err != nil {
			t.Fatalf("seed enqueue: %v", err)
		}
	}
	for _, id := range []string{"wu-1", "wu-2"} {
		if err := pool2.Release(id); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	}
	if err := pools.Save(ctx, pool2); err != nil {
		t.Fatalf("seed save: %v", err)
	}

	cutoff := time.Date(2026, 9, 13, 15, 0, 0, 0, time.UTC)
	sampleBacklog := usecases.NewSampleBacklog(pools, outboxPub, clock).WithUnitOfWork(uow)
	snapshot, err := sampleBacklog.Execute(ctx, usecases.SampleBacklogRequest{PathId: pathId, CutoffAt: cutoff})
	if err != nil {
		t.Fatalf("SampleBacklog: %v", err)
	}
	if !snapshot.RemainingCapacityKnown || snapshot.RemainingCapacityUnits != 3 {
		t.Fatalf("got known=%v remaining=%d, want known=true remaining=3", snapshot.RemainingCapacityKnown, snapshot.RemainingCapacityUnits)
	}
	if got := countOutbox(t, pool, "event_type = 'com.warehouse.wes.work-planning.workpool.PathCapacityChanged' AND published_at IS NULL"); got != 2 {
		t.Fatalf("expected 2 pending PathCapacityChanged outbox rows (integration + analytics topics), got %d", got)
	}

	// Drain the outbox onto the REAL broker.
	sink := outboundkafka.NewRelaySink(brokers)
	defer sink.Close()
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	if _, err := relay.RelayOnce(ctx); err != nil {
		t.Fatalf("RelayOnce: %v", err)
	}
	if got := countOutbox(t, pool, "event_type = 'com.warehouse.wes.work-planning.workpool.PathCapacityChanged' AND published_at IS NULL"); got != 0 {
		t.Fatalf("expected the PathCapacityChanged row to be marked published, %d still pending", got)
	}

	// The ids persisted with the outbox rows, read straight from the stored
	// CloudEvent bytes: the relay must republish exactly these.
	persistedIDs := map[string]bool{}
	rows, err := pool.Query(ctx, "SELECT value FROM outbox_events WHERE topic = $1", cloudevents.TopicWorkPlanningEvents)
	if err != nil {
		t.Fatalf("read outbox values: %v", err)
	}
	for rows.Next() {
		var v []byte
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan outbox value: %v", err)
		}
		stored, err := cloudevents.Decode(v)
		if err != nil {
			t.Fatalf("outbox row is not a CloudEvent: %v", err)
		}
		persistedIDs[stored.ID()] = true
	}
	rows.Close()

	// Consume it back off the REAL broker exactly like a real consumer
	// would: read raw bytes, decode the shared envelope, then the
	// event-type-specific data payload.
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   brokers,
		Topic:     cloudevents.TopicWorkPlanningEvents,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()
	if err := reader.SetOffset(0); err != nil {
		t.Fatalf("SetOffset: %v", err)
	}

	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var found bool
	for !found {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("ReadMessage (never saw PathCapacityChanged before timeout): %v", err)
		}
		env, err := cloudevents.Decode(msg.Value)
		if err != nil {
			t.Fatalf("decode CloudEvent: %v", err)
		}
		if env.Type() != "com.warehouse.wes.work-planning.workpool.PathCapacityChanged" {
			continue
		}
		found = true

		if env.Source() != "/warehouse/wes-work-planning" || env.Subject() != "pick-capacity-itest" ||
			env.DataSchema() != "urn:warehouse:wes-work-planning:events:PathCapacityChanged:v1" {
			t.Fatalf("unexpected CloudEvent attributes %s", env)
		}
		if !persistedIDs[env.ID()] {
			t.Fatalf("published id %q is not an id persisted in outbox_events %v", env.ID(), persistedIDs)
		}
		// ADR-0024: integration-topic events are keyed by the aggregate id
		// (here the path id), not the CloudEvents id, so one aggregate's
		// events stay on one partition.
		if string(msg.Key) != "pick-capacity-itest" {
			t.Fatalf("key = %q, want the aggregate id %q", msg.Key, "pick-capacity-itest")
		}
		if ct := capacityHeader(msg.Headers, "content-type"); ct != cloudevents.MediaType {
			t.Fatalf("content-type header = %q, want %q", ct, cloudevents.MediaType)
		}
		var data map[string]any
		if err := env.DataAs(&data); err != nil {
			t.Fatalf("unmarshal data: %v", err)
		}
		if data["path_id"] != "pick-capacity-itest" {
			t.Fatalf("got path_id %v, want pick-capacity-itest", data["path_id"])
		}
		if data["cutoff_at"] != "2026-09-13T15:00:00Z" {
			t.Fatalf("got cutoff_at %v, want 2026-09-13T15:00:00Z", data["cutoff_at"])
		}
		remaining, ok := data["remaining_units"].(float64)
		if !ok || remaining != 3 {
			t.Fatalf("got remaining_units %v, want 3", data["remaining_units"])
		}
		known, ok := data["known"].(bool)
		if !ok || !known {
			t.Fatalf("got known %v, want true", data["known"])
		}
	}
}

func createCapacityTopic(ctx context.Context, brokers []string, topic string) error {
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial Kafka: %w", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("Kafka controller: %w", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial Kafka controller: %w", err)
	}
	defer controllerConn.Close()
	if err := controllerConn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		return fmt.Errorf("create Kafka topic: %w", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("Kafka topic %q leader was not ready: %w", topic, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func capacityHeader(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
