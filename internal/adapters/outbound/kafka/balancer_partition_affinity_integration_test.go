//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	adapter "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// TestAnalyticsPublisherKeysMessagesForSameAggregateOntoTheSamePartition is
// the real-Kafka-level proof behind switching every writer in this package
// from &kafkago.LeastBytes{} to &kafkago.Hash{}: on an 8-partition topic
// (mirroring warehouse-infra PR #42's fleet-wide 1->8 partition scaleup),
// every event this publisher emits for the SAME aggregate (WorkUnitId) must
// land on the SAME partition, while a different aggregate is free to land
// elsewhere.
//
// AnalyticsPublisher.marshalAnalyticsData has kept the message Key
// correctly set to the aggregate id since it was written (verified by
// reading the source) — but a key-blind LeastBytes balancer silently
// discards that key for partition routing. Only a real broker exposes this:
// a fake Writer's WriteMessages never computes a partition at all, so a
// fake-writer unit test can prove the Key bytes are right and still miss
// this class of bug entirely. This test fails (aggregate's messages
// scattered across partitions) if the Balancer field is ever reverted to
// LeastBytes, and passes only when it is a key-aware balancer such as Hash.
func TestAnalyticsPublisherKeysMessagesForSameAggregateOntoTheSamePartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("wes-work-planning-kafka-itest-balancer"),
	)
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}

	const numPartitions = 8
	topic := fmt.Sprintf("warehouse.wes.analytics.itest-part-%d", time.Now().UnixNano())
	createTopicWithPartitions(t, ctx, brokers, topic, numPartitions)

	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: false,
	}
	t.Cleanup(func() { _ = writer.Close() })
	publisher := adapter.NewAnalyticsPublisherWithWriter(writer, func() string { return fmt.Sprintf("evt-%d", time.Now().UnixNano()) })

	pathId, err := shared.NewPathId("pick-itest-balancer")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}

	occurredAt := time.Now().UTC().Truncate(time.Second)
	const sameWorkUnit = "wu-itest-same-partition"
	const otherWorkUnit = "wu-itest-other-partition"

	// Three different event types for the SAME work unit (mirrors the
	// real fleet ordering concern: created, then released, then
	// completed) plus one event for a different work unit, to prove the
	// key -- not accident -- drives partition placement.
	events := []shared.DomainEvent{
		shared.NewWorkUnitCreated(sameWorkUnit, pathId, occurredAt),
		shared.NewWorkReleased(sameWorkUnit, pathId, occurredAt.Add(time.Minute)),
		shared.NewWorkUnitCompleted(sameWorkUnit, pathId, occurredAt.Add(2*time.Minute)),
		shared.NewWorkUnitCreated(otherWorkUnit, pathId, occurredAt),
	}
	if err := publisher.Publish(ctx, events...); err != nil {
		t.Fatalf("publish events: %v", err)
	}

	// Read every message back with its partition using one reader per
	// partition (a single Reader without a fixed Partition only sees a
	// round-robin subset via consumer-group semantics, not every
	// partition's contents) so we can attribute each key to the
	// partition it actually landed on.
	partitionOf := map[string]int{}
	countByKeyAndPartition := map[string]int{}
	for p := 0; p < numPartitions; p++ {
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: p,
			MaxWait:   2 * time.Second,
		})
		func() {
			defer func() { _ = reader.Close() }()
			readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
			defer readCancel()
			for {
				msg, err := reader.ReadMessage(readCtx)
				if err != nil {
					return // timeout: no more messages on this partition
				}
				key := string(msg.Key)
				partitionOf[key] = p
				countByKeyAndPartition[fmt.Sprintf("%s@%d", key, p)]++
			}
		}()
	}

	if len(partitionOf) != 2 {
		t.Fatalf("observed keys->partition = %v, want exactly 2 distinct keys (%s, %s)", partitionOf, sameWorkUnit, otherWorkUnit)
	}
	samePartition, ok := partitionOf[sameWorkUnit]
	if !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", sameWorkUnit, partitionOf)
	}
	if _, ok := partitionOf[otherWorkUnit]; !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", otherWorkUnit, partitionOf)
	}

	// All 3 of sameWorkUnit's messages must have landed on the SAME
	// partition -- proving the guarantee holds across every message, not
	// just a one-message coincidence.
	gotCount := countByKeyAndPartition[fmt.Sprintf("%s@%d", sameWorkUnit, samePartition)]
	if gotCount != 3 {
		t.Errorf("found %d of %s's 3 messages on partition %d, want 3 (all events for one aggregate must share a partition)", gotCount, sameWorkUnit, samePartition)
	}
}

// createTopicWithPartitions creates topic on brokers with the given
// partition count and waits for it to become ready. numPartitions=8
// mirrors the exact "1->8 partitions" scaleup (warehouse-infra PR #42)
// that exposed the LeastBytes/Hash balancer mismatch fleet-wide.
func createTopicWithPartitions(t *testing.T, ctx context.Context, brokers []string, topic string, numPartitions int) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: numPartitions, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) >= numPartitions {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Kafka topic %q never became ready with %d partitions", topic, numPartitions)
}
