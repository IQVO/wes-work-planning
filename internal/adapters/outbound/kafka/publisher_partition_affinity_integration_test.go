//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	adapter "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// TestPublisherKeysEveryEventOfOneWorkUnitOntoTheSamePartition is the
// integration-topic counterpart of the analytics affinity test (ADR-0024):
// WorkUnitCreated, WorkReleased and WorkUnitCompleted for ONE work unit —
// each minted with a DIFFERENT CloudEvents id — must land on one partition of
// an 8-partition topic. Keying by the event id (the pre-fix behaviour) scatters
// them; keying by the aggregate id keeps them together and ordered.
func TestPublisherKeysEveryEventOfOneWorkUnitOntoTheSamePartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("wes-work-planning-kafka-itest-pub-affinity"),
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
	topic := fmt.Sprintf("warehouse.work-planning.events.itest-part-%d", time.Now().UnixNano())
	createTopicWithPartitions(t, ctx, brokers, topic, numPartitions)

	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: false,
	}
	t.Cleanup(func() { _ = writer.Close() })

	const sameWorkUnit = "wu-pub-same-partition"
	const otherWorkUnit = "wu-pub-other-partition"
	// WorkReleased's payload is enriched from the WorkUnit repo, so the
	// released unit must exist there.
	workUnits := newReleasedWorkUnitWithGiftWrap(t, sameWorkUnit, "sku-1", false)

	var seq atomic.Int64
	newID := func() string { return fmt.Sprintf("evt-%d", seq.Add(1)) }
	publisher := adapter.NewPublisherWithWriter(writer, workUnits, nil, newID)

	pathId, err := shared.NewPathId("pick-itest-pub-affinity")
	if err != nil {
		t.Fatalf("NewPathId: %v", err)
	}
	at := time.Now().UTC().Truncate(time.Second)
	if err := publisher.Publish(ctx,
		shared.NewWorkUnitCreated(sameWorkUnit, pathId, at),
		shared.NewWorkReleased(sameWorkUnit, pathId, at.Add(time.Minute)),
		shared.NewWorkUnitCompleted(sameWorkUnit, pathId, at.Add(2*time.Minute)),
		shared.NewWorkUnitCreated(otherWorkUnit, pathId, at),
	); err != nil {
		t.Fatalf("publish: %v", err)
	}

	partitionOf := map[string]int{}
	countByKeyAndPartition := map[string]int{}
	for p := 0; p < numPartitions; p++ {
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers, Topic: topic, Partition: p, MaxWait: 2 * time.Second,
		})
		func() {
			defer func() { _ = reader.Close() }()
			readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
			defer readCancel()
			for {
				msg, err := reader.ReadMessage(readCtx)
				if err != nil {
					return
				}
				key := string(msg.Key)
				partitionOf[key] = p
				countByKeyAndPartition[fmt.Sprintf("%s@%d", key, p)]++
			}
		}()
	}

	if len(partitionOf) != 2 {
		t.Fatalf("observed keys->partition = %v, want exactly 2 distinct keys (%s, %s): the key must be the aggregate id, not the event id",
			partitionOf, sameWorkUnit, otherWorkUnit)
	}
	samePartition, ok := partitionOf[sameWorkUnit]
	if !ok {
		t.Fatalf("no message keyed %q; partitionOf = %v", sameWorkUnit, partitionOf)
	}
	if got := countByKeyAndPartition[fmt.Sprintf("%s@%d", sameWorkUnit, samePartition)]; got != 3 {
		t.Errorf("found %d of %s's 3 messages on partition %d, want 3", got, sameWorkUnit, samePartition)
	}
}
