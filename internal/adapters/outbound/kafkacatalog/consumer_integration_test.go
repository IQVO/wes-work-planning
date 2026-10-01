//go:build integration

package kafkacatalog

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
)

// TestNewConsumer_TwoInstancesInARow_BothReplayFully is the real regression
// test for the shared-consumer-group bug: a second process must independently
// replay the full topic history rather than resume from a previous process's
// committed offset. The test owns its broker, so CI executes it rather than
// silently skipping when no external Kafka is available.
func TestNewConsumer_TwoInstancesInARow_BothReplayFully(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("wes-catalogue-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.process-path-management.events.itest-%d", time.Now().UnixNano())
	if err := createTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false}
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Value:   []byte(`{"specversion":"1.0","id":"0b7e6c1e-1f1a-4c55-9d8e-1a2b3c4d5e6f","source":"/warehouse/process-path-management","type":"com.warehouse.wes.process-path-management.processpath.ProcessPathCreated","subject":"ITEST2","time":"2026-09-30T12:00:00Z","datacontenttype":"application/json","dataschema":"urn:warehouse:process-path-management:events:ProcessPathCreated:v1","data":{"path_id":"ITEST2","match_prefix":"itest2","direct":true,"required_capabilities":["itest2"]}}`),
		Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
	}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close Kafka writer: %v", err)
	}

	for instance := 0; instance < 2; instance++ {
		consumer, err := NewConsumerForTopic(ctx, brokers, topic, nil)
		if err != nil {
			t.Fatalf("NewConsumerForTopic instance %d: %v", instance, err)
		}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- consumer.Run(runCtx) }()
		if err := consumer.WaitReady(ctx); err != nil {
			stop()
			t.Fatalf("consumer instance %d never became ready: %v", instance, err)
		}
		if _, err := consumer.Lookup("itest2-zone-a"); err != nil {
			stop()
			t.Fatalf("consumer instance %d did not replay catalogue: %v", instance, err)
		}
		stop()
		if err := <-done; err != nil {
			t.Fatalf("consumer instance %d stopped with error: %v", instance, err)
		}
		if err := consumer.Close(); err != nil {
			t.Fatalf("close consumer instance %d: %v", instance, err)
		}
	}
}

func createTopic(ctx context.Context, brokers []string, topic string) error {
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
