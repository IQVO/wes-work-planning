//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// Outbox wire test for decision 18 (ADR-0036): the line stored on the work
// unit reaches BOTH WorkReleased outbox rows (read inside the use case's
// transaction) and survives the relay to the sink; a unit with no line puts
// no line_no on either topic.

func workReleasedData(t *testing.T, value []byte) map[string]any {
	t.Helper()
	evt, err := cloudevents.Decode(value)
	if err != nil {
		t.Fatalf("decode CloudEvent: %v", err)
	}
	var data map[string]any
	if err := evt.DataAs(&data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	return data
}

func TestOutbox_WorkReleased_CarriesLineNoOnBothTopics(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	clock := memory.FixedClock{At: time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)}
	integration, analytics := encoders(pool)
	pub := postgres.NewOutboxPublisher(pool, integration, analytics)
	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, pub, clock).WithUnitOfWork(uow)
	release := usecases.NewReleaseNextWork(pools, workUnits, pub, clock).WithUnitOfWork(uow)

	// Line 3 of an order, enqueued the way ApplyOrderAllocated does.
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "order-7-line-3", PathId: mustPath(t, "pick-ln"), CPT: shared.NewCPT(clock.Now().Add(time.Hour)),
		Reference: "order-7", SKU: "sku-3", LineNo: 3,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := release.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: mustPath(t, "pick-ln")}); err != nil {
		t.Fatalf("release: %v", err)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	if _, err := relay.RelayOnce(ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}

	seen := map[string]map[string]any{}
	for _, m := range sink.sent {
		if m.EventType != "com.warehouse.wes.work-planning.workunit.WorkReleased" {
			continue
		}
		seen[m.Topic] = workReleasedData(t, m.Value)
	}
	for _, topic := range []string{cloudevents.TopicWorkPlanningEvents, outboundkafka.AnalyticsTopic} {
		data, ok := seen[topic]
		if !ok {
			t.Fatalf("no WorkReleased relayed on %s", topic)
		}
		if data["line_no"] != float64(3) {
			t.Errorf("%s: line_no = %v, want 3 (payload %v)", topic, data["line_no"], data)
		}
		if data["work_unit_id"] != "order-7-line-3" {
			t.Errorf("%s: work_unit_id = %v, the id must stay <order>-line-<n>", topic, data["work_unit_id"])
		}
	}
	// The integration payload's other enrichment still comes through.
	if seen[cloudevents.TopicWorkPlanningEvents]["ref"] != "order-7" {
		t.Errorf("integration ref = %v, want order-7", seen[cloudevents.TopicWorkPlanningEvents]["ref"])
	}

	// Sanity: the stored row is what was published.
	var stored int
	if err := pool.QueryRow(ctx, `SELECT line_no FROM work_units WHERE id = 'order-7-line-3'`).Scan(&stored); err != nil || stored != 3 {
		t.Fatalf("stored line_no = %d, err %v, want 3", stored, err)
	}
}

func TestOutbox_WorkReleased_UnitWithoutLineNoPublishesNone(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	clock := memory.FixedClock{At: time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)}
	integration, analytics := encoders(pool)
	pub := postgres.NewOutboxPublisher(pool, integration, analytics)
	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)

	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, pub, clock).WithUnitOfWork(uow)
	release := usecases.NewReleaseNextWork(pools, workUnits, pub, clock).WithUnitOfWork(uow)
	if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-no-line", PathId: mustPath(t, "pick-ln"), CPT: shared.NewCPT(clock.Now().Add(time.Hour)), Reference: "order-8",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := release.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: mustPath(t, "pick-ln")}); err != nil {
		t.Fatalf("release: %v", err)
	}

	sink := &recordingSink{}
	if _, err := postgres.NewOutboxRelay(pool, sink, slog.Default()).RelayOnce(ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}
	checked := 0
	for _, m := range sink.sent {
		if m.EventType != "com.warehouse.wes.work-planning.workunit.WorkReleased" {
			continue
		}
		checked++
		var raw map[string]json.RawMessage
		evt, err := cloudevents.Decode(m.Value)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if err := evt.DataAs(&raw); err != nil {
			t.Fatalf("data: %v", err)
		}
		if _, ok := raw["line_no"]; ok {
			t.Errorf("%s: line_no must be omitted when unknown, got %s", m.Topic, m.Value)
		}
	}
	if checked != 2 {
		t.Fatalf("expected a WorkReleased on both topics, saw %d", checked)
	}
}
