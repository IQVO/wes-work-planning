//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0042 end to end on a real Kafka broker and a real Postgres
// (testcontainers), wired like cmd/wes with REPLENISHMENT_MODE=kafka: the real
// ReplenishmentConsumer, the real postgres UnitOfWork, repos and outbox. Every
// topic (and its dead-letter topic) is created explicitly and every read of
// Kafka is bounded by a context deadline.

// replWorkReleased is the WorkReleased data fulfillment-execution's REPLENISH
// task decodes (its ADR 0050), field for field.
type replWorkReleased struct {
	PathId        string `json:"path_id"`
	WorkUnitId    string `json:"work_unit_id"`
	Ref           string `json:"ref"`
	WorkKind      string `json:"work_kind,omitempty"`
	TransferRef   string `json:"transfer_ref,omitempty"`
	Relocation    any    `json:"relocation,omitempty"`
	Replenishment *struct {
		RequestId string `json:"request_id"`
		SKU       string `json:"sku"`
		FromBin   string `json:"from_bin"`
		ToBin     string `json:"to_bin"`
		Quantity  *int   `json:"quantity"`
		Priority  string `json:"priority"`
	} `json:"replenishment"`
}

// replEnv is the shared infrastructure: one Postgres and one Kafka.
type replEnv struct {
	ctx     context.Context
	brokers []string
	pool    *pgxpool.Pool
}

func startReplEnv(t *testing.T, ctx context.Context) replEnv {
	t.Helper()
	pool := startAtomicityPostgres(t, ctx)
	kafkaContainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("wes-repl-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, kafkaContainer)
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	return replEnv{ctx: ctx, brokers: brokers, pool: pool}
}

// replStack is one running consumer with the use cases it feeds.
type replStack struct {
	topic, dlq, group string
	workUnits         *postgres.WorkUnitRepo
	releaseNext       *usecases.ReleaseNextWork
	logs              *lockedBuffer
}

func (e replEnv) startStack(t *testing.T, name string) replStack {
	t.Helper()
	suffix := time.Now().UnixNano()
	s := replStack{
		topic: fmt.Sprintf("%s.itest-%s-%d", cloudevents.TopicPickFaceReplenishmentEvents, name, suffix),
		group: fmt.Sprintf("wes-repl-itest-%s-%d", name, suffix),
		logs:  &lockedBuffer{},
	}
	s.dlq = s.topic + ".dlq"
	if err := createTopics(e.ctx, e.brokers, s.topic, s.dlq); err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}

	uow := postgres.NewUnitOfWork(e.pool)
	s.workUnits = postgres.NewWorkUnitRepo(e.pool)
	pools := postgres.NewWorkPoolRepo(e.pool)
	processed := postgres.NewProcessedEventRepo(e.pool)
	clock := memory.SystemClock{}
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "REPLENISH", MatchPrefix: "replenish", RequiredCapabilities: []string{"replenish"}}})
	encoder := outboundkafka.NewPublisherWithWriter(&kafkago.Writer{
		Addr: kafkago.TCP(e.brokers...), Topic: cloudevents.TopicWorkPlanningEvents, Balancer: &kafkago.Hash{},
		BatchTimeout: 10 * time.Millisecond, RequiredAcks: kafkago.RequireAll,
	}, s.workUnits, nil, func() string { return fmt.Sprintf("evt-out-%d", time.Now().UnixNano()) })
	publisher := postgres.NewOutboxPublisher(e.pool, encoder)

	requested := usecases.NewApplyReplenishmentRequested(s.workUnits, pools, processed, catalogue, publisher, clock).WithUnitOfWork(uow)
	cancelled := usecases.NewApplyReplenishmentCancelled(s.workUnits, pools, processed, clock).WithUnitOfWork(uow)
	s.releaseNext = usecases.NewReleaseNextWork(pools, s.workUnits, publisher, clock).WithUnitOfWork(uow)

	logger := slog.New(slog.NewTextHandler(s.logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	consumer := inboundkafka.NewReplenishmentConsumerForTopic(e.brokers, s.group, s.topic, requested, cancelled, logger)
	runCtx, cancel := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = consumer.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = consumer.Close()
	})
	return s
}

func replRequestedEvent(t *testing.T, eventId, requestId, priority string, quantity int, source, forward string) []byte {
	t.Helper()
	return mustCloudEventJSON(t, eventId, cloudevents.TypeReplenishmentRequested, "pick-face-replenishment", map[string]any{
		"request_id": requestId, "site_id": "WH1", "sku": "SKU-1", "forward_bin": forward, "source_bin": source,
		"quantity": quantity, "priority": priority, "policy_min": 10, "policy_max": 60,
		"requested_at": time.Now().UTC().Format(time.RFC3339),
	})
}

func replCancelledEvent(t *testing.T, eventId, requestId string) []byte {
	t.Helper()
	return mustCloudEventJSON(t, eventId, cloudevents.TypeReplenishmentCancelled, "pick-face-replenishment", map[string]any{
		"request_id": requestId, "site_id": "WH1", "sku": "SKU-1", "reason": "OPERATOR",
		"cancelled_at": time.Now().UTC().Format(time.RFC3339),
	})
}

// send writes the messages to the topic in order (one partition), keyed by
// key and carrying the CloudEvents content-type header unless raw is set.
func (s replStack) send(t *testing.T, e replEnv, key string, withHeader bool, values ...[]byte) {
	t.Helper()
	w := &kafkago.Writer{Addr: kafkago.TCP(e.brokers...), Topic: s.topic, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond}
	defer func() { _ = w.Close() }()
	msgs := make([]kafkago.Message, 0, len(values))
	for _, v := range values {
		m := kafkago.Message{Key: []byte(key), Value: v}
		if withHeader {
			m.Headers = cloudEventHeaders()
		}
		msgs = append(msgs, m)
	}
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()
	// A freshly created topic can still be unknown to the broker that serves
	// the produce even after the partition listing shows it: retry that one
	// error until the deadline, fail on anything else.
	for {
		err := w.WriteMessages(ctx, msgs...)
		if err == nil {
			return
		}
		if !errors.Is(err, kafkago.UnknownTopicOrPartition) || ctx.Err() != nil {
			t.Fatalf("publish to %s: %v", s.topic, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// committedOffset is the offset the consumer group committed on partition 0.
func (s replStack) committedOffset(t *testing.T, e replEnv) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)
	defer cancel()
	client := &kafkago.Client{Addr: kafkago.TCP(e.brokers...)}
	resp, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{GroupID: s.group, Topics: map[string][]int{s.topic: {0}}})
	if err != nil {
		return -1
	}
	parts := resp.Topics[s.topic]
	if len(parts) == 0 {
		return -1
	}
	return parts[0].CommittedOffset
}

// assertCommitted waits until the group committed `want` offsets: every
// message, the skipped and the dead-lettered ones included, was committed past.
func (s replStack) assertCommitted(t *testing.T, e replEnv, want int64) {
	t.Helper()
	eventually(t, fmt.Sprintf("%d committed offsets of %s", want, s.topic), func() bool {
		return s.committedOffset(t, e) == want
	})
}

// deadLetters reads exactly `want` messages of the dead-letter topic (bounded
// by a deadline) and fails if one more arrives within a grace period.
func (s replStack) deadLetters(t *testing.T, e replEnv, want int) []kafkago.Message {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: e.brokers, Topic: s.dlq, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer func() { _ = r.Close() }()
	if err := r.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatalf("seek DLQ: %v", err)
	}
	ctx, cancel := context.WithTimeout(e.ctx, 60*time.Second)
	defer cancel()
	got := make([]kafkago.Message, 0, want)
	for len(got) < want {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read DLQ %s: got %d of %d messages: %v", s.dlq, len(got), want, err)
		}
		got = append(got, m)
	}
	extraCtx, extraCancel := context.WithTimeout(e.ctx, 3*time.Second)
	defer extraCancel()
	if extra, err := r.ReadMessage(extraCtx); err == nil {
		t.Fatalf("unexpected extra dead letter %s", extra.Value)
	}
	return got
}

func dlqHeader(m kafkago.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func replCount(t *testing.T, e replEnv, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(e.ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", query, err)
	}
	return n
}

func outboxRowsOf(t *testing.T, e replEnv, unitId string) int {
	t.Helper()
	return replCount(t, e, `SELECT count(*) FROM outbox_events WHERE key = $1`, []byte(unitId))
}

func TestReplenishmentConsumer_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 280*time.Second)
	defer cancel()
	env := startReplEnv(t, ctx)

	t.Run("a stream of valid, duplicate, legacy, unknown and poison messages", func(t *testing.T) {
		replEndToEnd(t, env)
	})
	t.Run("the claim, the unit and the outbox roll back together", func(t *testing.T) {
		replAtomicity(t, env)
	})
}

// replEndToEnd feeds one stream, in order:
//
//	0 ReplenishmentRequested A (event e1)            -> one unit
//	1 the same bytes again (event e1)                -> no-op (processed mark)
//	2 the same request under another id (event e2)   -> still one unit
//	3 a legacy flat message                          -> WARN + skip
//	4 ReplenishmentCompleted (unknown to this side)  -> ignored
//	5 a Requested with quantity 0                    -> dead letter
//	6 ReplenishmentRequested B (URGENT)              -> a second unit
//	7 ReplenishmentCancelled B                       -> B cancelled while Pending
func replEndToEnd(t *testing.T, env replEnv) {
	s := env.startStack(t, "e2e")
	suffix := time.Now().UnixNano()
	reqA, reqB := fmt.Sprintf("repl-itest-a-%d", suffix), fmt.Sprintf("repl-itest-b-%d", suffix)
	evA1, evA2, evBad, evB, evBCancel := fmt.Sprintf("evt-a1-%d", suffix), fmt.Sprintf("evt-a2-%d", suffix), fmt.Sprintf("evt-bad-%d", suffix), fmt.Sprintf("evt-b-%d", suffix), fmt.Sprintf("evt-bc-%d", suffix)

	requestedA := replRequestedEvent(t, evA1, reqA, "NORMAL", 24, "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-01-A")
	poison := replRequestedEvent(t, evBad, "repl-itest-bad-"+fmt.Sprint(suffix), "NORMAL", 0, "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-01-A")
	legacy := []byte(`{"event_type":"ReplenishmentRequested","request_id":"legacy","sku":"SKU-1","quantity":5}`)
	completed := mustCloudEventJSON(t, "evt-done-"+fmt.Sprint(suffix), "com.warehouse.wms.pick-face-replenishment.replenishment.ReplenishmentCompleted", "pick-face-replenishment", map[string]any{"request_id": reqA})

	s.send(t, env, reqA, true, requestedA, requestedA, replRequestedEvent(t, evA2, reqA, "NORMAL", 24, "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-01-A"))
	s.send(t, env, "legacy", false, legacy)
	s.send(t, env, reqA, true, completed, poison)
	s.send(t, env, reqB, true, replRequestedEvent(t, evB, reqB, "URGENT", 6, "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-02-A"), replCancelledEvent(t, evBCancel, reqB))

	// Every message, skipped and dead-lettered ones included, is committed past.
	s.assertCommitted(t, env, 8)

	unitA, unitB := workunit.ReplenishmentWorkUnitId(reqA), workunit.ReplenishmentWorkUnitId(reqB)
	assertPlannedUnit(t, env, s, unitA, reqA)
	if n := replCount(t, env, `SELECT count(*) FROM work_units WHERE id = $1`, unitA); n != 1 {
		t.Fatalf("request A planned %d units, want exactly 1 (duplicate id and same request under another id)", n)
	}
	assertCancelledUnit(t, env, s, unitB)

	for _, id := range []string{evA1, evA2, evB, evBCancel} {
		if !processedRowExists(t, env.pool, id) {
			t.Fatalf("event %s must be marked processed", id)
		}
	}
	if processedRowExists(t, env.pool, evBad) {
		t.Fatal("a dead-lettered event must not be marked processed")
	}
	if logs := s.logs.String(); !strings.Contains(logs, "not a valid CloudEvent") {
		t.Fatalf("the legacy flat message must be skipped with a WARN, logs:\n%s", logs)
	}

	letters := s.deadLetters(t, env, 1)
	if string(letters[0].Value) != string(poison) || dlqHeader(letters[0], "x-dlq-source-topic") != s.topic {
		t.Fatalf("dead letter = %s %v, want the poison message from %s", letters[0].Value, letters[0].Headers, s.topic)
	}

	assertReleasedInFulfillmentShape(t, env, s, unitA, reqA)
}

func assertPlannedUnit(t *testing.T, env replEnv, s replStack, unitId, requestId string) {
	t.Helper()
	unit, err := s.workUnits.FindById(env.ctx, unitId)
	if err != nil {
		t.Fatalf("FindById %s: %v", unitId, err)
	}
	move, ok := unit.Replenishment()
	if !ok || unit.WorkKind() != workunit.WorkKindReplenish || unit.State() != workunit.Pending || unit.Reference() != requestId || unit.SiteId() != "WH1" {
		t.Fatalf("unit %s = kind %v state %v ref %q site %q move %+v", unitId, unit.WorkKind(), unit.State(), unit.Reference(), unit.SiteId(), move)
	}
	if move.FromBin != "WH1-RSV-A07-03-02-B" || move.ToBin != "WH1-FWD-A01-01-01-A" || move.Quantity != 24 || move.Priority != workunit.ReplenishmentPriorityNormal || move.SKU != "SKU-1" {
		t.Fatalf("replenishment = %+v", move)
	}
	if replCount(t, env, `SELECT count(*) FROM outbox_events WHERE key = $1`, []byte(unitId)) == 0 {
		t.Fatalf("WorkUnitCreated of %s was not outboxed", unitId)
	}
}

func assertCancelledUnit(t *testing.T, env replEnv, s replStack, unitId string) {
	t.Helper()
	unit, err := s.workUnits.FindById(env.ctx, unitId)
	if err != nil {
		t.Fatalf("FindById %s: %v", unitId, err)
	}
	move, _ := unit.Replenishment()
	if unit.State() != workunit.Cancelled || move.Priority != workunit.ReplenishmentPriorityUrgent {
		t.Fatalf("unit %s = state %v priority %v, want a cancelled URGENT unit", unitId, unit.State(), move.Priority)
	}
}

// assertReleasedInFulfillmentShape releases the REPLENISH path and decodes the
// outboxed WorkReleased the way fulfillment-execution's consumer does.
func assertReleasedInFulfillmentShape(t *testing.T, env replEnv, s replStack, unitId, requestId string) {
	t.Helper()
	pathId, _ := shared.NewPathId(usecases.ReplenishPathId)
	released, err := s.releaseNext.Execute(env.ctx, usecases.ReleaseNextWorkRequest{PathId: pathId})
	if err != nil || released.Id() != unitId {
		t.Fatalf("release = (%v, %v), want %s (the cancelled unit must never be released)", released, err, unitId)
	}
	var got replWorkReleased
	if err := json.Unmarshal(outboxedWorkReleased(t, env.ctx, env.pool, unitId), &got); err != nil {
		t.Fatalf("decode WorkReleased as fulfillment-execution: %v", err)
	}
	if got.PathId != "REPLENISH" || got.Ref != requestId || got.WorkKind != "" || got.TransferRef != "" || got.Relocation != nil {
		t.Fatalf("WorkReleased data = %+v", got)
	}
	assertReplenishmentObject(t, got)
}

func assertReplenishmentObject(t *testing.T, got replWorkReleased) {
	t.Helper()
	r := got.Replenishment
	if r == nil || r.Quantity == nil {
		t.Fatalf("replenishment = %+v, want the object with a quantity", r)
	}
	if r.RequestId != got.Ref || r.SKU != "SKU-1" || r.Priority != "NORMAL" || *r.Quantity != 24 {
		t.Fatalf("replenishment = %+v", r)
	}
	if r.FromBin != "WH1-RSV-A07-03-02-B" || r.ToBin != "WH1-FWD-A01-01-01-A" {
		t.Fatalf("replenishment bins = %+v", r)
	}
}

// replAtomicity makes Postgres itself fail the outbox insert of one unit (a real
// BEFORE INSERT trigger raising an error, no Go fake), which happens inside the
// open transaction AFTER the claim, the pool and the unit were written: all of
// them must roll back together. The failing event is retried, dead-lettered and
// committed past; once the cause is removed, replaying the SAME CloudEvents id
// plans the unit exactly once, proving the failed attempt left no mark behind.
func replAtomicity(t *testing.T, env replEnv) {
	s := env.startStack(t, "atomic")
	suffix := time.Now().UnixNano()
	reqC := fmt.Sprintf("repl-itest-c-%d", suffix)
	evC := fmt.Sprintf("evt-c-%d", suffix)
	unitC := workunit.ReplenishmentWorkUnitId(reqC)

	mustExec(t, env, `CREATE OR REPLACE FUNCTION repl_itest_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'repl itest: injected outbox failure'; END $$`)
	mustExec(t, env, fmt.Sprintf(`CREATE TRIGGER repl_itest_fail_c BEFORE INSERT ON outbox_events FOR EACH ROW WHEN (NEW.key = '%s'::bytea) EXECUTE FUNCTION repl_itest_fail()`, unitC))

	// (1) Every attempt fails after the claim, the pool and the unit were written.
	eventC := replRequestedEvent(t, evC, reqC, "NORMAL", 12, "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-03-A")
	s.send(t, env, reqC, true, eventC)
	letters := s.deadLetters(t, env, 1)
	if string(letters[0].Value) != string(eventC) {
		t.Fatalf("dead letter = %s, want the failing event", letters[0].Value)
	}
	s.assertCommitted(t, env, 1)
	if processedRowExists(t, env.pool, evC) {
		t.Fatal("the claim of a failed attempt must roll back with it")
	}
	if n := replCount(t, env, `SELECT count(*) FROM work_units WHERE id = $1`, unitC); n != 0 {
		t.Fatalf("a rolled-back attempt left %d work unit rows", n)
	}
	if n := outboxRowsOf(t, env, unitC); n != 0 {
		t.Fatalf("a rolled-back attempt left %d outbox rows", n)
	}

	// (2) The cause is removed and the same event (same CloudEvents id) is
	// replayed: it is applied, once, with its outbox rows.
	mustExec(t, env, `DROP TRIGGER repl_itest_fail_c ON outbox_events`)
	s.send(t, env, reqC, true, eventC)
	s.assertCommitted(t, env, 2)
	if !processedRowExists(t, env.pool, evC) {
		t.Fatal("the replayed event must be marked processed once applied")
	}
	if n := replCount(t, env, `SELECT count(*) FROM work_units WHERE id = $1`, unitC); n != 1 {
		t.Fatalf("replayed request planned %d units, want 1", n)
	}
	if n := outboxRowsOf(t, env, unitC); n == 0 {
		t.Fatal("the applied attempt must have committed its outbox rows")
	}
	if got := s.deadLetters(t, env, 1); string(got[0].Value) != string(eventC) {
		t.Fatalf("only the failed attempt may be dead-lettered, got %s", got[0].Value)
	}
}

func mustExec(t *testing.T, env replEnv, sql string) {
	t.Helper()
	if _, err := env.pool.Exec(env.ctx, sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}
