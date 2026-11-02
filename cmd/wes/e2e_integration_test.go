//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
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

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// The end-to-end proof of the real-wiring rule (ADR-0044): the REAL entry
// point, run(ctx), booted in-process with only the deployment's environment
// against a real Postgres and a real Kafka (testcontainers), driven ONLY
// through REST and Kafka. Assertions are on Postgres rows and on the
// CloudEvents the relay publishes. Every Kafka read is bounded by a deadline.

const e2eCatalogue = `building: e2e
paths:
  - id: PICK
    matchPrefix: pick
    direct: true
    requiredCapabilities: [pick]
  - id: REPLENISH
    matchPrefix: replenish
    direct: true
    requiredCapabilities: [replenish]
  - id: RELOCATE
    matchPrefix: relocate
    direct: true
    requiredCapabilities: [relocate]
`

type e2eEnv struct {
	dsn       string
	brokers   []string
	addr      string
	dir       string
	suffix    int64
	kafkaCont testcontainers.Container
}

// consumedTopics are the topics this service's consumers read. A kafka-go
// group consumer that starts before its topic exists does not pick the topic
// up when it appears (ADR-0044, same finding as inbound-receiving ADR 0011), so
// the test creates them like the cluster does. The OUTPUT topics are NOT
// created: the relay's writer auto-creates them, as in the cluster.
var consumedTopics = []string{
	cloudevents.TopicWorkforceEvents, cloudevents.TopicInventoryEvents, cloudevents.TopicFulfillmentEvents,
	cloudevents.TopicOrderManagementEvents, cloudevents.TopicNetworkDemandEvents, cloudevents.TopicSlottingEvents,
	cloudevents.TopicProductMasterEvents, cloudevents.TopicPickFaceReplenishmentEvents,
}

func startE2EInfra(t *testing.T, ctx context.Context) e2eEnv {
	t.Helper()
	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wes_work_planning"), tcpostgres.WithUsername("wes"), tcpostgres.WithPassword("wes"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}

	suffix := time.Now().UnixNano()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID(fmt.Sprintf("wes-e2e-%d", suffix)))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kc) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	for _, topic := range consumedTopics {
		e2eCreateTopic(t, ctx, brokers[0], topic)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "process-paths.yaml"), []byte(e2eCatalogue), 0o600); err != nil {
		t.Fatal(err)
	}
	return e2eEnv{dsn: dsn, brokers: brokers, addr: e2eFreeAddr(t), dir: dir, suffix: suffix, kafkaCont: kc}
}

func e2eFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func e2eCreateTopic(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial kafka: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	e2eWaitFor(t, ctx, 30*time.Second, "topic "+topic+" has a leader", func() bool {
		parts, err := conn.ReadPartitions(topic)
		return err == nil && len(parts) == 1 && parts[0].Leader.Host != ""
	})
}

func e2eWaitFor(t *testing.T, ctx context.Context, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

// startE2EService runs the REAL entry point in-process and returns a stop func
// that cancels it and returns run()'s result.
func startE2EService(t *testing.T, ctx context.Context, env e2eEnv) func() error {
	t.Helper()
	t.Setenv("DATABASE_URL", env.dsn)
	t.Setenv("MIGRATIONS_PATH", "../../migrations")
	t.Setenv("KAFKA_BROKERS", strings.Join(env.brokers, ","))
	t.Setenv("KAFKA_CONSUMER_GROUP", fmt.Sprintf("wes-e2e-%d", env.suffix))
	t.Setenv("PRODUCT_CLASSIFICATION_CONSUMER_GROUP", fmt.Sprintf("wes-e2e-classification-%d", env.suffix))
	t.Setenv("REPLENISHMENT_CONSUMER_GROUP", fmt.Sprintf("wes-e2e-replenishment-%d", env.suffix))
	t.Setenv("PACK_VIEW_CONSUMER_GROUP", fmt.Sprintf("wes-e2e-pack-view-%d", env.suffix))
	t.Setenv("PRODUCT_CLASSIFICATION_MODE", "kafka")
	t.Setenv("REPLENISHMENT_MODE", "kafka")
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv("HTTP_ADDR", env.addr)
	t.Setenv("PATH_CATALOGUE_FILE", filepath.Join(env.dir, "process-paths.yaml"))
	t.Setenv("OUTBOX_RELAY_INTERVAL", "200ms")
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from retrying a dial
	// to localhost:4317 at shutdown.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- run(runCtx) }()
	stopped := false
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(40 * time.Second):
			return errors.New("run() did not return within 40s of cancellation")
		}
	}
	t.Cleanup(func() { _ = stop() })

	e2eWaitFor(t, ctx, 120*time.Second, "GET /readyz = 200 on real Postgres + Kafka", func() bool {
		code, _, _ := e2eDo(t, ctx, http.MethodGet, env.url("/readyz"), "", "")
		return code == http.StatusOK
	})
	return stop
}

func (e e2eEnv) url(path string) string { return "http://" + e.addr + path }

// e2eDo issues a REST call and returns status, body and headers. A transport
// error returns 0 (the service may still be starting).
func e2eDo(t *testing.T, ctx context.Context, method, url, key, body string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", nil
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func e2eCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// e2eEvent is one message read from an output topic.
type e2eEvent struct {
	ce.Event
	Key string
}

// e2eEventsOn reads topic from the beginning, in a loop bounded by a deadline
// (the relay auto-creates the topic on its first write, so it may not exist
// yet), until every wanted "<EventName>|<subject>" was seen.
func e2eEventsOn(t *testing.T, ctx context.Context, brokers []string, topic string, want ...string) map[string]e2eEvent {
	t.Helper()
	got := map[string]e2eEvent{}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MaxWait: 500 * time.Millisecond})
		if err := r.SetOffset(kafkago.FirstOffset); err == nil {
			readCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			for {
				m, err := r.ReadMessage(readCtx)
				if err != nil {
					break
				}
				var ev ce.Event
				if json.Unmarshal(m.Value, &ev) == nil {
					name := ev.Type()[strings.LastIndex(ev.Type(), ".")+1:]
					got[name+"|"+ev.Subject()] = e2eEvent{Event: ev, Key: string(m.Key)}
				}
			}
			cancel()
		}
		_ = r.Close()
		missing := false
		for _, w := range want {
			if _, ok := got[w]; !ok {
				missing = true
			}
		}
		if !missing {
			return got
		}
		time.Sleep(500 * time.Millisecond)
	}
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	t.Fatalf("timed out reading %s: wanted %v, saw %v", topic, want, keys)
	return nil
}

func e2eProduce(t *testing.T, ctx context.Context, env e2eEnv, topic, key string, value []byte) {
	t.Helper()
	w := &kafkago.Writer{Addr: kafkago.TCP(env.brokers...), Topic: topic, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond}
	defer func() { _ = w.Close() }()
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(wctx, kafkago.Message{Key: []byte(key), Value: value, Headers: []kafkago.Header{cloudevents.ContentTypeHeader()}}); err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
}

// e2eReleased decodes the WorkReleased data the way fulfillment-execution's
// consumer does (its REPLENISH task decodes the replenishment object).
type e2eReleased struct {
	PathId        string `json:"path_id"`
	WorkUnitId    string `json:"work_unit_id"`
	Ref           string `json:"ref"`
	WorkKind      string `json:"work_kind,omitempty"`
	Replenishment *struct {
		RequestId string `json:"request_id"`
		SKU       string `json:"sku"`
		FromBin   string `json:"from_bin"`
		ToBin     string `json:"to_bin"`
		Quantity  *int   `json:"quantity"`
		Priority  string `json:"priority"`
	} `json:"replenishment"`
}

func TestRunBootsOnRealPostgresAndKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	env := startE2EInfra(t, ctx)
	stop := startE2EService(t, ctx, env)
	pool, err := pgxpool.New(ctx, env.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	t.Run("readyz is 200 only because Postgres and Kafka answer", func(t *testing.T) {
		code, body, _ := e2eDo(t, ctx, http.MethodGet, env.url("/readyz"), "", "")
		if code != http.StatusOK || !strings.Contains(body, `"ready"`) {
			t.Fatalf("readyz = %d %s", code, body)
		}
		if code, _, _ := e2eDo(t, ctx, http.MethodGet, env.url("/healthz"), "", ""); code != http.StatusOK {
			t.Fatalf("healthz = %d", code)
		}
	})

	t.Run("a REST write is a Postgres row, an idempotent replay and a published CloudEvent", func(t *testing.T) {
		e2eRESTFlow(t, ctx, env, pool)
	})

	t.Run("ReplenishmentRequested over Kafka plans REPLENISH work that is released to fulfillment", func(t *testing.T) {
		e2eReplenishmentFlow(t, ctx, env, pool)
	})

	t.Run("SlotPlanApproved over Kafka plans RELOCATE work", func(t *testing.T) {
		e2eSlotPlanFlow(t, ctx, env, pool)
	})

	t.Run("readyz goes 503 when the broker goes away, and shutdown still drains", func(t *testing.T) {
		timeout := 10 * time.Second
		if err := env.kafkaCont.Stop(ctx, &timeout); err != nil {
			t.Fatalf("stop kafka: %v", err)
		}
		e2eWaitFor(t, ctx, 30*time.Second, "GET /readyz = 503 dependency-unavailable (kafka)", func() bool {
			code, body, _ := e2eDo(t, ctx, http.MethodGet, env.url("/readyz"), "", "")
			return code == http.StatusServiceUnavailable && strings.Contains(body, "dependency-unavailable") && strings.Contains(body, "kafka")
		})
		// Liveness is unaffected by a dependency.
		if code, _, _ := e2eDo(t, ctx, http.MethodGet, env.url("/healthz"), "", ""); code != http.StatusOK {
			t.Fatalf("healthz with the broker down = %d, want 200", code)
		}
		start := time.Now()
		_ = stop()
		if d := time.Since(start); d > 35*time.Second {
			t.Fatalf("shutdown with the broker down took %s", d)
		}
	})
}

func e2eRESTFlow(t *testing.T, ctx context.Context, env e2eEnv, pool *pgxpool.Pool) {
	t.Helper()
	const unit = "wu-e2e-1"
	path := env.url("/paths/pick-e2e/work-units")
	body := `{"workUnitId":"wu-e2e-1","cpt":"2026-12-01T12:00:00Z","reference":"order-e2e-1","sku":"SKU-E2E"}`

	if code, resp, _ := e2eDo(t, ctx, http.MethodPost, path, "", body); code != http.StatusBadRequest || !strings.Contains(resp, "idempotency-key-required") {
		t.Fatalf("POST without Idempotency-Key = %d %s, want 400 idempotency-key-required (Postgres is the only runtime)", code, resp)
	}
	code, first, _ := e2eDo(t, ctx, http.MethodPost, path, "key-e2e-1", body)
	if code != http.StatusCreated {
		t.Fatalf("POST work unit = %d %s", code, first)
	}
	if code, replay, _ := e2eDo(t, ctx, http.MethodPost, path, "key-e2e-1", body); code != http.StatusCreated || replay != first {
		t.Fatalf("replay = %d %s, want the stored 201 %s", code, replay, first)
	}
	if code, resp, _ := e2eDo(t, ctx, http.MethodPost, path, "key-e2e-1", strings.Replace(body, "SKU-E2E", "SKU-OTHER", 1)); code != http.StatusConflict || !strings.Contains(resp, "idempotency-key-reused") {
		t.Fatalf("same key, other body = %d %s, want 409 idempotency-key-reused", code, resp)
	}
	nul := strings.Replace(body, "order-e2e-1", `order\u0000-1`, 1)
	if code, resp, _ := e2eDo(t, ctx, http.MethodPost, path, "key-e2e-nul", nul); code != http.StatusBadRequest || !strings.Contains(resp, "malformed-request-body") {
		t.Fatalf("NUL in the body = %d %s, want 400 malformed-request-body (not a 500)", code, resp)
	}

	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM work_units WHERE id = $1`, unit); n != 1 {
		t.Fatalf("work_units rows = %d, want exactly 1 after a replay", n)
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM idempotency_keys WHERE key = ANY($1)`, []string{"key-e2e-1", "key-e2e-nul"}); n != 1 {
		t.Fatalf("idempotency_keys rows = %d, want 1 (the rejected NUL consumed no key)", n)
	}

	code, released, _ := e2eDo(t, ctx, http.MethodPost, env.url("/paths/pick-e2e/work-releases"), "key-e2e-release", "")
	if code != http.StatusOK || !strings.Contains(released, `"state":"Released"`) {
		t.Fatalf("release = %d %s", code, released)
	}

	events := e2eEventsOn(t, ctx, env.brokers, cloudevents.TopicWorkPlanningEvents, "WorkUnitCreated|"+unit, "WorkReleased|"+unit)
	wr := events["WorkReleased|"+unit]
	if wr.SpecVersion() != "1.0" || wr.Type() != "com.warehouse.wes.work-planning.workunit.WorkReleased" ||
		wr.Source() != "/warehouse/wes-work-planning" || wr.DataSchema() != "urn:warehouse:wes-work-planning:events:WorkReleased:v1" || wr.Key != unit {
		t.Fatalf("WorkReleased envelope = type %q source %q schema %q key %q", wr.Type(), wr.Source(), wr.DataSchema(), wr.Key)
	}
	var data e2eReleased
	if err := json.Unmarshal(wr.Data(), &data); err != nil || data.WorkUnitId != unit || data.PathId != "pick-e2e" || data.Ref != "order-e2e-1" || data.Replenishment != nil {
		t.Fatalf("WorkReleased data = %s (%v)", wr.Data(), err)
	}
	analytics := e2eEventsOn(t, ctx, env.brokers, cloudevents.TopicAnalytics, "WorkReleased|"+unit)
	if analytics["WorkReleased|"+unit].SpecVersion() != "1.0" {
		t.Fatal("the analytics copy of WorkReleased is not a CloudEvent 1.0")
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); n != 0 {
		e2eWaitFor(t, ctx, 20*time.Second, "the outbox drains", func() bool {
			return e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
		})
	}
}

func e2eReplenishmentFlow(t *testing.T, ctx context.Context, env e2eEnv, pool *pgxpool.Pool) {
	t.Helper()
	// The exact bytes pick-face-replenishment publishes (the consumer's golden).
	golden, err := os.ReadFile(filepath.Join("..", "..", "internal", "adapters", "inbound", "kafka", "testdata", "pickface_ReplenishmentRequested.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	const requestID = "repl-3b0e8d2a-7c41-4f6a-9d15-2a8b6c4e1f07"
	unitID := workunit.ReplenishmentWorkUnitId(requestID)

	// A consumer group that joins late may still be assigning partitions:
	// re-publishing the same CloudEvents id is harmless (the consumer dedupes).
	e2eWaitFor(t, ctx, 90*time.Second, "the REPLENISH work unit is planned in Postgres", func() bool {
		e2eProduce(t, ctx, env, cloudevents.TopicPickFaceReplenishmentEvents, requestID, golden)
		time.Sleep(1500 * time.Millisecond)
		return e2eCount(t, ctx, pool, `SELECT count(*) FROM work_units WHERE id = $1`, unitID) == 1
	})
	var kind, state, from, to, priority string
	var qty int
	if err := pool.QueryRow(ctx, `SELECT work_kind, state, replenishment_from_bin, replenishment_to_bin, replenishment_priority, quantity FROM work_units WHERE id = $1`, unitID).
		Scan(&kind, &state, &from, &to, &priority, &qty); err != nil {
		t.Fatal(err)
	}
	if kind != "REPLENISH" || state != "Pending" || from != "WH1-RSV-A07-03-02-B" || to != "WH1-FWD-A01-01-01-A" || priority != "NORMAL" || qty != 24 {
		t.Fatalf("planned unit = %s %s %s -> %s %s x%d", kind, state, from, to, priority, qty)
	}

	// Replay of the same CloudEvent: still one unit, one processed mark.
	e2eProduce(t, ctx, env, cloudevents.TopicPickFaceReplenishmentEvents, requestID, golden)
	time.Sleep(3 * time.Second)
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM work_units WHERE id = $1`, unitID); n != 1 {
		t.Fatalf("replay planned %d units, want 1", n)
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM processed_events WHERE event_id = $1`, "6f1d2c3b-4a5e-4b7c-8d9e-0a1b2c3d4e5f"); n != 1 {
		t.Fatalf("processed_events rows = %d, want 1", n)
	}

	// REST shows it; releasing the REPLENISH path publishes the WorkReleased
	// fulfillment-execution turns into a REPLENISH task.
	if code, resp, _ := e2eDo(t, ctx, http.MethodGet, env.url("/work-units/"+unitID), "", ""); code != http.StatusOK || !strings.Contains(resp, requestID) {
		t.Fatalf("GET work unit = %d %s", code, resp)
	}
	if code, resp, _ := e2eDo(t, ctx, http.MethodPost, env.url("/paths/REPLENISH/work-releases"), "key-e2e-release-replenish", ""); code != http.StatusOK || !strings.Contains(resp, unitID) {
		t.Fatalf("release REPLENISH = %d %s", code, resp)
	}
	events := e2eEventsOn(t, ctx, env.brokers, cloudevents.TopicWorkPlanningEvents, "WorkUnitCreated|"+unitID, "WorkReleased|"+unitID)
	wr := events["WorkReleased|"+unitID]
	var data e2eReleased
	if err := json.Unmarshal(wr.Data(), &data); err != nil || data.PathId != "REPLENISH" || data.Ref != requestID || data.Replenishment == nil ||
		data.Replenishment.Quantity == nil || *data.Replenishment.Quantity != 24 || data.Replenishment.FromBin != "WH1-RSV-A07-03-02-B" ||
		data.Replenishment.ToBin != "WH1-FWD-A01-01-01-A" || data.Replenishment.SKU != "SKU-1" || data.Replenishment.Priority != "NORMAL" || wr.Key != unitID {
		t.Fatalf("WorkReleased data = %s (%v), key %q", wr.Data(), err, wr.Key)
	}
}

func e2eSlotPlanFlow(t *testing.T, ctx context.Context, env e2eEnv, pool *pgxpool.Pool) {
	t.Helper()
	golden, err := os.ReadFile(filepath.Join("..", "..", "internal", "adapters", "inbound", "kafka", "testdata", "slotting_SlotPlanApproved.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	const planID = "plan-3b0e8d2a-7c41-4f6a-9d15-2a8b6c4e1f07"
	e2eWaitFor(t, ctx, 90*time.Second, "the RELOCATE work units are planned in Postgres", func() bool {
		e2eProduce(t, ctx, env, cloudevents.TopicSlottingEvents, planID, golden)
		time.Sleep(1500 * time.Millisecond)
		return e2eCount(t, ctx, pool, `SELECT count(*) FROM work_units WHERE work_kind = 'RELOCATE' AND relocation_plan_id = $1`, planID) > 0
	})
	time.Sleep(2 * time.Second)
	n := e2eCount(t, ctx, pool, `SELECT count(*) FROM work_units WHERE work_kind = 'RELOCATE' AND relocation_plan_id = $1`, planID)
	if n != 3 {
		t.Fatalf("RELOCATE units for the approved plan = %d, want 3 (Assign, Relocate, Vacate), and a replay must not add more", n)
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM processed_events WHERE event_id = $1`, "0b9a8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d"); n != 1 {
		t.Fatalf("processed_events rows = %d, want 1", n)
	}
}
