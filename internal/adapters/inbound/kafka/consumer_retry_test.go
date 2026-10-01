package kafka

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// These tests drive the consumer's real retry-then-dead-letter path
// (dispatch -> handleWithRetry -> dlqPublish) with a fake DLQ writer, to
// pin ADR-0028: a failure on attempt 1 must be retried and actually
// applied, a persistent failure must reach the DLQ, and a TaskCompleted
// for a work unit this context never planned is a logged skip, not a DLQ.

var errBlip = errors.New("transient postgres blip")

// fakeDLQ records every dead-lettered message.
type fakeDLQ struct {
	mu   sync.Mutex
	msgs []kafkago.Message
}

func (d *fakeDLQ) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.msgs = append(d.msgs, msgs...)
	return nil
}

func (d *fakeDLQ) Close() error { return nil }

func (d *fakeDLQ) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.msgs)
}

// failingDLQ cannot publish.
type failingDLQ struct{}

func (failingDLQ) WriteMessages(context.Context, ...kafkago.Message) error {
	return errors.New("dlq down")
}
func (failingDLQ) Close() error { return nil }

// flakyFindWorkUnits fails the first `failures` FindById calls (-1: always).
type flakyFindWorkUnits struct {
	ports.WorkUnitRepo
	mu       sync.Mutex
	failures int
	calls    int
}

func (r *flakyFindWorkUnits) FindById(ctx context.Context, id string) (*workunit.WorkUnit, error) {
	r.mu.Lock()
	r.calls++
	if r.failures != 0 {
		if r.failures > 0 {
			r.failures--
		}
		r.mu.Unlock()
		return nil, errBlip
	}
	r.mu.Unlock()
	return r.WorkUnitRepo.FindById(ctx, id)
}

// flakyFindPools fails the first `failures` FindByPathId calls (-1: always).
type flakyFindPools struct {
	ports.WorkPoolRepo
	mu       sync.Mutex
	failures int
}

func (r *flakyFindPools) FindByPathId(ctx context.Context, pathId shared.PathId) (*release.WorkPool, error) {
	r.mu.Lock()
	if r.failures != 0 {
		if r.failures > 0 {
			r.failures--
		}
		r.mu.Unlock()
		return nil, errBlip
	}
	r.mu.Unlock()
	return r.WorkPoolRepo.FindByPathId(ctx, pathId)
}

type retryFixture struct {
	consumer  *Consumer
	dlq       *fakeDLQ
	workUnits *memory.WorkUnitRepo
	flakyWU   *flakyFindWorkUnits
	logs      *bytes.Buffer
}

// newRetryFixture wires the real use cases over in-memory repos, with a
// released work unit "wu-1" and transient failures injected on the
// TaskCompleted (wuFailures) and OrderAllocated (poolFailures) paths.
func newRetryFixture(t *testing.T, wuFailures, poolFailures int) retryFixture {
	t.Helper()
	ctx := context.Background()
	workUnits := memory.NewWorkUnitRepo()
	pools := memory.NewWorkPoolRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)}
	processed := memory.NewProcessedEventRepo()

	pathId := mustPathId(t, "pick-a")
	if _, err := usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock).Execute(ctx, usecases.EnqueueWorkUnitRequest{
		WorkUnitId: "wu-1", PathId: pathId, CPT: shared.NewCPT(clock.At.Add(2 * time.Hour)), Reference: "order-1",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := usecases.NewReleaseNextWork(pools, workUnits, publisher, clock).Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: pathId}); err != nil {
		t.Fatalf("release: %v", err)
	}

	flakyWU := &flakyFindWorkUnits{WorkUnitRepo: workUnits, failures: wuFailures}
	flakyPools := &flakyFindPools{WorkPoolRepo: pools, failures: poolFailures}
	record := usecases.NewRecordCompletion(flakyWU, pools, publisher, clock)
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, flakyPools, publisher, clock)

	dlq := &fakeDLQ{}
	logs := &bytes.Buffer{}
	return retryFixture{
		consumer: &Consumer{
			dlqWriters: map[string]dlqWriter{
				cloudevents.TopicFulfillmentEvents:     dlq,
				cloudevents.TopicOrderManagementEvents: dlq,
			},
			applyTaskCompleted:  usecases.NewApplyTaskCompleted(record, processed),
			applyOrderAllocated: usecases.NewApplyOrderAllocated(enqueue, processed, testCatalogue()),
			catalogue:           testCatalogue(),
			logger:              slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		},
		dlq:       dlq,
		workUnits: workUnits,
		flakyWU:   flakyWU,
		logs:      logs,
	}
}

func wireMessage(t *testing.T, env ce.Event) kafkago.Message {
	t.Helper()
	body, err := env.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafkago.Message{Key: []byte(env.ID()), Value: body}
}

func (f retryFixture) dispatch(t *testing.T, topic string, msg kafkago.Message, handle func(context.Context, ce.Event) error) error {
	t.Helper()
	ctx := context.Background()
	return f.consumer.dispatch(ctx, ctx, trace.SpanFromContext(ctx), topic, msg, handle)
}

func taskCompletedWithType(t *testing.T, eventId, workUnitId, taskType string) ce.Event {
	t.Helper()
	return testEvent(t, eventId, cloudevents.TypeTaskCompleted, "/warehouse/fulfillment-execution", "task-9",
		taskCompletedData{TaskId: "task-9", StationId: "pack-3", WorkUnitId: workUnitId, TaskType: taskType})
}

// (a) The exact bug sequence: attempt 1 fails after (formerly) committing
// the mark, attempt 2 used to see the mark and return success. Now the
// retry really completes the unit, frees nothing to the DLQ.
func TestDispatch_TaskCompleted_TransientFailureIsRetriedAndApplied(t *testing.T) {
	f := newRetryFixture(t, 1, 0)
	msg := wireMessage(t, taskCompletedWithType(t, "evt-tc-1", "wu-1", "PICK"))

	if err := f.dispatch(t, cloudevents.TopicFulfillmentEvents, msg, f.consumer.handleFulfillmentEvent); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	unit, err := f.workUnits.FindById(context.Background(), "wu-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if unit.State() != workunit.Completed {
		t.Fatalf("work unit state = %v, want Completed (the retry was swallowed)", unit.State())
	}
	if f.dlq.count() != 0 {
		t.Fatalf("a healed transient failure must not be dead-lettered, got %d DLQ messages", f.dlq.count())
	}
	if f.flakyWU.calls != 2 {
		t.Fatalf("RecordCompletion ran %d times, want 2 (1 failure + 1 real retry)", f.flakyWU.calls)
	}
}

// (b) A failure that never heals exhausts every attempt and is
// dead-lettered with the raw payload and error context — never committed
// as a silent success.
func TestDispatch_TaskCompleted_PersistentFailureIsDeadLettered(t *testing.T) {
	f := newRetryFixture(t, -1, 0)
	msg := wireMessage(t, taskCompletedWithType(t, "evt-tc-2", "wu-1", "PICK"))

	if err := f.dispatch(t, cloudevents.TopicFulfillmentEvents, msg, f.consumer.handleFulfillmentEvent); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if f.flakyWU.calls != maxHandlerAttempts {
		t.Fatalf("attempts = %d, want %d (every attempt must re-run the use case, none may short-circuit on a stale mark)", f.flakyWU.calls, maxHandlerAttempts)
	}
	if f.dlq.count() != 1 {
		t.Fatalf("DLQ messages = %d, want 1", f.dlq.count())
	}
	dl := f.dlq.msgs[0]
	if !bytes.Equal(dl.Value, msg.Value) {
		t.Fatal("DLQ payload must be the raw original message")
	}
	if got := headerValue(dl.Headers, "x-dlq-source-topic"); got != cloudevents.TopicFulfillmentEvents {
		t.Fatalf("x-dlq-source-topic = %q", got)
	}
	if got := headerValue(dl.Headers, "x-dlq-error"); !strings.Contains(got, errBlip.Error()) {
		t.Fatalf("x-dlq-error = %q, want it to carry the cause", got)
	}
	unit, _ := f.workUnits.FindById(context.Background(), "wu-1")
	if unit.State() != workunit.Released {
		t.Fatalf("work unit state = %v, want still Released", unit.State())
	}
}

// (c) A completion for a work unit this context never planned (a PACK
// task fulfillment-execution created during rebin consolidation carries
// the ORDER id) is a deliberate INFO skip: nil, no retry, no DLQ, and
// marked processed so a redelivery is cheap.
func TestDispatch_TaskCompleted_UnknownWorkUnitIsSkippedNotDeadLettered(t *testing.T) {
	f := newRetryFixture(t, 0, 0)
	msg := wireMessage(t, taskCompletedWithType(t, "evt-pack-1", "order-77", "PACK"))

	if err := f.dispatch(t, cloudevents.TopicFulfillmentEvents, msg, f.consumer.handleFulfillmentEvent); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("an unknown work unit must never be dead-lettered, got %d", f.dlq.count())
	}
	if f.flakyWU.calls != 1 {
		t.Fatalf("RecordCompletion ran %d times, want exactly 1 (a skip is not retried)", f.flakyWU.calls)
	}
	logs := f.logs.String()
	for _, want := range []string{"level=INFO", "never planned", "event_id=evt-pack-1", "work_unit_id=order-77", "task_type=PACK"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("skip log missing %q:\n%s", want, logs)
		}
	}

	f.logs.Reset()
	if err := f.dispatch(t, cloudevents.TopicFulfillmentEvents, msg, f.consumer.handleFulfillmentEvent); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if f.flakyWU.calls != 1 {
		t.Fatalf("a redelivered skip re-ran RecordCompletion (calls=%d); it must have been marked processed", f.flakyWU.calls)
	}
	if strings.Contains(f.logs.String(), "never planned") {
		t.Fatal("a redelivered skip must short-circuit on the processed mark, not log the skip again")
	}
}

func orderAllocatedMessage(t *testing.T, eventId string) kafkago.Message {
	t.Helper()
	return wireMessage(t, orderAllocatedEnvelope(t, eventId, cloudevents.TypeOrderAllocated, "order-5",
		[]orderLineData{{LineNo: 1, SKU: "SKU-1", PathId: "pick-a"}, {LineNo: 2, SKU: "SKU-2", PathId: "pick-a", GiftWrap: true}}))
}

// (d) Same contract for OrderAllocated: a transient failure is retried
// and every line is really enqueued; nothing is dead-lettered.
func TestDispatch_OrderAllocated_TransientFailureIsRetriedAndApplied(t *testing.T) {
	f := newRetryFixture(t, 0, 1)
	if err := f.dispatch(t, cloudevents.TopicOrderManagementEvents, orderAllocatedMessage(t, "evt-oa-1"), f.consumer.handleOrderManagementEvent); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	for _, id := range []string{"order-5-line-1", "order-5-line-2"} {
		if _, err := f.workUnits.FindById(context.Background(), id); err != nil {
			t.Fatalf("%s never enqueued (the retry was swallowed): %v", id, err)
		}
	}
	if f.dlq.count() != 0 {
		t.Fatalf("DLQ messages = %d, want 0", f.dlq.count())
	}
}

func TestDispatch_OrderAllocated_PersistentFailureIsDeadLettered(t *testing.T) {
	f := newRetryFixture(t, 0, -1)
	if err := f.dispatch(t, cloudevents.TopicOrderManagementEvents, orderAllocatedMessage(t, "evt-oa-2"), f.consumer.handleOrderManagementEvent); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if f.dlq.count() != 1 {
		t.Fatalf("DLQ messages = %d, want 1", f.dlq.count())
	}
	if _, err := f.workUnits.FindById(context.Background(), "order-5-line-1"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("nothing may be enqueued for a dead-lettered event, got %v", err)
	}
}

func TestDispatch_InvalidCloudEventIsDeadLetteredWithoutRetry(t *testing.T) {
	f := newRetryFixture(t, 0, 0)
	calls := 0
	handle := func(context.Context, ce.Event) error { calls++; return nil }
	if err := f.dispatch(t, cloudevents.TopicFulfillmentEvents, kafkago.Message{Value: []byte(`{"event_id":"legacy"}`)}, handle); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if calls != 0 || f.dlq.count() != 1 {
		t.Fatalf("calls=%d dlq=%d, want 0 and 1", calls, f.dlq.count())
	}
}

func TestDispatch_DeadLetterPublishFailureIsReturned(t *testing.T) {
	f := newRetryFixture(t, 0, 0)
	f.consumer.dlqWriters[cloudevents.TopicFulfillmentEvents] = failingDLQ{}
	failing := func(context.Context, ce.Event) error { return errBlip }

	msg := wireMessage(t, taskCompletedWithType(t, "evt-x", "wu-1", "PICK"))
	if err := f.dispatch(t, cloudevents.TopicFulfillmentEvents, msg, failing); err == nil {
		t.Fatal("a failed DLQ publish after exhausted retries must abort (the offset must not be committed)")
	}
	if err := f.dispatch(t, cloudevents.TopicFulfillmentEvents, kafkago.Message{Value: []byte("not json")}, failing); err == nil {
		t.Fatal("a failed DLQ publish for an invalid CloudEvent must abort")
	}
}

func TestDispatch_TaskCompleted_BadPayloadIsDeadLettered(t *testing.T) {
	f := newRetryFixture(t, 0, 0)
	env := testEvent(t, "evt-bad", cloudevents.TypeTaskCompleted, "/warehouse/fulfillment-execution", "task-1", map[string]any{"work_unit_id": 42})
	if err := f.dispatch(t, cloudevents.TopicFulfillmentEvents, wireMessage(t, env), f.consumer.handleFulfillmentEvent); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if f.dlq.count() != 1 {
		t.Fatalf("DLQ messages = %d, want 1", f.dlq.count())
	}
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
