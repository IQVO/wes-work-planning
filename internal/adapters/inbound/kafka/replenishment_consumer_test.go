package kafka

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0042: the replenishment stream decodes pick-face-replenishment's
// ReplenishmentRequested / ReplenishmentCancelled EXACTLY as its
// apis/asyncapi.yaml publishes them (testdata/pickface_*.golden.json are its
// golden files, byte for byte), plans one REPLENISH unit per request id,
// ignores every other type, skips what is not a CloudEvent with a WARN,
// dead-letters poison after the retries and never dead-letters a message only
// shutdown cancellation interrupted.

const (
	goldenRequestId = "repl-3b0e8d2a-7c41-4f6a-9d15-2a8b6c4e1f07"
	goldenUnitId    = "replenish-" + goldenRequestId
	replTestTopic   = cloudevents.TopicPickFaceReplenishmentEvents
)

type replConsumerFixture struct {
	consumer  *ReplenishmentConsumer
	reader    *fakeReader
	dlq       *fakeDLQ
	workUnits *memory.WorkUnitRepo
	flaky     *flakyFindWorkUnits
	logs      *bytes.Buffer
}

func newReplConsumerFixture(t *testing.T, failures int, msgs ...kafkago.Message) replConsumerFixture {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	flaky := &flakyFindWorkUnits{WorkUnitRepo: workUnits, failures: failures}
	pools := memory.NewWorkPoolRepo()
	processed := memory.NewProcessedEventRepo()
	clock := memory.FixedClock{At: time.Date(2026, 10, 17, 9, 31, 0, 0, time.UTC)}
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "REPLENISH", MatchPrefix: "replenish", RequiredCapabilities: []string{"replenish"}}})
	requested := usecases.NewApplyReplenishmentRequested(flaky, pools, processed, catalogue, events.NewLogPublisher(nil), clock)
	cancelled := usecases.NewApplyReplenishmentCancelled(flaky, pools, processed, clock)
	reader := newFakeReader(msgs...)
	dlq := &fakeDLQ{}
	logs := &bytes.Buffer{}
	return replConsumerFixture{
		consumer:  newReplenishmentConsumer(reader, dlq, replTestTopic, requested, cancelled, slog.New(slog.NewTextHandler(logs, nil))),
		reader:    reader,
		dlq:       dlq,
		workUnits: workUnits,
		flaky:     flaky,
		logs:      logs,
	}
}

func goldenPickFace(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "pickface_"+name+".golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	return raw
}

func replMsg(value []byte) kafkago.Message {
	return kafkago.Message{Topic: replTestTopic, Key: []byte(goldenRequestId), Value: value}
}

// run consumes every queued message, then stops the consumer.
func (f replConsumerFixture) run(t *testing.T, queued int) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.consumer.Run(ctx) }()
	for i := 0; i < queued; i++ {
		select {
		case <-f.reader.committed:
		case err := <-done:
			return err
		case <-ctx.Done():
			t.Fatalf("only %d of %d messages were committed in time", i, queued)
		}
	}
	cancel()
	return <-done
}

func (f replConsumerFixture) mustUnit(t *testing.T, id string) *workunit.WorkUnit {
	t.Helper()
	u, err := f.workUnits.FindById(context.Background(), id)
	if err != nil {
		t.Fatalf("work unit %s: %v", id, err)
	}
	return u
}

func TestReplenishmentConsumer_GoldenRequestedPlansTheReplenishUnit(t *testing.T) {
	f := newReplConsumerFixture(t, 0, replMsg(goldenPickFace(t, "ReplenishmentRequested")))
	if err := f.run(t, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	u := f.mustUnit(t, goldenUnitId)
	m, ok := u.Replenishment()
	want := workunit.ReplenishmentMove{RequestId: goldenRequestId, SKU: "SKU-1", FromBin: "WH1-RSV-A07-03-02-B", ToBin: "WH1-FWD-A01-01-01-A", Quantity: 24, Priority: workunit.ReplenishmentPriorityNormal}
	if !ok || m != want {
		t.Fatalf("replenishment = (%+v, %v), want %+v (source_bin is the from side, forward_bin the pick face)", m, ok, want)
	}
	if u.SiteId() != "WH1" || u.PathId().String() != "REPLENISH" || u.Reference() != goldenRequestId || u.WorkKind() != workunit.WorkKindReplenish {
		t.Fatalf("unit = site %s path %s ref %s kind %s", u.SiteId(), u.PathId(), u.Reference(), u.WorkKind())
	}
	if !u.CPT().Time().Equal(time.Date(2026, 10, 17, 17, 30, 0, 0, time.UTC)) {
		t.Fatalf("cpt = %v, want requested_at + 8h", u.CPT().Time())
	}
	if f.dlq.count() != 0 || f.reader.commitCount() != 1 {
		t.Fatalf("dlq=%d commits=%d, want 0 and 1", f.dlq.count(), f.reader.commitCount())
	}
}

func TestReplenishmentConsumer_UrgentPriorityGetsTheFrontBandCPT(t *testing.T) {
	urgent := strings.Replace(string(goldenPickFace(t, "ReplenishmentRequested")), `"priority": "NORMAL"`, `"priority": "URGENT"`, 1)
	f := newReplConsumerFixture(t, 0, replMsg([]byte(urgent)))
	if err := f.run(t, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	u := f.mustUnit(t, goldenUnitId)
	if m, _ := u.Replenishment(); m.Priority != workunit.ReplenishmentPriorityUrgent {
		t.Fatalf("priority = %s", m.Priority)
	}
	if !u.CPT().Time().Equal(time.Date(2026, 10, 17, 10, 30, 0, 0, time.UTC)) {
		t.Fatalf("cpt = %v, want requested_at + 1h", u.CPT().Time())
	}
}

func TestReplenishmentConsumer_CancelledWithdrawsAPendingUnit(t *testing.T) {
	f := newReplConsumerFixture(t, 0,
		replMsg(goldenPickFace(t, "ReplenishmentRequested")), replMsg(goldenPickFace(t, "ReplenishmentCancelled")))
	if err := f.run(t, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if u := f.mustUnit(t, goldenUnitId); u.State() != workunit.Cancelled {
		t.Fatalf("state = %v, want Cancelled", u.State())
	}
}

// A redelivery of the same CloudEvents id, and a DIFFERENT event for the
// same request id, both leave exactly one unit.
func TestReplenishmentConsumer_DuplicateIdAndSameRequestPlanOneUnit(t *testing.T) {
	raw := goldenPickFace(t, "ReplenishmentRequested")
	otherEvent := strings.Replace(string(raw), `"id": "6f1d2c3b-4a5e-4b7c-8d9e-0a1b2c3d4e5f"`, `"id": "99999999-4a5e-4b7c-8d9e-0a1b2c3d4e5f"`, 1)
	f := newReplConsumerFixture(t, 0, replMsg(raw), replMsg(raw), replMsg([]byte(otherEvent)))
	if err := f.run(t, 3); err != nil {
		t.Fatalf("Run: %v", err)
	}
	units, err := f.workUnits.FindByReference(context.Background(), goldenRequestId)
	if err != nil || len(units) != 1 {
		t.Fatalf("units for the request = %d (%v), want exactly one", len(units), err)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("a replay was dead-lettered (%d)", f.dlq.count())
	}
}

func TestReplenishmentConsumer_CompletedAndUnknownTypesAreIgnored(t *testing.T) {
	unknown := strings.Replace(string(goldenPickFace(t, "ReplenishmentCompleted")), "ReplenishmentCompleted", "SomethingNew", 2)
	f := newReplConsumerFixture(t, 0, replMsg(goldenPickFace(t, "ReplenishmentCompleted")), replMsg([]byte(unknown)))
	if err := f.run(t, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if units, _ := f.workUnits.FindByReference(context.Background(), goldenRequestId); len(units) != 0 {
		t.Fatalf("a non-request event planned work: %d units", len(units))
	}
	if f.dlq.count() != 0 || f.reader.commitCount() != 2 {
		t.Fatalf("dlq=%d commits=%d, want both committed past without a DLQ", f.dlq.count(), f.reader.commitCount())
	}
}

// What is not a CloudEvents 1.0 event (the retired flat envelope, bad JSON)
// is WARN-logged and committed past: never parsed, never retried, never
// planned.
func TestReplenishmentConsumer_LegacyFlatAndGarbageAreWarnSkipped(t *testing.T) {
	flat := []byte(`{"event_id":"legacy-1","event_type":"ReplenishmentRequested","occurred_at":"2026-10-17T09:30:00Z","source":"pick-face-replenishment","data":{"request_id":"legacy-req","site_id":"WH1","sku":"SKU-1","forward_bin":"F","source_bin":"R","quantity":3,"priority":"NORMAL"}}`)
	f := newReplConsumerFixture(t, 0, replMsg(flat), replMsg([]byte(`not json at all`)))
	if err := f.run(t, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if units, _ := f.workUnits.FindByReference(context.Background(), "legacy-req"); len(units) != 0 {
		t.Fatalf("the flat message was parsed as a legacy shape: %d units", len(units))
	}
	if f.dlq.count() != 0 || f.reader.commitCount() != 2 {
		t.Fatalf("dlq=%d commits=%d, want skipped (committed, not dead-lettered)", f.dlq.count(), f.reader.commitCount())
	}
	if strings.Count(f.logs.String(), "level=WARN") != 2 || !strings.Contains(f.logs.String(), "not a valid CloudEvent") {
		t.Fatalf("expected two WARN lines, got:\n%s", f.logs.String())
	}
}

// A request that can never be applied is dead-lettered at once, raw and
// byte-identical, with its error context, and the partition moves on.
func TestReplenishmentConsumer_PoisonRequestGoesStraightToTheDeadLetterTopic(t *testing.T) {
	poison := strings.Replace(string(goldenPickFace(t, "ReplenishmentRequested")), `"quantity": 24`, `"quantity": 0`, 1)
	f := newReplConsumerFixture(t, 0, replMsg([]byte(poison)))
	if err := f.run(t, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if f.dlq.count() != 1 {
		t.Fatalf("DLQ messages = %d, want 1", f.dlq.count())
	}
	dead := f.dlq.msgs[0]
	if string(dead.Value) != poison || string(dead.Key) != goldenRequestId {
		t.Fatalf("the DLQ message is not the raw original: %q / %q", dead.Key, dead.Value)
	}
	if got := headerValue(dead.Headers, "x-dlq-source-topic"); got != replTestTopic {
		t.Fatalf("x-dlq-source-topic = %q", got)
	}
	if got := headerValue(dead.Headers, "x-dlq-error"); !strings.Contains(got, "quantity must be at least 1") {
		t.Fatalf("x-dlq-error = %q", got)
	}
	if f.flaky.calls != 0 {
		t.Fatalf("a refused request reached the repository (%d reads)", f.flaky.calls)
	}
	if f.reader.commitCount() != 1 {
		t.Fatalf("commits = %d, want the poison committed past", f.reader.commitCount())
	}
}

func TestReplenishmentConsumer_UndecodablePayloadIsPoison(t *testing.T) {
	bad := strings.Replace(string(goldenPickFace(t, "ReplenishmentRequested")), `"quantity": 24`, `"quantity": "many"`, 1)
	f := newReplConsumerFixture(t, 0, replMsg([]byte(bad)))
	if err := f.run(t, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if f.dlq.count() != 1 || !strings.Contains(headerValue(f.dlq.msgs[0].Headers, "x-dlq-error"), "malformed replenishment event payload") {
		t.Fatalf("dlq=%d, want the undecodable payload dead-lettered", f.dlq.count())
	}
}

// A payload that can never be applied is permanent (one attempt); any other
// error is retried up to maxHandlerAttempts.
func TestReplenishmentConsumer_OnlyNonPoisonErrorsAreRetried(t *testing.T) {
	f := newReplConsumerFixture(t, 0)
	env, err := cloudevents.Decode(goldenPickFace(t, "ReplenishmentRequested"))
	if err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	cases := []struct {
		name  string
		err   error
		tries int
	}{
		{"bad payload", errBadReplenishmentPayload, 1},
		{"invalid move", workunit.ErrInvalidReplenishmentMove, 1},
		{"unknown priority", workunit.ErrUnknownReplenishmentPriority, 1},
		{"invalid request", usecases.ErrInvalidReplenishmentRequest, 1},
		{"transient", errBlip, maxHandlerAttempts},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tries := 0
			err := f.consumer.applyWithRetry(context.Background(), env, func(context.Context, ce.Event) error {
				tries++
				return tc.err
			})
			if !errors.Is(err, tc.err) || tries != tc.tries {
				t.Fatalf("err=%v tries=%d, want %v after %d attempt(s)", err, tries, tc.err, tc.tries)
			}
		})
	}
}

func TestReplenishmentConsumer_TransientFailureIsRetriedAndApplied(t *testing.T) {
	f := newReplConsumerFixture(t, 1, replMsg(goldenPickFace(t, "ReplenishmentRequested")))
	if err := f.run(t, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	f.mustUnit(t, goldenUnitId)
	if f.dlq.count() != 0 {
		t.Fatalf("a healed transient failure reached the DLQ (%d)", f.dlq.count())
	}
}

func TestReplenishmentConsumer_PersistentFailureIsDeadLetteredAfterTheRetries(t *testing.T) {
	f := newReplConsumerFixture(t, -1, replMsg(goldenPickFace(t, "ReplenishmentRequested")))
	if err := f.run(t, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if f.dlq.count() != 1 || f.reader.commitCount() != 1 {
		t.Fatalf("dlq=%d commits=%d, want 1 and 1", f.dlq.count(), f.reader.commitCount())
	}
	if f.flaky.calls != maxHandlerAttempts {
		t.Fatalf("attempts = %d, want %d", f.flaky.calls, maxHandlerAttempts)
	}
}

// An unknown REPLENISH path is not poison but a catalogue that has not
// caught up: retried, then dead-lettered for a replay.
func TestReplenishmentConsumer_UnknownPathIsDeadLetteredForReplay(t *testing.T) {
	f := newReplConsumerFixture(t, 0, replMsg(goldenPickFace(t, "ReplenishmentRequested")))
	noPath := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "PICK", MatchPrefix: "pick"}})
	f.consumer.requested = usecases.NewApplyReplenishmentRequested(f.workUnits, memory.NewWorkPoolRepo(), memory.NewProcessedEventRepo(), noPath, events.NewLogPublisher(nil), memory.SystemClock{})
	if err := f.run(t, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if f.dlq.count() != 1 || !strings.Contains(headerValue(f.dlq.msgs[0].Headers, "x-dlq-error"), pathcatalog.ErrUnknownPath.Error()) {
		t.Fatalf("dlq=%d, want the unknown-path request dead-lettered", f.dlq.count())
	}
}

// Shutdown mid-handle: nothing is dead-lettered and nothing is committed, so
// the message is redelivered.
func TestReplenishmentConsumer_CancellationIsNeverDeadLettered(t *testing.T) {
	f := newReplConsumerFixture(t, 0, replMsg(goldenPickFace(t, "ReplenishmentRequested")))
	f.consumer.requested = usecases.NewApplyReplenishmentRequested(f.workUnits, memory.NewWorkPoolRepo(), cancelAwareProcessed{memory.NewProcessedEventRepo()},
		pathcatalog.New([]pathcatalog.PathDefinition{{Id: "REPLENISH", MatchPrefix: "replenish"}}), events.NewLogPublisher(nil), memory.SystemClock{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.consumer.settle(ctx, replMsg(goldenPickFace(t, "ReplenishmentRequested"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("settle on a cancelled context = %v, want the cancellation (no commit)", err)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("a message interrupted by cancellation was dead-lettered (%d)", f.dlq.count())
	}
	if err := f.consumer.Run(ctx); err != nil {
		t.Fatalf("Run on a cancelled context = %v, want a clean stop", err)
	}
	if f.reader.commitCount() != 0 {
		t.Fatalf("commits = %d, want none", f.reader.commitCount())
	}
}

// A dead-letter publish that fails stops the loop WITHOUT committing: the
// message is never lost.
func TestReplenishmentConsumer_DeadLetterFailureStopsWithoutCommitting(t *testing.T) {
	poison := strings.Replace(string(goldenPickFace(t, "ReplenishmentRequested")), `"quantity": 24`, `"quantity": 0`, 1)
	f := newReplConsumerFixture(t, 0, replMsg([]byte(poison)))
	f.consumer.dlq = failingDLQ{}
	err := f.consumer.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "dead-letter") {
		t.Fatalf("Run = %v, want the dead-letter failure", err)
	}
	if f.reader.commitCount() != 0 {
		t.Fatalf("commits = %d, want none: the message must not be lost", f.reader.commitCount())
	}
}

// The offset is committed only AFTER the effect.
func TestReplenishmentConsumer_CommitsOnlyAfterTheUnitIsPlanned(t *testing.T) {
	f := newReplConsumerFixture(t, 0, replMsg(goldenPickFace(t, "ReplenishmentRequested")))
	var plannedAtCommit bool
	f.reader.onCommit = func(kafkago.Message) {
		_, err := f.workUnits.FindById(context.Background(), goldenUnitId)
		plannedAtCommit = !errors.Is(err, ports.ErrNotFound)
	}
	if err := f.run(t, 1); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !plannedAtCommit {
		t.Fatal("the offset was committed before the unit existed")
	}
}

func TestReplenishmentConsumer_ConstructorWiresTheTopicAndGroup(t *testing.T) {
	c := NewReplenishmentConsumerForTopic([]string{"localhost:1"}, "g-repl", "t.repl", nil, nil, nil)
	defer func() { _ = c.Close() }()
	r, ok := c.reader.(*kafkago.Reader)
	if !ok || r.Config().Topic != "t.repl" || r.Config().GroupID != "g-repl" {
		t.Fatalf("reader = %#v", c.reader)
	}
	if c.topic != "t.repl" {
		t.Fatalf("topic = %q", c.topic)
	}
	prod := NewReplenishmentConsumer([]string{"localhost:1"}, "g", nil, nil, nil)
	defer func() { _ = prod.Close() }()
	if prod.topic != cloudevents.TopicPickFaceReplenishmentEvents {
		t.Fatalf("production topic = %q", prod.topic)
	}
}
