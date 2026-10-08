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

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// fakeReader serves a fixed list of messages, then blocks until ctx ends.
// It records every commit together with the copy's state at commit time,
// so a test can prove the offset was committed only AFTER the effect.
type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafkago.Message
	next      int
	commits   []kafkago.Message
	onCommit  func(kafkago.Message)
	committed chan struct{}
}

func newFakeReader(msgs ...kafkago.Message) *fakeReader {
	return &fakeReader{msgs: msgs, committed: make(chan struct{}, 64)}
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if r.next < len(r.msgs) {
		m := r.msgs[r.next]
		r.next++
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	r.commits = append(r.commits, msgs...)
	hook := r.onCommit
	r.mu.Unlock()
	for _, m := range msgs {
		if hook != nil {
			hook(m)
		}
		r.committed <- struct{}{}
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

func (r *fakeReader) commitCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.commits)
}

// flakyCopies fails ApplyIfNewer `remaining` times (forever when < 0).
type flakyCopies struct {
	*productclassificationcopy.MemoryStore
	mu        sync.Mutex
	remaining int
	attempts  int
}

func (f *flakyCopies) ApplyIfNewer(ctx context.Context, view productclassificationview.ProductClassificationView, dot int, version int64, at time.Time) (bool, error) {
	f.mu.Lock()
	f.attempts++
	if f.remaining != 0 {
		if f.remaining > 0 {
			f.remaining--
		}
		f.mu.Unlock()
		return false, errors.New("transient database failure")
	}
	f.mu.Unlock()
	return f.MemoryStore.ApplyIfNewer(ctx, view, dot, version, at)
}

func (f *flakyCopies) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func productMasterEvent(t *testing.T, id, ceType string, data map[string]any) kafkago.Message {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetType(ceType)
	e.SetSource("/warehouse/product-master")
	e.SetSubject("SKU-1")
	e.SetTime(time.Now().UTC())
	if err := e.SetData(ce.ApplicationJSON, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	body, err := e.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafkago.Message{Topic: cloudevents.TopicProductMasterEvents, Key: []byte("SKU-1"), Value: body}
}

func classifiedEvent(t *testing.T, id string, version int64, tags ...string) kafkago.Message {
	t.Helper()
	return productMasterEvent(t, id, cloudevents.TypeProductClassified, map[string]any{
		"sku": "SKU-1", "handling_tags": tags, "temperature_class": "Frozen", "dot_hazard_class": 3,
		"classification_source": "native", "version": version,
	})
}

// runUntilCommitted runs the consumer until want commits happened (or fails
// the test after a timeout), then stops it and waits for Run to return.
func runUntilCommitted(t *testing.T, c *ProductClassificationConsumer, r *fakeReader, want int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.After(10 * time.Second)
	for got := 0; got < want; got++ {
		select {
		case <-r.committed:
		case <-deadline:
			cancel()
			t.Fatalf("only %d of %d messages committed", r.commitCount(), want)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func newTestClassificationConsumer(r messageReader, copies ports.ProductClassificationCopyRepo, logs *syncBuffer) *ProductClassificationConsumer {
	observe := usecases.NewObserveProductClassification(copies, memory.NewProcessedEventRepo())
	var logger *slog.Logger
	if logs != nil {
		logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	c := newProductClassificationConsumer(r, observe, logger)
	c.retryInitial = time.Millisecond
	c.retryMax = 5 * time.Millisecond
	return c
}

func TestProductClassificationConsumer_AppliesAndCommits(t *testing.T) {
	copies := productclassificationcopy.NewMemoryStore()
	r := newFakeReader(classifiedEvent(t, "evt-1", 1, "Hazmat", "TemperatureSensitive"))
	runUntilCommitted(t, newTestClassificationConsumer(r, copies, nil), r, 1)

	got, _ := copies.GetClassification(context.Background(), "SKU-1")
	if !got.Known || !got.HasTag("Hazmat") || got.TemperatureClass != "Frozen" || copies.Version("SKU-1") != 1 {
		t.Fatalf("copy = %+v (version %d)", got, copies.Version("SKU-1"))
	}
}

func TestProductClassificationConsumer_StaleVersionIgnored(t *testing.T) {
	copies := productclassificationcopy.NewMemoryStore()
	r := newFakeReader(
		classifiedEvent(t, "evt-2", 2, "Hazmat"),
		classifiedEvent(t, "evt-1", 1, "Fragile"), // older, arrives late
	)
	runUntilCommitted(t, newTestClassificationConsumer(r, copies, nil), r, 2)

	got, _ := copies.GetClassification(context.Background(), "SKU-1")
	if !got.HasTag("Hazmat") || got.HasTag("Fragile") || copies.Version("SKU-1") != 2 {
		t.Fatalf("stale version overwrote the copy: %+v (version %d)", got, copies.Version("SKU-1"))
	}
}

func TestProductClassificationConsumer_UnknownTypeIgnoredAndCommitted(t *testing.T) {
	copies := productclassificationcopy.NewMemoryStore()
	r := newFakeReader(
		productMasterEvent(t, "evt-reg", "com.warehouse.wms.product-master.product.ProductRegistered", map[string]any{"sku": "SKU-1", "description": "x", "version": 1}),
		// A suffix match must not count: dispatch is on the FULL type.
		productMasterEvent(t, "evt-legacy", "com.warehouse.wms.inventory-storage.product.ProductClassified", map[string]any{"sku": "SKU-1", "handling_tags": []string{"Hazmat"}, "version": 9}),
	)
	runUntilCommitted(t, newTestClassificationConsumer(r, copies, nil), r, 2)

	if got, _ := copies.GetClassification(context.Background(), "SKU-1"); got.Known {
		t.Fatalf("a non-ProductClassified type changed the copy: %+v", got)
	}
}

func TestProductClassificationConsumer_InvalidMessagesSkippedWithWarn(t *testing.T) {
	cases := map[string]kafkago.Message{
		"not a CloudEvent":       {Topic: cloudevents.TopicProductMasterEvents, Value: []byte(`{"event_type":"ProductClassified","sku":"SKU-1"}`)},
		"missing sku":            productMasterEvent(t, "evt-nosku", cloudevents.TypeProductClassified, map[string]any{"handling_tags": []string{"Hazmat"}, "version": 1}),
		"version zero":           productMasterEvent(t, "evt-v0", cloudevents.TypeProductClassified, map[string]any{"sku": "SKU-1", "handling_tags": []string{"Hazmat"}, "version": 0}),
		"DOT class out of range": productMasterEvent(t, "evt-dot", cloudevents.TypeProductClassified, map[string]any{"sku": "SKU-1", "handling_tags": []string{"Hazmat"}, "dot_hazard_class": 12, "version": 1}),
		"undecodable data":       productMasterEvent(t, "evt-bad", cloudevents.TypeProductClassified, map[string]any{"sku": 42, "version": "one"}),
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			copies := productclassificationcopy.NewMemoryStore()
			logs := &syncBuffer{}
			r := newFakeReader(msg)
			runUntilCommitted(t, newTestClassificationConsumer(r, copies, logs), r, 1)

			if got, _ := copies.GetClassification(context.Background(), "SKU-1"); got.Known {
				t.Fatalf("poison message changed the copy: %+v", got)
			}
			if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "skipping invalid") {
				t.Fatalf("expected a WARN skip log, got:\n%s", logs.String())
			}
		})
	}
}

func TestProductClassificationConsumer_TransientFailureRetriesSameMessageThenCommits(t *testing.T) {
	copies := &flakyCopies{MemoryStore: productclassificationcopy.NewMemoryStore(), remaining: 2}
	r := newFakeReader(classifiedEvent(t, "evt-1", 1, "Hazmat"))
	appliedAtCommit := false
	r.onCommit = func(kafkago.Message) {
		got, _ := copies.GetClassification(context.Background(), "SKU-1")
		appliedAtCommit = got.Known
	}
	logs := &syncBuffer{}
	runUntilCommitted(t, newTestClassificationConsumer(r, copies, logs), r, 1)

	if copies.attemptCount() != 3 {
		t.Fatalf("attempts = %d, want 3 (two failures, then success)", copies.attemptCount())
	}
	if r.commitCount() != 1 || !appliedAtCommit {
		t.Fatalf("commits = %d, applied before commit = %v; want exactly one commit, after the effect", r.commitCount(), appliedAtCommit)
	}
	if strings.Count(logs.String(), "retrying the same message") != 2 {
		t.Fatalf("expected two ERROR retry logs, got:\n%s", logs.String())
	}
}

func TestProductClassificationConsumer_StoppedMidRetryDoesNotCommit(t *testing.T) {
	copies := &flakyCopies{MemoryStore: productclassificationcopy.NewMemoryStore(), remaining: -1}
	r := newFakeReader(classifiedEvent(t, "evt-1", 1, "Hazmat"))
	c := newTestClassificationConsumer(r, copies, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for copies.attemptCount() < 3 {
		if time.Now().After(deadline) {
			t.Fatal("consumer never retried")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run after stop = %v, want nil", err)
	}
	if r.commitCount() != 0 {
		t.Fatalf("a message that never applied was committed (%d commits)", r.commitCount())
	}
}

// failingFetchReader fails FetchMessage with a non-cancellation error.
type failingFetchReader struct{ fakeReader }

func (*failingFetchReader) FetchMessage(context.Context) (kafkago.Message, error) {
	return kafkago.Message{}, errors.New("broker gone")
}

func TestProductClassificationConsumer_FetchErrorStopsRun(t *testing.T) {
	observe := usecases.NewObserveProductClassification(productclassificationcopy.NewMemoryStore(), memory.NewProcessedEventRepo())
	c := newProductClassificationConsumer(&failingFetchReader{}, observe, nil)
	if err := c.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "broker gone") {
		t.Fatalf("Run = %v, want the fetch error", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
