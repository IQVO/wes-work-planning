package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// fakeWriter is a stub outboundkafka.Writer so these tests never hit a
// real broker; it captures every message written for inspection.
type fakeWriter struct {
	msgs   []kafkago.Message
	err    error
	closed bool
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}

func (f *fakeWriter) Close() error {
	f.closed = true
	return nil
}

// fakeClassificationLookup is a stub ports.ProductClassificationLookup.
type fakeClassificationLookup struct {
	views map[string]productclassificationview.ProductClassificationView
	err   error
}

func (f *fakeClassificationLookup) GetClassification(_ context.Context, sku string) (productclassificationview.ProductClassificationView, error) {
	if f.err != nil {
		return productclassificationview.ProductClassificationView{}, f.err
	}
	return f.views[sku], nil
}

func newReleasedWorkUnit(t *testing.T, id, sku string) *memory.WorkUnitRepo {
	t.Helper()
	return newReleasedWorkUnitWithGiftWrap(t, id, sku, false)
}

// newReleasedWorkUnitWithGiftWrap is newReleasedWorkUnit plus an explicit
// gift-wrap request, for tests that exercise the gift_wrap enrichment
// (ADR-0010) independently of the SKU-derived classification hints
// (ADR-0009).
func newReleasedWorkUnitWithGiftWrap(t *testing.T, id, sku string, giftWrap bool) *memory.WorkUnitRepo {
	t.Helper()
	workUnits := memory.NewWorkUnitRepo()
	pathId, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cpt := shared.NewCPT(time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC))
	unit, err := workunit.NewWorkUnit(id, pathId, cpt, "ref-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	unit.SetSKU(sku)
	unit.SetGiftWrap(giftWrap)
	if err := unit.Release(time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := workUnits.Save(context.Background(), unit); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return workUnits
}

func decodeWorkReleasedData(t *testing.T, msg kafkago.Message) map[string]any {
	t.Helper()
	evt, err := cloudevents.Decode(msg.Value)
	if err != nil {
		t.Fatalf("decode CloudEvent: %v", err)
	}
	var data map[string]any
	if err := evt.DataAs(&data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	return data
}

func TestPublisher_WorkReleased_HazmatSKU_AppendsRequiredCapability(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-1", "sku-hazmat")
	lookup := &fakeClassificationLookup{views: map[string]productclassificationview.ProductClassificationView{
		"sku-hazmat": {SKU: "sku-hazmat", HandlingTags: []string{"Hazmat"}, Known: true},
	}}
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, lookup, func() string { return "evt-1" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-1", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(writer.msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(writer.msgs))
	}
	data := decodeWorkReleasedData(t, writer.msgs[0])

	caps, ok := data["required_capabilities"].([]any)
	if !ok {
		t.Fatalf("expected required_capabilities in payload, got %v", data)
	}
	if len(caps) != 1 || caps[0] != "hazmat" {
		t.Fatalf("got required_capabilities %v, want [hazmat]", caps)
	}
	if _, ok := data["fragile"]; ok {
		t.Fatalf("did not expect fragile field for a non-fragile SKU, got %v", data)
	}
}

func TestPublisher_WorkReleased_FragileSKU_SetsFragileTrue(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-2", "sku-fragile")
	lookup := &fakeClassificationLookup{views: map[string]productclassificationview.ProductClassificationView{
		"sku-fragile": {SKU: "sku-fragile", HandlingTags: []string{"Fragile"}, Known: true},
	}}
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, lookup, func() string { return "evt-2" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-2", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	fragile, ok := data["fragile"].(bool)
	if !ok || !fragile {
		t.Fatalf("expected fragile=true, got %v", data)
	}
	if _, ok := data["required_capabilities"]; ok {
		t.Fatalf("did not expect required_capabilities for a non-hazmat SKU, got %v", data)
	}
}

func TestPublisher_WorkReleased_UnclassifiedSKU_OmitsBothFields(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-3", "sku-plain")
	lookup := &fakeClassificationLookup{views: map[string]productclassificationview.ProductClassificationView{
		"sku-plain": {SKU: "sku-plain", Known: true}, // classified, but no relevant tags
	}}
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, lookup, func() string { return "evt-3" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-3", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	if _, ok := data["required_capabilities"]; ok {
		t.Fatalf("did not expect required_capabilities, got %v", data)
	}
	if _, ok := data["fragile"]; ok {
		t.Fatalf("did not expect fragile, got %v", data)
	}
}

func TestPublisher_WorkReleased_LookupUnavailable_OmitsBothFieldsAndStillPublishes(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-4", "sku-hazmat")
	lookup := &fakeClassificationLookup{err: errors.New("classification copy unreadable")}
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, lookup, func() string { return "evt-4" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-4", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: publish must not fail on a classification lookup error, got %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	if _, ok := data["required_capabilities"]; ok {
		t.Fatalf("did not expect required_capabilities on lookup failure, got %v", data)
	}
	if _, ok := data["fragile"]; ok {
		t.Fatalf("did not expect fragile on lookup failure, got %v", data)
	}
	// Base fields must still be present — enrichment failure is not
	// allowed to degrade the existing contract.
	if data["work_unit_id"] != "wu-4" {
		t.Fatalf("expected work_unit_id wu-4 to still be published, got %v", data)
	}
}

func TestPublisher_WorkReleased_NilClassificationsPort_OmitsBothFields(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-5", "sku-hazmat")
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-5" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-5", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	if _, ok := data["required_capabilities"]; ok {
		t.Fatalf("did not expect required_capabilities with a nil classifications port, got %v", data)
	}
	if _, ok := data["fragile"]; ok {
		t.Fatalf("did not expect fragile with a nil classifications port, got %v", data)
	}
}

func TestPublisher_WorkReleased_EmptySKU_SkipsLookupEntirely(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-6", "") // no SKU known
	lookup := &fakeClassificationLookup{views: map[string]productclassificationview.ProductClassificationView{
		"": {SKU: "", HandlingTags: []string{"Hazmat"}, Known: true}, // would be wrong to ever hit this
	}}
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, lookup, func() string { return "evt-6" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-6", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	if _, ok := data["required_capabilities"]; ok {
		t.Fatalf("did not expect required_capabilities for an empty SKU, got %v", data)
	}
}

func TestPublisher_WorkReleased_PermissiveLookup_OmitsBothFields(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-7", "sku-hazmat")
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, productclassificationcopy.NewPermissiveLookup(), func() string { return "evt-7" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-7", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	if _, ok := data["required_capabilities"]; ok {
		t.Fatalf("did not expect required_capabilities under PermissiveLookup, got %v", data)
	}
	if _, ok := data["fragile"]; ok {
		t.Fatalf("did not expect fragile under PermissiveLookup, got %v", data)
	}
}

func TestPublisher_WorkReleased_BaseFieldsStillPopulated(t *testing.T) {
	workUnits := newReleasedWorkUnit(t, "wu-8", "sku-plain")
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, productclassificationcopy.NewPermissiveLookup(), func() string { return "evt-8" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-8", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	if data["path_id"] != "pick-a" || data["work_unit_id"] != "wu-8" || data["ref"] != "ref-1" {
		t.Fatalf("unexpected base fields: %v", data)
	}
}

func TestPublisher_WorkReleased_GiftWrapRequested_SetsGiftWrapTrue(t *testing.T) {
	workUnits := newReleasedWorkUnitWithGiftWrap(t, "wu-9", "", true)
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-9" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-9", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	giftWrap, ok := data["gift_wrap"].(bool)
	if !ok || !giftWrap {
		t.Fatalf("expected gift_wrap=true, got %v", data)
	}
}

func TestPublisher_WorkReleased_GiftWrapNotRequested_OmitsGiftWrapField(t *testing.T) {
	workUnits := newReleasedWorkUnitWithGiftWrap(t, "wu-10", "", false)
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-10" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-10", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	if _, ok := data["gift_wrap"]; ok {
		t.Fatalf("did not expect gift_wrap field when not requested, got %v", data)
	}
}

func TestPublisher_WorkReleased_GiftWrapAndFragileAreIndependent(t *testing.T) {
	workUnits := newReleasedWorkUnitWithGiftWrap(t, "wu-11", "sku-fragile", true)
	lookup := &fakeClassificationLookup{views: map[string]productclassificationview.ProductClassificationView{
		"sku-fragile": {SKU: "sku-fragile", HandlingTags: []string{"Fragile"}, Known: true},
	}}
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, lookup, func() string { return "evt-11" })

	pathId, _ := shared.NewPathId("pick-a")
	event := shared.NewWorkReleased("wu-11", pathId, time.Now())
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	giftWrap, ok := data["gift_wrap"].(bool)
	if !ok || !giftWrap {
		t.Fatalf("expected gift_wrap=true (caller-supplied), got %v", data)
	}
	fragile, ok := data["fragile"].(bool)
	if !ok || !fragile {
		t.Fatalf("expected fragile=true (SKU-derived), got %v", data)
	}
}

func TestPublisher_PathCapacityChanged_Known_EncodesAllFields(t *testing.T) {
	workUnits := memory.NewWorkUnitRepo()
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-cap-1" })

	pathId, _ := shared.NewPathId("pick-a")
	cutoff := time.Date(2026, 8, 21, 15, 0, 0, 0, time.UTC)
	event := shared.NewPathCapacityChanged(pathId, cutoff, 17, true, time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC))
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	if data["path_id"] != "pick-a" {
		t.Fatalf("got path_id %v, want pick-a", data["path_id"])
	}
	if data["cutoff_at"] != "2026-08-21T15:00:00Z" {
		t.Fatalf("got cutoff_at %v, want RFC3339 2026-08-21T15:00:00Z", data["cutoff_at"])
	}
	remaining, ok := data["remaining_units"].(float64)
	if !ok || remaining != 17 {
		t.Fatalf("got remaining_units %v, want 17", data["remaining_units"])
	}
	known, ok := data["known"].(bool)
	if !ok || !known {
		t.Fatalf("got known %v, want true", data["known"])
	}
}

func TestPublisher_PathCapacityChanged_Unknown_EncodesFalseAndZero(t *testing.T) {
	workUnits := memory.NewWorkUnitRepo()
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-cap-2" })

	pathId, _ := shared.NewPathId("pack-b")
	cutoff := time.Date(2026, 8, 21, 16, 0, 0, 0, time.UTC)
	event := shared.NewPathCapacityChanged(pathId, cutoff, 0, false, time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC))
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	known, ok := data["known"].(bool)
	if !ok || known {
		t.Fatalf("got known %v, want false", data["known"])
	}
	remaining, ok := data["remaining_units"].(float64)
	if !ok || remaining != 0 {
		t.Fatalf("got remaining_units %v, want 0", data["remaining_units"])
	}
}

// ADR-0019: PathPlanDriftDetected carries both heads figures, the signed drift
// and Workforce's own commit timestamp.
func TestPublisher_PathPlanDriftDetected_EncodesAllFields(t *testing.T) {
	workUnits := memory.NewWorkUnitRepo()
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-drift-1" })

	pathId, _ := shared.NewPathId("pick-a")
	observedAt := time.Date(2026, 8, 21, 11, 0, 0, 0, time.UTC)
	event := shared.NewPathPlanDriftDetected(pathId, 6, 4, observedAt, time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC))
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := decodeWorkReleasedData(t, writer.msgs[0])
	want := map[string]any{
		"path_id":                "pick-a",
		"wes_planned_heads":      float64(6),
		"observed_planned_heads": float64(4),
		"drift_heads":            float64(-2), // signed: observed - ours
		"observed_at":            "2026-08-21T11:00:00Z",
	}
	for k, v := range want {
		if data[k] != v {
			t.Fatalf("data[%q] = %v, want %v (full: %v)", k, data[k], v, data)
		}
	}
}
