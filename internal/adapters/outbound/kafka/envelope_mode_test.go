package kafka_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/envelope"
	outboundkafka "github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// updateGolden regenerates the golden files below when set (go test
// -run TestPublisher_EnvelopeMode -update-golden ./...). Never enabled by
// default -- the whole point of this test is to CATCH an accidental wire
// format change, not to silently absorb one.
var updateGolden = os.Getenv("UPDATE_GOLDEN") == "1"

func goldenPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join("testdata", name)
}

func compareOrWriteGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := goldenPath(t, name)
	if updateGolden {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run with UPDATE_GOLDEN=1 to create it)", path, err)
	}
	if string(got) != string(want) {
		t.Fatalf("flat-mode JSON output for %s changed -- this is the ADR-0021 regression gate: flat mode must stay byte-identical to the wire format shipped before EVENT_ENVELOPE_MODE existed.\ngot:  %s\nwant: %s", name, got, want)
	}
}

// --- Fixture builders shared by every mode below -------------------------

func goldenWorkReleasedFixture(t *testing.T) (*memory.WorkUnitRepo, shared.WorkReleased, time.Time) {
	t.Helper()
	workUnits := newReleasedWorkUnitWithGiftWrap(t, "wu-golden-1", "sku-plain", true)
	pathId := mustPathId(t, "pick-a")
	at := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	return workUnits, shared.NewWorkReleased("wu-golden-1", pathId, at), at
}

func goldenPathCapacityChangedFixture(t *testing.T) (shared.PathCapacityChanged, time.Time) {
	t.Helper()
	pathId := mustPathId(t, "pick-a")
	cutoff := time.Date(2026, 8, 21, 15, 0, 0, 0, time.UTC)
	at := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	return shared.NewPathCapacityChanged(pathId, cutoff, 17, true, at), at
}

// --- 1. flat mode: byte-identical golden-file regression gate ------------

func TestPublisher_EnvelopeMode_Flat_WorkReleased_ByteIdenticalGolden(t *testing.T) {
	workUnits, event, _ := goldenWorkReleasedFixture(t)
	// No WithEnvelopeMode option: the zero value is EnvelopeModeFlat, so
	// this exercises the exact same default path every pre-existing
	// caller of NewPublisherWithWriter goes through.
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "evt-golden-1" })

	encoded, err := pub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("flat mode must emit exactly 1 message, got %d", len(encoded))
	}
	compareOrWriteGolden(t, "flat_workreleased.golden.json", encoded[0].Value)
}

func TestPublisher_EnvelopeMode_Flat_PathCapacityChanged_ByteIdenticalGolden(t *testing.T) {
	event, _ := goldenPathCapacityChangedFixture(t)
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, memory.NewWorkUnitRepo(), nil, func() string { return "evt-golden-2" })

	encoded, err := pub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("flat mode must emit exactly 1 message, got %d", len(encoded))
	}
	compareOrWriteGolden(t, "flat_pathcapacitychanged.golden.json", encoded[0].Value)
}

func TestPublisher_EnvelopeMode_ExplicitFlat_MatchesDefault(t *testing.T) {
	workUnits, event, _ := goldenWorkReleasedFixture(t)
	defaultPub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "evt-golden-1" })
	explicitPub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "evt-golden-1" },
		outboundkafka.WithEnvelopeMode(outboundkafka.EnvelopeModeFlat))

	gotDefault, err := defaultPub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotExplicit, err := explicitPub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(gotDefault[0].Value) != string(gotExplicit[0].Value) {
		t.Fatalf("explicit EnvelopeModeFlat produced different output than the zero value default:\ndefault:  %s\nexplicit: %s", gotDefault[0].Value, gotExplicit[0].Value)
	}
}

// --- 2. cloudevents mode: schema-field assertions -------------------------

func decodeCloudEvent(t *testing.T, raw []byte) envelope.CloudEvent {
	t.Helper()
	var ce envelope.CloudEvent
	if err := json.Unmarshal(raw, &ce); err != nil {
		t.Fatalf("unmarshal CloudEvent: %v", err)
	}
	return ce
}

func TestPublisher_EnvelopeMode_CloudEvents_WorkReleased_SchemaFields(t *testing.T) {
	workUnits, event, at := goldenWorkReleasedFixture(t)
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "evt-ce-1" },
		outboundkafka.WithEnvelopeMode(outboundkafka.EnvelopeModeCloudEvents))

	encoded, err := pub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("cloudevents mode must emit exactly 1 message per event, got %d", len(encoded))
	}
	ce := decodeCloudEvent(t, encoded[0].Value)

	if ce.SpecVersion != "1.0" {
		t.Errorf("specversion = %q, want 1.0", ce.SpecVersion)
	}
	if ce.Id != "evt-ce-1" {
		t.Errorf("id = %q, want the event id (evt-ce-1), same value as today's event_id", ce.Id)
	}
	wantType := "com.warehouse.wes.work-planning.workunit.WorkReleased"
	if ce.Type != wantType {
		t.Errorf("type = %q, want %q", ce.Type, wantType)
	}
	if ce.Source != "/warehouse/wes-work-planning" {
		t.Errorf("source = %q, want /warehouse/wes-work-planning", ce.Source)
	}
	if ce.Subject != "wu-golden-1" {
		t.Errorf("subject = %q, want the released work unit id (wu-golden-1)", ce.Subject)
	}
	if !ce.Time.Equal(at) {
		t.Errorf("time = %v, want %v (same value as today's occurred_at)", ce.Time, at)
	}
	if ce.DataContentType != "application/json" {
		t.Errorf("datacontenttype = %q, want application/json", ce.DataContentType)
	}

	var data map[string]any
	if err := json.Unmarshal(ce.Data, &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if data["work_unit_id"] != "wu-golden-1" || data["ref"] != "ref-1" || data["gift_wrap"] != true {
		t.Fatalf("data payload changed shape vs. the flat envelope's data: %v", data)
	}
}

func TestPublisher_EnvelopeMode_CloudEvents_PathCapacityChanged_SchemaFields(t *testing.T) {
	event, at := goldenPathCapacityChangedFixture(t)
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, memory.NewWorkUnitRepo(), nil, func() string { return "evt-ce-2" },
		outboundkafka.WithEnvelopeMode(outboundkafka.EnvelopeModeCloudEvents))

	encoded, err := pub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("cloudevents mode must emit exactly 1 message per event, got %d", len(encoded))
	}
	ce := decodeCloudEvent(t, encoded[0].Value)

	if ce.SpecVersion != "1.0" {
		t.Errorf("specversion = %q, want 1.0", ce.SpecVersion)
	}
	if ce.Id != "evt-ce-2" {
		t.Errorf("id = %q, want evt-ce-2", ce.Id)
	}
	wantType := "com.warehouse.wes.work-planning.workpool.PathCapacityChanged"
	if ce.Type != wantType {
		t.Errorf("type = %q, want %q", ce.Type, wantType)
	}
	if ce.Source != "/warehouse/wes-work-planning" {
		t.Errorf("source = %q, want /warehouse/wes-work-planning", ce.Source)
	}
	if ce.Subject != "pick-a" {
		t.Errorf("subject = %q, want the path id (pick-a)", ce.Subject)
	}
	if !ce.Time.Equal(at) {
		t.Errorf("time = %v, want %v", ce.Time, at)
	}
	if ce.DataContentType != "application/json" {
		t.Errorf("datacontenttype = %q, want application/json", ce.DataContentType)
	}

	var data map[string]any
	if err := json.Unmarshal(ce.Data, &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if data["path_id"] != "pick-a" || data["cutoff_at"] != "2026-08-21T15:00:00Z" || data["remaining_units"] != float64(17) || data["known"] != true {
		t.Fatalf("data payload changed shape vs. the flat envelope's data: %v", data)
	}
}

func TestPublisher_EnvelopeMode_CloudEvents_KeyIsEventId(t *testing.T) {
	workUnits, event, _ := goldenWorkReleasedFixture(t)
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "evt-ce-key" },
		outboundkafka.WithEnvelopeMode(outboundkafka.EnvelopeModeCloudEvents))

	encoded, err := pub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(encoded[0].Key) != "evt-ce-key" {
		t.Fatalf("Kafka message key = %q, want the event id evt-ce-key (unchanged partitioning/ordering semantics)", encoded[0].Key)
	}
}

// --- 3. dual mode: exactly 2 messages, same key, one flat one cloudevents -

func TestPublisher_EnvelopeMode_Dual_TwoMessagesSharedKeyBothShapes(t *testing.T) {
	workUnits, event, at := goldenWorkReleasedFixture(t)
	pub := outboundkafka.NewPublisherWithWriter(&fakeWriter{}, workUnits, nil, func() string { return "evt-dual-1" },
		outboundkafka.WithEnvelopeMode(outboundkafka.EnvelopeModeDual))

	encoded, err := pub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(encoded) != 2 {
		t.Fatalf("dual mode must emit exactly 2 physical messages per domain event, got %d", len(encoded))
	}

	first, second := encoded[0], encoded[1]

	assertDualSharedMessageShape(t, first, second)
	assertDualFlatMessage(t, first, at)

	// The SECOND message must decode as CloudEvents 1.0: specversion
	// present and "1.0", id equals the shared event id.
	ce := decodeCloudEvent(t, second.Value)
	if ce.SpecVersion != "1.0" {
		t.Fatalf("second (cloudevents) message specversion = %q, want 1.0", ce.SpecVersion)
	}
	if ce.Id != "evt-dual-1" {
		t.Fatalf("second (cloudevents) message id = %q, want evt-dual-1 (same as the flat message's key/event_id)", ce.Id)
	}
	if ce.Type != "com.warehouse.wes.work-planning.workunit.WorkReleased" {
		t.Fatalf("second (cloudevents) message type = %q", ce.Type)
	}
	if ce.Subject != "wu-golden-1" {
		t.Fatalf("second (cloudevents) message subject = %q, want wu-golden-1", ce.Subject)
	}

	// data payload identical between the two physical messages.
	var flatData, ceData map[string]any
	if err := json.Unmarshal(flatEnvData(t, first).Data, &flatData); err != nil {
		t.Fatalf("unmarshal flat data: %v", err)
	}
	if err := json.Unmarshal(ce.Data, &ceData); err != nil {
		t.Fatalf("unmarshal cloudevents data: %v", err)
	}
	flatJSON, _ := json.Marshal(flatData)
	ceJSON, _ := json.Marshal(ceData)
	if string(flatJSON) != string(ceJSON) {
		t.Fatalf("dual mode's two physical messages carry different data payloads:\nflat: %s\ncloudevents: %s", flatJSON, ceJSON)
	}
}

// assertDualSharedMessageShape checks the properties both physical
// messages of a dual-mode event share: the same key (ADR-0021 Design
// decision #3) and the same integration topic.
func assertDualSharedMessageShape(t *testing.T, first, second outboundkafka.Encoded) {
	t.Helper()
	if string(first.Key) != string(second.Key) {
		t.Fatalf("dual mode messages do not share the same key: %q vs %q", first.Key, second.Key)
	}
	if string(first.Key) != "evt-dual-1" {
		t.Fatalf("shared key = %q, want the event id evt-dual-1", first.Key)
	}
	if first.Topic != envelope.TopicWorkPlanningEvents || second.Topic != envelope.TopicWorkPlanningEvents {
		t.Fatalf("dual mode messages must both target %s, got %q and %q", envelope.TopicWorkPlanningEvents, first.Topic, second.Topic)
	}
}

// assertDualFlatMessage checks that the FIRST dual-mode message decodes
// as the legacy flat envelope: it has event_id/event_type/occurred_at,
// and critically has NO specversion key at all -- ADR-0021 depends on
// specversion's absence being the dual-read discriminator.
func assertDualFlatMessage(t *testing.T, first outboundkafka.Encoded, at time.Time) {
	t.Helper()
	var probe map[string]any
	if err := json.Unmarshal(first.Value, &probe); err != nil {
		t.Fatalf("unmarshal first message as JSON: %v", err)
	}
	if _, hasSpecVersion := probe["specversion"]; hasSpecVersion {
		t.Fatalf("first (flat) message must not carry a specversion key, got %v", probe)
	}
	flatEnv := flatEnvData(t, first)
	if flatEnv.EventId != "evt-dual-1" || flatEnv.EventType != "WorkReleased" || !flatEnv.OccurredAt.Equal(at) || flatEnv.Source != envelope.Source {
		t.Fatalf("first (flat) message envelope wrong: %+v", flatEnv)
	}
}

// flatEnvData decodes a dual-mode flat message into its envelope shape.
func flatEnvData(t *testing.T, msg outboundkafka.Encoded) envelope.Envelope {
	t.Helper()
	var flatEnv envelope.Envelope
	if err := json.Unmarshal(msg.Value, &flatEnv); err != nil {
		t.Fatalf("unmarshal first message as Envelope: %v", err)
	}
	return flatEnv
}

func TestPublisher_EnvelopeMode_Dual_PublishWritesBothMessages(t *testing.T) {
	workUnits, event, _ := goldenWorkReleasedFixture(t)
	writer := &fakeWriter{}
	pub := outboundkafka.NewPublisherWithWriter(writer, workUnits, nil, func() string { return "evt-dual-2" },
		outboundkafka.WithEnvelopeMode(outboundkafka.EnvelopeModeDual))

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(writer.msgs) != 2 {
		t.Fatalf("Publish in dual mode must write 2 physical messages to the broker, got %d", len(writer.msgs))
	}
	if string(writer.msgs[0].Key) != string(writer.msgs[1].Key) {
		t.Fatalf("both written messages must share the same key, got %q and %q", writer.msgs[0].Key, writer.msgs[1].Key)
	}
}

// --- ParseEnvelopeMode -----------------------------------------------------

func TestParseEnvelopeMode(t *testing.T) {
	cases := map[string]outboundkafka.EnvelopeMode{
		"":              outboundkafka.EnvelopeModeFlat,
		"flat":          outboundkafka.EnvelopeModeFlat,
		"cloudevents":   outboundkafka.EnvelopeModeCloudEvents,
		"dual":          outboundkafka.EnvelopeModeDual,
		"bogus":         outboundkafka.EnvelopeModeFlat,
		"CloudEvents":   outboundkafka.EnvelopeModeFlat, // case-sensitive: unrecognized falls back to flat
		"  cloudevents": outboundkafka.EnvelopeModeFlat,
	}
	for raw, want := range cases {
		if got := outboundkafka.ParseEnvelopeMode(raw); got != want {
			t.Errorf("ParseEnvelopeMode(%q) = %q, want %q", raw, got, want)
		}
	}
}
