package kafkacatalog

// Event consumer contract tests (E1-E4) for the process-path-management
// edge: this package's Consumer replays process-path-management's topic
// warehouse.process-path-management.events (the Topic constant wired
// into kafka.ReaderConfig by NewConsumer) into the local PathCatalogue,
// acting on ProcessPathCreated, ProcessPathUpdated (upsert) and
// ProcessPathDeactivated (remove). The contract is the PINNED copy of
// process-path-management's apis/asyncapi.yaml
// (testdata/process-path-management.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/process-path-management@develop).
//
// NOTE ON CONVENTION: this producer documents its OWN topic under
// 'publish:' (app perspective), unlike the fleet's channel-perspective
// majority — verified against this repo's reader config, which is the
// subscription source of truth either way.
//
// Documented WITHOUT routing here: CPTScheduleChanged — E1-only (this
// catalogue never reads CPT schedules).
//
// The pinned-spec helpers here are a small in-package duplicate of the
// inbound/kafka contract helpers (unexported, same names across the two
// test packages are legal).

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
)

const pinnedProcessPathSpec = "testdata/process-path-management.asyncapi.yaml"

type pinnedDoc struct {
	Channels   map[string]map[string]any `yaml:"channels"`
	Components struct {
		Messages map[string]pinnedMessage `yaml:"messages"`
		Schemas  map[string]any           `yaml:"schemas"`
	} `yaml:"components"`
}

type pinnedMessage struct {
	Payload  any             `yaml:"payload"`
	Examples []pinnedExample `yaml:"examples"`
}

type pinnedExample struct {
	Name    string         `yaml:"name"`
	Payload map[string]any `yaml:"payload"`
}

func loadPinned(t *testing.T) *pinnedDoc {
	t.Helper()
	raw, err := os.ReadFile(pinnedProcessPathSpec)
	if err != nil {
		t.Fatalf("read pinned spec (copy it from the producer, never hand-write it): %v", err)
	}
	var doc pinnedDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec: %v", err)
	}
	return &doc
}

// channelMessageNames finds the topic's message names wherever this
// spec shape keeps them (subscribe/publish operation, or a channel-level
// map), returning the operation side for the failure messages.
func (d *pinnedDoc) channelMessageNames(t *testing.T, topic string) ([]string, string) {
	t.Helper()
	ch, ok := d.Channels[topic]
	if !ok {
		t.Fatalf("pinned spec has no channel for topic %q", topic)
	}
	for _, side := range []string{"subscribe", "publish"} {
		op, _ := ch[side].(map[string]any)
		msg, _ := op["message"].(map[string]any)
		if msg == nil {
			continue
		}
		names := messageNames(t, msg)
		if len(names) > 0 {
			return names, side
		}
	}
	t.Fatalf("pinned spec documents topic %q with no operation messages", topic)
	return nil, ""
}

func messageNames(t *testing.T, msg map[string]any) []string {
	t.Helper()
	var refs []string
	if ref, _ := msg["$ref"].(string); ref != "" {
		refs = append(refs, ref)
	}
	if oneOf, ok := msg["oneOf"].([]any); ok {
		for _, o := range oneOf {
			if m, ok := o.(map[string]any); ok {
				if ref, _ := m["$ref"].(string); ref != "" {
					refs = append(refs, ref)
				}
			}
		}
	}
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		i := strings.LastIndex(r, "/")
		if i < 0 {
			t.Fatalf("malformed local $ref %q", r)
		}
		names = append(names, r[i+1:])
	}
	return names
}

func newContractConsumer() *Consumer {
	return &Consumer{paths: make(map[string]pathcatalog.PathDefinition)}
}

func processPathGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	msg, ok := loadPinned(t).Components.Messages[message]
	if !ok {
		t.Fatalf("pinned spec references undefined components/messages/%s", message)
	}
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned process-path-management spec documents %s without examples", message)
	}
	return msg.Examples
}

// E1: the topic this repo's reader replays is documented by the
// producer's pinned spec, AND the channel's message list documents all
// three CloudEvents types the handle switch routes on.
func TestPinnedAsyncAPICoversSubscriptions_ProcessPathEvents(t *testing.T) {
	doc := loadPinned(t)
	names, how := doc.channelMessageNames(t, Topic)
	if len(names) == 0 {
		t.Fatalf("no message list found for %s", Topic)
	}
	documented := map[string]bool{}
	for _, name := range names {
		msg, ok := doc.Components.Messages[name]
		if !ok {
			t.Fatalf("pinned spec references undefined components/messages/%s", name)
		}
		for _, ex := range msg.Examples {
			if ty, ok := ex.Payload["type"].(string); ok {
				documented[ty] = true
			}
		}
	}
	for _, want := range []string{eventTypeCreated, eventTypeUpdated, eventTypeDeactivated} {
		if !documented[want] {
			t.Errorf("pinned process-path-management spec (%s side) does not document %s on %s (documented: %v)", how, want, Topic, documented)
		}
	}
}

// E2: every ProcessPathCreated / Updated / Deactivated example the
// producer's pinned spec documents decodes through the REAL
// cloudevents.Decode and routes through the REAL handle -> applyUpsert
// / applyDeactivated path: the catalogue ends up carrying the wire's
// own path definition (Created/Updated) or lacking it (Deactivated).
func TestGoldenEventsDecode_ProcessPaths(t *testing.T) {
	c := newContractConsumer()
	created := 0
	for _, ex := range processPathGoldenExamples(t, "ProcessPathCreated") {
		if err := c.handle(goldenPathMsg(t, ex)); err != nil {
			t.Fatalf("%s: handle: %v", ex.Name, err)
		}
		created++
	}
	if got := len(c.paths); got != created {
		t.Errorf("%d ProcessPathCreated examples produced %d catalogue entries", created, got)
	}
	for _, ex := range processPathGoldenExamples(t, "ProcessPathUpdated") {
		if err := c.handle(goldenPathMsg(t, ex)); err != nil {
			t.Fatalf("%s: handle: %v", ex.Name, err)
		}
		assertPathUpserted(t, c, ex)
	}
	for _, ex := range processPathGoldenExamples(t, "ProcessPathDeactivated") {
		if err := c.handle(goldenPathMsg(t, ex)); err != nil {
			t.Fatalf("%s: handle: %v", ex.Name, err)
		}
		pathId := ex.Payload["data"].(map[string]any)["path_id"].(string)
		if _, still := c.paths[strings.ToUpper(pathId)]; still {
			t.Errorf("%s: deactivation left %s in the catalogue", ex.Name, pathId)
		}
	}
}

func assertPathUpserted(t *testing.T, c *Consumer, ex pinnedExample) {
	t.Helper()
	data := ex.Payload["data"].(map[string]any)
	pathId := data["path_id"].(string)
	def, ok := c.paths[strings.ToUpper(pathId)]
	if !ok {
		t.Fatalf("%s: upsert left no catalogue entry for %s", ex.Name, pathId)
	}
	if prefix, _ := data["match_prefix"].(string); def.MatchPrefix != prefix {
		t.Errorf("%s: catalogue match_prefix %q, wire says %q", ex.Name, def.MatchPrefix, prefix)
	}
}

func goldenPathMsg(t *testing.T, ex pinnedExample) kafkago.Message {
	t.Helper()
	raw, err := json.Marshal(ex.Payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	return kafkago.Message{Topic: Topic, Value: raw}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope and the producer's dataschema urn.
func TestCloudEventsEnvelopeInvariants_ProcessPaths(t *testing.T) {
	for _, message := range []string{"ProcessPathCreated", "ProcessPathUpdated", "ProcessPathDeactivated"} {
		for _, ex := range processPathGoldenExamples(t, message) {
			evt, err := cloudevents.Decode(goldenPathMsg(t, ex).Value)
			if err != nil {
				t.Fatalf("decode golden example %s: %v", ex.Name, err)
			}
			assertPathEnvelope(t, ex.Name, evt)
		}
	}
}

func assertPathEnvelope(t *testing.T, name string, evt ce.Event) {
	t.Helper()
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s: specversion %q, want %q", name, evt.SpecVersion(), cloudevents.SpecVersion)
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
		t.Errorf("%s: id/source/subject/time must all be present", name)
	}
	if evt.Source() != "/warehouse/process-path-management" {
		t.Errorf("%s: source %q, want /warehouse/process-path-management", name, evt.Source())
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s: datacontenttype %q, want %q", name, evt.DataMediaType(), cloudevents.DataContentType)
	}
	if !strings.HasPrefix(evt.DataSchema(), "urn:warehouse:process-path-management:") {
		t.Errorf("%s: dataschema %q, want the producer's urn prefix", name, evt.DataSchema())
	}
}

// E4: an unknown event TYPE outside this consumer's contract is ignored
// (the switch's default arm, per its own code comment), and a
// non-CloudEvent message is returned as an error wrapping
// cloudevents.ErrNotCloudEvent (the replay loop's documented poison
// signal — see consumer_test.go's legacy-envelope case).
func TestUnknownAttributesTolerated_ProcessPaths(t *testing.T) {
	c := newContractConsumer()
	golden := processPathGoldenExamples(t, "ProcessPathCreated")[0]

	tolerant := cloneGolden(golden)
	tolerant["id"] = "pp-tolerated"
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	if err := c.handle(goldenPathMsg(t, pinnedExample{Payload: tolerant})); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}

	otherType := cloneGolden(golden)
	otherType["id"] = "pp-othertype"
	otherType["type"] = "com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged"
	before := len(c.paths)
	if err := c.handle(goldenPathMsg(t, pinnedExample{Payload: otherType})); err != nil {
		t.Fatalf("unknown event type must be ignored per the consumer's documented policy: %v", err)
	}
	if after := len(c.paths); after != before {
		t.Errorf("an out-of-contract type changed the catalogue: %d -> %d", before, after)
	}

	if err := (&Consumer{}).handle(kafkago.Message{Topic: Topic, Value: []byte(`{"path_id":"PICK"}`)}); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("flat payload returned %v, want ErrNotCloudEvent (the replay loop's poison signal)", err)
	}
}

func cloneGolden(p pinnedExample) map[string]any {
	return cloneAny(p.Payload).(map[string]any)
}

func cloneAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneAny(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneAny(val)
		}
		return out
	default:
		return v
	}
}
