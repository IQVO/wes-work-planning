package kafka

// Shared support for the event consumer contract tests (E1-E4), which
// validate this repo's inbound Kafka edges against PINNED copies of the
// producing side's AsyncAPI specs (contracts/pinned.json; the
// pinned-contracts CI job fails when a copy drifts from the producer's
// develop). The pinned copies are byte-for-byte producer files: never
// edit them — refresh = copy the producer's file over the pin.
//
// The pinned specs are NOT uniform in how they declare a channel's
// messages. Fleet convention is channel-perspective (a producer's OWN
// topics under 'subscribe:'), but this wave pins producers that deviate:
// process-path-management documents its own topic under 'publish:', and
// network-inventory-planning attaches a channel-level 'messages:' map
// with no operations at all. channelMessageNames accepts all three
// shapes — the message list, wherever it sits, is the contract; the
// reader config in this repo (never the spec's operations) is the
// subscription source of truth.

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
)

// pinnedAsyncAPI is the minimal slice of AsyncAPI 2.6 the contract tests
// read: channels (with their operations and channel-level message maps),
// and the components/messages + components/schemas the message $refs
// point at.
type pinnedAsyncAPI struct {
	AsyncAPI           string                   `yaml:"asyncapi"`
	DefaultContentType string                   `yaml:"defaultContentType"`
	Channels           map[string]pinnedChannel `yaml:"channels"`
	Components         pinnedComponents         `yaml:"components"`
}

type pinnedChannel struct {
	Subscribe *pinnedOperation `yaml:"subscribe"`
	Publish   *pinnedOperation `yaml:"publish"`
	// Messages is the channel-level message map some producers use
	// instead of subscribe/publish operations.
	Messages map[string]pinnedMessageRef `yaml:"messages"`
}

type pinnedOperation struct {
	OperationID string            `yaml:"operationId"`
	Message     *pinnedMessageRef `yaml:"message"`
}

// pinnedMessageRef is either a single $ref or a oneOf list of $refs.
type pinnedMessageRef struct {
	Ref   string             `yaml:"$ref"`
	OneOf []pinnedMessageRef `yaml:"oneOf"`
}

type pinnedComponents struct {
	Messages map[string]pinnedMessage `yaml:"messages"`
	Schemas  map[string]any           `yaml:"schemas"`
}

type pinnedMessage struct {
	Name        string          `yaml:"name"`
	ContentType string          `yaml:"contentType"`
	Payload     any             `yaml:"payload"`
	Examples    []pinnedExample `yaml:"examples"`
}

// pinnedExample is a message example: a full wire payload (a structured
// CloudEvents JSON document, as YAML) plus its Kafka headers.
type pinnedExample struct {
	Name    string            `yaml:"name"`
	Headers map[string]string `yaml:"headers"`
	Payload map[string]any    `yaml:"payload"`
}

// loadPinnedAsyncAPI reads and parses one pinned spec copy.
func loadPinnedAsyncAPI(t *testing.T, path string) *pinnedAsyncAPI {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pinned spec %s (copy it from the producer, never hand-write it): %v", path, err)
	}
	var doc pinnedAsyncAPI
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec %s: %v", path, err)
	}
	return &doc
}

// channelMessageNames returns the components/messages names a topic's
// channel documents, and how it documents them ("subscribe", "publish"
// or "channel-messages"). Failing loudly on a missing channel: a topic
// the reader config subscribes to that the producer does not document is
// exactly the incompatibility E1 exists to catch.
func (d *pinnedAsyncAPI) channelMessageNames(t *testing.T, topic string) ([]string, string) {
	t.Helper()
	ch, ok := d.Channels[topic]
	if !ok {
		t.Fatalf("pinned spec has no channel for topic %q", topic)
	}
	if ch.Subscribe != nil && ch.Subscribe.Message != nil {
		return messageRefNames(t, *ch.Subscribe.Message), "subscribe"
	}
	if ch.Publish != nil && ch.Publish.Message != nil {
		return messageRefNames(t, *ch.Publish.Message), "publish"
	}
	if len(ch.Messages) > 0 {
		names := make([]string, 0, len(ch.Messages))
		for name := range ch.Messages {
			names = append(names, refName(t, ch.Messages[name].Ref))
		}
		return names, "channel-messages"
	}
	t.Fatalf("pinned spec documents topic %q with no messages (no subscribe/publish operation, no channel-level message map)", topic)
	return nil, ""
}

func messageRefNames(t *testing.T, m pinnedMessageRef) []string {
	t.Helper()
	var refs []pinnedMessageRef
	if m.Ref != "" {
		refs = append(refs, m)
	}
	refs = append(refs, m.OneOf...)
	if len(refs) == 0 {
		t.Fatalf("channel operation carries no messages")
	}
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		names = append(names, refName(t, r.Ref))
	}
	return names
}

// message resolves one components/messages entry, failing on a dangling
// $ref (a spec whose channel points at a message it does not define).
func (d *pinnedAsyncAPI) message(t *testing.T, name string) pinnedMessage {
	t.Helper()
	msg, ok := d.Components.Messages[name]
	if !ok {
		t.Fatalf("pinned spec references undefined components/messages/%s", name)
	}
	return msg
}

// documentedTypes returns every CloudEvents `type` the channel's
// messages declare, from a payload-schema const wherever the spec pins
// one and — for specs whose payload schemas declare no const — from the
// type attribute the message's own examples carry. A message that
// declares neither is a spec gap worth seeing in the failure output.
func (d *pinnedAsyncAPI) documentedTypes(t *testing.T, topic string) map[string]bool {
	t.Helper()
	names, _ := d.channelMessageNames(t, topic)
	documented := map[string]bool{}
	for _, name := range names {
		msg := d.message(t, name) // fails the test on a dangling $ref
		if c := d.payloadConsts(t, msg.Payload)["type"]; c != "" {
			documented[c] = true
		}
		for _, ex := range msg.Examples {
			if ty, ok := ex.Payload["type"].(string); ok {
				documented[ty] = true
			}
		}
	}
	return documented
}

// payloadConsts collects the const values a message payload declares for
// CloudEvents envelope attributes (type, dataschema, ...), wherever the
// spec keeps them — directly under properties, in an allOf overlay on
// the shared envelope schema, or behind a payload-level $ref into
// components/schemas.
func (d *pinnedAsyncAPI) payloadConsts(t *testing.T, payload any) map[string]string {
	t.Helper()
	consts := map[string]string{}
	d.collectPayloadConsts(t, payload, consts, map[string]bool{})
	return consts
}

func (d *pinnedAsyncAPI) collectPayloadConsts(t *testing.T, node any, consts map[string]string, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectPayloadConsts(t, schema, consts, resolving)
		return
	}
	for attr, c := range propertyConsts(v) {
		if _, seen := consts[attr]; !seen {
			consts[attr] = c
		}
	}
	for _, item := range allOfItems(v) {
		d.collectPayloadConsts(t, item, consts, resolving)
	}
}

// resolveSchemaRef resolves node's top-level $ref into components/schemas
// (cycle-guarded). ok is false when node carries no $ref, or the ref is
// already being resolved.
func (d *pinnedAsyncAPI) resolveSchemaRef(t *testing.T, node map[string]any, resolving map[string]bool) (any, bool) {
	ref, ok := node["$ref"].(string)
	if !ok {
		return nil, false
	}
	name := refName(t, ref)
	if resolving[name] {
		return nil, false
	}
	resolving[name] = true
	schema, ok := d.Components.Schemas[name]
	if !ok {
		t.Fatalf("pinned spec: unresolved schema $ref %q", ref)
	}
	return schema, true
}

// propertyConsts returns the const string each directly-declared property
// of node pins (empty when node pins none).
func propertyConsts(node map[string]any) map[string]string {
	consts := map[string]string{}
	props, ok := node["properties"].(map[string]any)
	if !ok {
		return consts
	}
	for attr, p := range props {
		if pm, ok := p.(map[string]any); ok {
			if c, ok := pm["const"].(string); ok {
				consts[attr] = c
			}
		}
	}
	return consts
}

// allOfItems returns node's allOf entries (nil when none).
func allOfItems(node map[string]any) []any {
	items, _ := node["allOf"].([]any)
	return items
}

// envelopeRequired returns the required attribute names declared by a
// message payload's envelope schema (resolving top-level $refs into
// components/schemas and walking allOf). The subset that names
// CloudEvents attributes is what E3 asserts on.
func (d *pinnedAsyncAPI) envelopeRequired(t *testing.T, payload any) map[string]bool {
	t.Helper()
	required := map[string]bool{}
	d.collectRequired(t, payload, required, map[string]bool{})
	return required
}

func (d *pinnedAsyncAPI) collectRequired(t *testing.T, node any, required map[string]bool, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	for _, r := range requiredList(v) {
		required[r] = true
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectRequired(t, schema, required, resolving)
		return
	}
	for _, item := range allOfItems(v) {
		d.collectRequired(t, item, required, resolving)
	}
}

// requiredList returns node's required attribute names (nil when none).
func requiredList(node map[string]any) []string {
	raw, ok := node["required"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// refName extracts the last path segment of a local $ref
// ("#/components/messages/TaskCompleted" -> "TaskCompleted").
func refName(t *testing.T, ref string) string {
	t.Helper()
	i := strings.LastIndex(ref, "/")
	if i < 0 || i == len(ref)-1 {
		t.Fatalf("malformed local $ref %q", ref)
	}
	return ref[i+1:]
}

// goldenJSON marshals a pinned example's YAML-decoded payload into the
// JSON bytes a Kafka consumer actually receives (structured-mode
// CloudEvents: the message value IS the JSON envelope).
func goldenJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	return raw
}

// goldenInt coerces a numeric field of a YAML-decoded golden payload to
// int — yaml.v3 decodes whole numbers as int, json.Unmarshal as float64,
// and E2 fixtures flow through both.
func goldenInt(t *testing.T, v any) int {
	t.Helper()
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		t.Fatalf("golden payload field is not numeric: %T (%v)", v, v)
		return 0
	}
}

// clonePayload deep-copies a golden example payload so a test can mutate
// it without touching the shared fixture.
func clonePayload(p map[string]any) map[string]any {
	return cloneMap(p).(map[string]any)
}

func cloneMap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneMap(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneMap(val)
		}
		return out
	default:
		return v
	}
}

// freshGoldenID mints a fresh CloudEvents id on a perturbed clone of a
// golden example: the id is the idempotency key, so a clone that keeps
// the original id can silently pass through a processed-set gate another
// test run already claimed (fail-soft skips still claim).
func freshGoldenID(t *testing.T, payload map[string]any, suffix string) map[string]any {
	t.Helper()
	p := clonePayload(payload)
	id, _ := p["id"].(string)
	p["id"] = fmt.Sprintf("%s-%s", id, suffix)
	return p
}

// assertGoldenEnvelope runs the fleet-mandatory CloudEvents 1.0 checks
// (E3) on one pinned example: specversion, id/source/subject/time, the
// producer's source and dataschema urn, and this repo's datacontenttype
// constant. Per-example helper so every producer's E3 loop stays under
// the cognitive-complexity gate.
func assertGoldenEnvelope(t *testing.T, ex pinnedExample, wantSource, wantSchema string) {
	t.Helper()
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode golden example %s: %v", ex.Name, err)
	}
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s: specversion %q, want %q", ex.Name, evt.SpecVersion(), cloudevents.SpecVersion)
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
		t.Errorf("%s: id/source/subject/time must all be present", ex.Name)
	}
	if evt.Source() != wantSource {
		t.Errorf("%s: source %q, want %q", ex.Name, evt.Source(), wantSource)
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
	}
	if evt.DataSchema() != wantSchema {
		t.Errorf("%s: dataschema %q, want %q", ex.Name, evt.DataSchema(), wantSchema)
	}
}
