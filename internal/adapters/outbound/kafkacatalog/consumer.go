// Package kafkacatalog is the outbound adapter that keeps a local,
// in-memory process-path catalogue up to date by consuming
// process-path-management's warehouse.process-path-management.events
// topic, replacing the static YAML file (filecatalog) this service used
// to boot-load. It satisfies ports.PathCatalogue (the read side both the
// inbound Kafka consumer and the HTTP handlers already depend on) and a
// Ready()/WaitReady() gate so this service's composition root can block
// starting real work until the initial replay of the topic's full
// history completes.
//
// This package mirrors fulfillment-execution's own
// internal/adapters/outbound/kafkacatalog byte-for-byte in its
// concurrency/readiness design (both consume the SAME topic from the
// SAME upstream service) -- see that package's doc comments for the
// full rationale, including two real bugs found and fixed there before
// this copy was made:
//
//  1. A readiness gate that only re-evaluates "am I caught up" on a NEW
//     message deadlocks forever on an ordinary restart where nothing new
//     has been published since the last run.
//  2. A FIXED, shared consumer group name lets a new process resume from
//     a PRIOR process's committed offset, reporting itself ready with an
//     empty local cache having never actually replayed anything. Fixed
//     fleet-wide (here too) by making every NewConsumer call use a
//     unique, process-scoped group id -- never a shared name.
//
// One real difference from fulfillment-execution's copy: this service's
// pathcatalog.PathDefinition has NO Direct field (wes-work-planning
// never needed it) -- the Kafka payload's "direct" field is read and
// simply discarded here, not carried into the local PathDefinition.
// DestinationLocationRole (process-path-management's ADR-0009), by
// contrast, IS carried into PathDefinition verbatim -- decoded here and
// made available to any consumer of this local catalogue, even though
// nothing in this service's own domain logic branches on it yet.
package kafkacatalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
)

// Topic is process-path-management's publish topic — this service has no
// business knowing anything else about that service beyond this topic
// name and the CloudEvents type/payload shape below.
const Topic = "warehouse.process-path-management.events"

// consumerGroupPrefix names this service's dedicated, PER-PROCESS
// consumer group on Topic. Deliberately NOT a fixed, shared name — see
// the package doc comment's bug (2) for why per-process uniqueness is a
// correctness requirement here, not a cosmetic choice.
const consumerGroupPrefix = "wes-work-planning-process-path-catalogue"

// Full CloudEvents `type` strings this consumer acts on —
// process-path-management's own past-tense domain events. Dispatch is on
// the exact string, never a short name or suffix match.
const (
	eventTypeCreated     = cloudevents.TypeProcessPathCreated
	eventTypeUpdated     = cloudevents.TypeProcessPathUpdated
	eventTypeDeactivated = cloudevents.TypeProcessPathDeactivated
)

// pathData is the payload shape for all three event types on Topic.
// Direct is read but has no home in this service's own
// pathcatalog.PathDefinition (see package doc comment) — it is decoded
// and then simply discarded. DestinationLocationRole is omitted by the
// publisher (not empty-stringed) on a path with no declared destination
// role, so its Go zero value ("") already matches "not declared" with no
// extra handling needed here.
type pathData struct {
	PathId                  string   `json:"path_id"`
	MatchPrefix             string   `json:"match_prefix"`
	Direct                  bool     `json:"direct"`
	RequiredCapabilities    []string `json:"required_capabilities"`
	DestinationLocationRole string   `json:"destination_location_role,omitempty"`
}

// Reader is the subset of *kafkago.Reader this Consumer needs, so tests
// can substitute a fake without a live broker.
type Reader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
	Close() error
}

// Consumer maintains a local pathcatalog.Catalogue by replaying Topic
// from its earliest offset (a fresh, per-process consumer group) and
// applying every ProcessPath* event as it arrives. It satisfies
// ports.PathCatalogue via Lookup, delegating to the current in-memory
// snapshot.
type Consumer struct {
	Reader Reader
	Logger *slog.Logger

	mu      sync.RWMutex
	paths   map[string]pathcatalog.PathDefinition
	ready   bool
	readyCh chan struct{}
	target  targetOffsets
}

// targetOffsets is the per-partition "caught up" watermark captured once
// at startup (see newTargetOffsets), so Ready() reflects "has this
// consumer seen everything that existed in the topic at the moment it
// started", not "will it ever catch up to a topic that keeps growing".
type targetOffsets map[int]int64

// NewConsumer constructs a Consumer reading Topic from brokers under a
// fresh, PROCESS-UNIQUE consumer group, starting at the earliest offset.
// See the package doc comment for why the group must never be a fixed
// shared name.
func NewConsumer(ctx context.Context, brokers []string, logger *slog.Logger) (*Consumer, error) {
	return NewConsumerForTopic(ctx, brokers, Topic, logger)
}

// NewConsumerForTopic constructs the same full-replay consumer as NewConsumer,
// but against an explicit topic. It lets integration tests exercise production
// replay/readiness behavior against an isolated throwaway topic.
func NewConsumerForTopic(ctx context.Context, brokers []string, topic string, logger *slog.Logger) (*Consumer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	target, err := newTargetOffsets(ctx, brokers, topic)
	if err != nil {
		return nil, fmt.Errorf("kafkacatalog: determine readiness target: %w", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     uniqueConsumerGroup(),
		StartOffset: kafkago.FirstOffset,
	})

	c := &Consumer{
		Reader:  reader,
		Logger:  logger,
		paths:   make(map[string]pathcatalog.PathDefinition),
		readyCh: make(chan struct{}),
		target:  target,
	}
	if len(target) == 0 {
		// The topic has no partitions with any messages yet (a brand
		// new topic, or process-path-management has never published)
		// — there is nothing to catch up to, so this consumer is
		// trivially ready from the start.
		c.markReady()
	}
	return c, nil
}

// uniqueConsumerGroup builds a group id unique to this process instance
// (hostname + PID + a nanosecond timestamp) — see the package doc
// comment for why per-process uniqueness is a correctness requirement,
// not a cosmetic choice.
func uniqueConsumerGroup() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s-%s-%d-%d", consumerGroupPrefix, host, os.Getpid(), time.Now().UnixNano())
}

// newTargetOffsets dials Topic directly (no consumer group) and reads
// each partition's current last offset, returning only the partitions
// that actually have at least one message.
func newTargetOffsets(ctx context.Context, brokers []string, topic string) (targetOffsets, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafkacatalog: no brokers configured")
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafkacatalog: dial %s: %w", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("kafkacatalog: read partitions for %s: %w", topic, err)
	}

	out := make(targetOffsets, len(partitions))
	for _, p := range partitions {
		pconn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			return nil, fmt.Errorf("kafkacatalog: dial leader for partition %d: %w", p.ID, err)
		}
		first, last, err := pconn.ReadOffsets()
		closeErr := pconn.Close()
		if err != nil {
			return nil, fmt.Errorf("kafkacatalog: read offsets for partition %d: %w", p.ID, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("kafkacatalog: close leader conn for partition %d: %w", p.ID, closeErr)
		}
		if last > first {
			// last is exclusive (the offset of the NEXT message to be
			// written) — a consumer has caught up once it has
			// processed the message at offset last-1.
			out[p.ID] = last
		}
	}
	return out, nil
}

// Close releases the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.Reader.Close()
}

// Ready reports whether this consumer has processed every message that
// existed in Topic at the moment it started. Safe to call concurrently.
func (c *Consumer) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

// WaitReady blocks until Ready() would return true or ctx is done,
// whichever comes first. Used by the composition root to gate starting
// real work on catalogue completeness, mirroring the retired
// filecatalog.Load's "never process real work against an incomplete
// catalogue" guarantee.
func (c *Consumer) WaitReady(ctx context.Context) error {
	if c.Ready() {
		return nil
	}
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) markReady() {
	c.mu.Lock()
	alreadyReady := c.ready
	c.ready = true
	c.mu.Unlock()
	if !alreadyReady {
		close(c.readyCh)
	}
}

// Lookup satisfies ports.PathCatalogue against this consumer's current
// in-memory snapshot. Matching semantics are byte-for-byte identical to
// pathcatalog.Catalogue.Lookup — this consumer builds a real
// pathcatalog.Catalogue on every read rather than reimplementing the
// match rule, so the two can never drift.
func (c *Consumer) Lookup(id string) (pathcatalog.PathDefinition, error) {
	c.mu.RLock()
	defs := make([]pathcatalog.PathDefinition, 0, len(c.paths))
	for _, d := range c.paths {
		defs = append(defs, d)
	}
	c.mu.RUnlock()
	return pathcatalog.New(defs).Lookup(id)
}

// Ids returns every currently-known path's canonical id (ACTIVE only —
// a Deactivated path is removed from the local cache entirely).
func (c *Consumer) Ids() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.paths))
	for id := range c.paths {
		out = append(out, id)
	}
	return out
}

// Run consumes Topic until ctx is cancelled or the reader returns a
// fatal error. A handling error is logged and the loop continues, so one
// malformed message cannot wedge this consumer.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := c.handle(msg); errors.Is(err, cloudevents.ErrNotCloudEvent) {
			// Deterministic poison message (e.g. the retired flat
			// envelope): WARN and move past it, never parse a legacy shape.
			c.Logger.WarnContext(ctx, "skipping invalid CloudEvent on process-path catalogue topic",
				"topic", msg.Topic, "offset", msg.Offset, "partition", msg.Partition, "error", err)
		} else if err != nil {
			c.Logger.ErrorContext(ctx, "process-path catalogue message handling failed",
				"topic", msg.Topic, "offset", msg.Offset, "partition", msg.Partition, "error", err)
		}
		c.checkReady(msg)
	}
}

func (c *Consumer) checkReady(msg kafkago.Message) {
	c.mu.RLock()
	target, tracked := c.target[msg.Partition]
	alreadyReady := c.ready
	c.mu.RUnlock()
	if alreadyReady || !tracked {
		return
	}
	if msg.Offset+1 < target {
		return
	}

	c.mu.Lock()
	delete(c.target, msg.Partition)
	allCaughtUp := len(c.target) == 0
	c.mu.Unlock()

	if allCaughtUp {
		c.markReady()
	}
}

func (c *Consumer) handle(msg kafkago.Message) error {
	evt, err := cloudevents.Decode(msg.Value)
	if err != nil {
		return fmt.Errorf("kafkacatalog: %w", err)
	}

	switch evt.Type() {
	case eventTypeCreated, eventTypeUpdated:
		var data pathData
		if err := evt.DataAs(&data); err != nil {
			return fmt.Errorf("kafkacatalog: unmarshal %s data: %w", evt.Type(), err)
		}
		c.applyUpsert(data)
	case eventTypeDeactivated:
		var data pathData
		if err := evt.DataAs(&data); err != nil {
			return fmt.Errorf("kafkacatalog: unmarshal %s data: %w", evt.Type(), err)
		}
		c.applyDeactivated(data.PathId)
	default:
		// An event type outside this consumer's contract — ignored,
		// same convention as every other consumer in this fleet that
		// reads a shared/fan-out-shaped topic.
	}
	return nil
}

func (c *Consumer) applyUpsert(data pathData) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paths[strings.ToUpper(data.PathId)] = pathcatalog.PathDefinition{
		Id:                      data.PathId,
		MatchPrefix:             data.MatchPrefix,
		RequiredCapabilities:    data.RequiredCapabilities,
		DestinationLocationRole: data.DestinationLocationRole,
	}
}

func (c *Consumer) applyDeactivated(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.paths, strings.ToUpper(id))
}

// WaitReadyTimeout bounds how long the composition root waits for the
// initial replay before giving up and failing startup outright — a
// Kafka outage lasting longer than this is treated the same way the
// retired filecatalog.Load treated a missing file: fatal, not a silent
// partial-catalogue start.
const WaitReadyTimeout = 60 * time.Second
