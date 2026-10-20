package main

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
)

// lazyPool is a pgx pool that never dials (pgxpool.New is lazy), so the wiring
// can be asserted without a database.
func lazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestBuildClassification_KafkaIsTheDefaultAndWiresThePostgresCopy(t *testing.T) {
	for _, mode := range []string{"", "kafka", " KAFKA "} {
		w, err := buildClassification(classificationConfig{mode: mode, groupID: "wes-work-planning-product-classification", kafkaBrokers: "kafka:9092"}, lazyPool(t), quietLogger())
		if err != nil {
			t.Fatalf("mode %q: buildClassification: %v", mode, err)
		}
		store, ok := w.lookup.(*productclassificationcopy.Store)
		if !ok || w.copies == nil || w.groupID != "wes-work-planning-product-classification" {
			t.Fatalf("mode %q: wiring = %+v, want the Postgres copy as lookup and copy, with the group", mode, w)
		}
		if any(w.copies) != any(store) {
			t.Fatal("the consumer must write the same copy the lookup reads")
		}
	}
}

func TestBuildClassification_RejectsAtBoot(t *testing.T) {
	cases := map[string]struct {
		cfg     classificationConfig
		wantErr string
	}{
		"permissive is rejected": {classificationConfig{mode: "permissive", groupID: "g", kafkaBrokers: "kafka:9092"}, "PRODUCT_CLASSIFICATION_MODE=permissive is not supported"},
		"off is rejected":        {classificationConfig{mode: "off", groupID: "g", kafkaBrokers: "kafka:9092"}, "PRODUCT_CLASSIFICATION_MODE=off is not supported"},
		"http is removed":        {classificationConfig{mode: "http"}, "http was removed"},
		"unknown mode":           {classificationConfig{mode: "rest"}, "not supported"},
		"without the group":      {classificationConfig{kafkaBrokers: "kafka:9092"}, "PRODUCT_CLASSIFICATION_CONSUMER_GROUP"},
		"with a blank group":     {classificationConfig{mode: "kafka", groupID: "  ", kafkaBrokers: "kafka:9092"}, "PRODUCT_CLASSIFICATION_CONSUMER_GROUP"},
		"without brokers":        {classificationConfig{mode: "kafka", groupID: "g"}, "KAFKA_BROKERS"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := buildClassification(c.cfg, lazyPool(t), quietLogger())
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want one mentioning %q", err, c.wantErr)
			}
		})
	}
}

func TestLoadClassificationConfig_ReadsTheEnvironment(t *testing.T) {
	t.Setenv("PRODUCT_CLASSIFICATION_MODE", "kafka")
	t.Setenv("PRODUCT_CLASSIFICATION_CONSUMER_GROUP", "g-1")
	cfg := loadClassificationConfig("b:9092")
	if cfg.mode != "kafka" || cfg.groupID != "g-1" || cfg.kafkaBrokers != "b:9092" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestStartClassificationConsumer_NoOpWithoutACopy(t *testing.T) {
	s := &serving{logger: quietLogger()}
	s.startClassificationConsumer()
	if s.stopClassification != nil {
		t.Fatal("a zero-value wiring must not start a consumer")
	}
	s.stopClassificationConsumer(context.Background()) // safe when never started
}

func TestStartClassificationConsumer_StartsAndStops(t *testing.T) {
	// The wiring is built by hand over the in-memory double: this test is
	// about the consumer lifecycle, not about the store.
	w := classificationWiring{copies: productclassificationcopy.NewMemoryStore(), groupID: "g"}
	s := &serving{logger: quietLogger(), kafkaBrokers: "127.0.0.1:1", classification: w, repos: memoryRepositories()}
	s.startClassificationConsumer()
	if s.stopClassification == nil {
		t.Fatal("kafka mode must start the ProductClassified consumer")
	}
	s.stopClassificationConsumerBounded()
	if s.stopClassification != nil {
		t.Fatal("stop must clear the handle so a second stop is a no-op")
	}
	s.stopClassificationConsumerBounded()
}

// ADR-0035 + ADR-0023 §8.3: the ProductClassified consumer is stopped and
// drained during graceful shutdown, after the integration consumer and
// before the catalogue consumer and the server.
func TestGracefulShutdown_StopsTheClassificationConsumerInOrder(t *testing.T) {
	var order []string
	var stopped atomic.Bool
	s := &serving{
		logger:   quietLogger(),
		server:   &http.Server{},
		handlers: &inboundhttp.Handlers{Readiness: &inboundhttp.Readiness{}},
		cancelCatalogueConsumer: func() {
			order = append(order, "catalogue")
		},
		stopClassification: func(context.Context) {
			stopped.Store(true)
			order = append(order, "classification")
		},
	}
	consumerDone := make(chan struct{})
	close(consumerDone)
	relayDone := make(chan struct{})
	close(relayDone)
	if err := s.gracefulShutdown(func() { order = append(order, "integration") }, nil, consumerDone, func() {}, relayDone); err != nil {
		t.Fatalf("gracefulShutdown: %v", err)
	}
	if !stopped.Load() || strings.Join(order, ",") != "integration,classification,catalogue" {
		t.Fatalf("shutdown order = %v, want integration,classification,catalogue", order)
	}
}
