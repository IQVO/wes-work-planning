package main

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/http"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
)

func TestBuildReplenishment_PermissiveByDefault(t *testing.T) {
	for _, mode := range []string{"", "permissive", " Permissive "} {
		w, err := buildReplenishment(replenishmentConfig{mode: mode}, quietLogger())
		if err != nil {
			t.Fatalf("mode %q: %v", mode, err)
		}
		if w.enabled {
			t.Fatalf("mode %q: wiring = %+v, want no consumer", mode, w)
		}
	}
}

func TestBuildReplenishment_KafkaEnablesTheConsumer(t *testing.T) {
	w, err := buildReplenishment(replenishmentConfig{mode: "KAFKA", groupID: "wes-replenishment", kafkaBrokers: "kafka:9092", hasDatabase: true}, quietLogger())
	if err != nil {
		t.Fatalf("buildReplenishment: %v", err)
	}
	if !w.enabled || w.groupID != "wes-replenishment" {
		t.Fatalf("wiring = %+v, want enabled with the group", w)
	}
}

func TestBuildReplenishment_RejectsAtBoot(t *testing.T) {
	cases := map[string]struct {
		cfg     replenishmentConfig
		wantErr string
	}{
		"unknown mode":             {replenishmentConfig{mode: "http", hasDatabase: true}, "REPLENISHMENT_MODE"},
		"kafka without the group":  {replenishmentConfig{mode: "kafka", kafkaBrokers: "kafka:9092", hasDatabase: true}, "REPLENISHMENT_CONSUMER_GROUP"},
		"kafka with a blank group": {replenishmentConfig{mode: "kafka", groupID: "  ", kafkaBrokers: "kafka:9092", hasDatabase: true}, "REPLENISHMENT_CONSUMER_GROUP"},
		"kafka without brokers":    {replenishmentConfig{mode: "kafka", groupID: "g", hasDatabase: true}, "KAFKA_BROKERS"},
		"kafka with blank brokers": {replenishmentConfig{mode: "kafka", groupID: "g", kafkaBrokers: " ", hasDatabase: true}, "KAFKA_BROKERS"},
		"kafka without Postgres":   {replenishmentConfig{mode: "kafka", groupID: "g", kafkaBrokers: "kafka:9092"}, "DATABASE_URL"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := buildReplenishment(c.cfg, quietLogger())
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want one mentioning %q", err, c.wantErr)
			}
		})
	}
}

func TestLoadReplenishmentConfig_ReadsTheEnvironment(t *testing.T) {
	t.Setenv("REPLENISHMENT_MODE", "kafka")
	t.Setenv("REPLENISHMENT_CONSUMER_GROUP", "g-1")
	cfg := loadReplenishmentConfig("b:9092", true)
	if cfg.mode != "kafka" || cfg.groupID != "g-1" || cfg.kafkaBrokers != "b:9092" || !cfg.hasDatabase {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestReplenishmentCPTHorizons_DefaultsAndOverrides(t *testing.T) {
	normal, urgent := replenishmentCPTHorizons()
	if normal != usecases.DefaultReplenishmentCPTHorizon || urgent != usecases.DefaultReplenishmentUrgentCPTHorizon {
		t.Fatalf("defaults = %v, %v", normal, urgent)
	}
	t.Setenv("REPLENISHMENT_CPT_HORIZON", "4h")
	t.Setenv("REPLENISHMENT_URGENT_CPT_HORIZON", "30m")
	normal, urgent = replenishmentCPTHorizons()
	if normal != 4*time.Hour || urgent != 30*time.Minute {
		t.Fatalf("overrides = %v, %v", normal, urgent)
	}
	t.Setenv("REPLENISHMENT_CPT_HORIZON", "garbage")
	t.Setenv("REPLENISHMENT_URGENT_CPT_HORIZON", "-1h")
	normal, urgent = replenishmentCPTHorizons()
	if normal != usecases.DefaultReplenishmentCPTHorizon || urgent != usecases.DefaultReplenishmentUrgentCPTHorizon {
		t.Fatalf("malformed values must fall back, got %v, %v", normal, urgent)
	}
}

func TestStartReplenishmentConsumer_NoOpWhenPermissive(t *testing.T) {
	s := &serving{logger: quietLogger()}
	s.startReplenishmentConsumer()
	if s.stopReplenishment != nil {
		t.Fatal("permissive mode must not start a consumer")
	}
	s.stopReplenishmentConsumer(context.Background()) // safe when never started
}

func TestStartReplenishmentConsumer_StartsAndStops(t *testing.T) {
	s := &serving{
		logger:       quietLogger(),
		kafkaBrokers: "127.0.0.1:1",
		replenishment: replenishmentWiring{
			enabled: true,
			groupID: "g",
		},
		repos: memoryRepositories(),
	}
	s.startReplenishmentConsumer()
	if s.stopReplenishment == nil {
		t.Fatal("kafka mode must start the replenishment consumer")
	}
	s.stopReplenishmentConsumerBounded()
	if s.stopReplenishment != nil {
		t.Fatal("stop must clear the handle so a second stop is a no-op")
	}
	s.stopReplenishmentConsumerBounded()
}

// ADR-0042 + ADR-0023 §8.3: the replenishment consumer is stopped and
// drained during graceful shutdown, after the classification consumer and
// before the catalogue consumer and the server.
func TestGracefulShutdown_StopsTheReplenishmentConsumerInOrder(t *testing.T) {
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
			order = append(order, "classification")
		},
		stopReplenishment: func(context.Context) {
			stopped.Store(true)
			order = append(order, "replenishment")
		},
	}
	consumerDone := make(chan struct{})
	close(consumerDone)
	relayDone := make(chan struct{})
	close(relayDone)
	if err := s.gracefulShutdown(func() { order = append(order, "integration") }, nil, consumerDone, func() {}, relayDone); err != nil {
		t.Fatalf("gracefulShutdown: %v", err)
	}
	want := "integration,classification,replenishment,catalogue"
	if !stopped.Load() || strings.Join(order, ",") != want {
		t.Fatalf("shutdown order = %v, want %s", order, want)
	}
}
