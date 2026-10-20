package main

import (
	"strings"
	"testing"
)

// completeEnv sets every mandatory variable to a valid value.
func completeEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("KAFKA_CONSUMER_GROUP", "g-main")
	t.Setenv("EVENT_PUBLISHER", "")
	t.Setenv("PRODUCT_CLASSIFICATION_MODE", "")
	t.Setenv("PRODUCT_CLASSIFICATION_CONSUMER_GROUP", "g-classification")
	t.Setenv("REPLENISHMENT_MODE", "")
	t.Setenv("REPLENISHMENT_CONSUMER_GROUP", "g-replenishment")
}

func bootConfigError() error {
	cfg := loadConfig()
	return validateBootConfig(cfg, loadClassificationConfig(cfg.kafkaBrokers), loadReplenishmentConfig(cfg.kafkaBrokers))
}

func TestBootConfig_AcceptsTheCompleteEnvironment(t *testing.T) {
	completeEnv(t)
	if err := bootConfigError(); err != nil {
		t.Fatalf("complete environment rejected: %v", err)
	}
	// The modes keep their names: kafka is accepted explicitly too.
	t.Setenv("PRODUCT_CLASSIFICATION_MODE", "kafka")
	t.Setenv("REPLENISHMENT_MODE", "kafka")
	t.Setenv("EVENT_PUBLISHER", "kafka")
	if err := bootConfigError(); err != nil {
		t.Fatalf("explicit kafka modes rejected: %v", err)
	}
}

// Every missing or rejected value is a boot error that names the variable.
func TestBootConfig_RejectsAndNamesTheVariable(t *testing.T) {
	cases := []struct {
		name string
		set  map[string]string
		want string
	}{
		{"DATABASE_URL unset", map[string]string{"DATABASE_URL": ""}, "DATABASE_URL"},
		{"DATABASE_URL blank", map[string]string{"DATABASE_URL": "   "}, "DATABASE_URL"},
		{"KAFKA_BROKERS unset", map[string]string{"KAFKA_BROKERS": ""}, "KAFKA_BROKERS"},
		{"KAFKA_BROKERS only commas", map[string]string{"KAFKA_BROKERS": " , "}, "KAFKA_BROKERS"},
		{"KAFKA_CONSUMER_GROUP unset", map[string]string{"KAFKA_CONSUMER_GROUP": ""}, "KAFKA_CONSUMER_GROUP"},
		{"KAFKA_CONSUMER_GROUP blank", map[string]string{"KAFKA_CONSUMER_GROUP": "  "}, "KAFKA_CONSUMER_GROUP"},
		{"EVENT_PUBLISHER=log", map[string]string{"EVENT_PUBLISHER": "log"}, "EVENT_PUBLISHER"},
		{"PRODUCT_CLASSIFICATION_MODE=permissive", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "permissive"}, "PRODUCT_CLASSIFICATION_MODE"},
		{"PRODUCT_CLASSIFICATION_MODE=off", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "off"}, "PRODUCT_CLASSIFICATION_MODE"},
		{"PRODUCT_CLASSIFICATION_MODE=http", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "http"}, "PRODUCT_CLASSIFICATION_MODE"},
		{"PRODUCT_CLASSIFICATION_CONSUMER_GROUP unset", map[string]string{"PRODUCT_CLASSIFICATION_CONSUMER_GROUP": ""}, "PRODUCT_CLASSIFICATION_CONSUMER_GROUP"},
		{"REPLENISHMENT_MODE=permissive", map[string]string{"REPLENISHMENT_MODE": "permissive"}, "REPLENISHMENT_MODE"},
		{"REPLENISHMENT_MODE=off", map[string]string{"REPLENISHMENT_MODE": "off"}, "REPLENISHMENT_MODE"},
		{"REPLENISHMENT_MODE unknown", map[string]string{"REPLENISHMENT_MODE": "http"}, "REPLENISHMENT_MODE"},
		{"REPLENISHMENT_CONSUMER_GROUP unset", map[string]string{"REPLENISHMENT_CONSUMER_GROUP": ""}, "REPLENISHMENT_CONSUMER_GROUP"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			completeEnv(t)
			for k, v := range c.set {
				t.Setenv(k, v)
			}
			err := bootConfigError()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want one naming %q", err, c.want)
			}
		})
	}
}

// The Postgres adapters refuse a missing DSN themselves, before any dial.
func TestWireRepositories_RefusesAMissingDatabaseURL(t *testing.T) {
	for _, dsn := range []string{"", "  "} {
		_, err := wireRepositories(dsn, dsn, "migrations", quietLogger())
		if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
			t.Fatalf("wireRepositories(%q) err = %v, want one naming DATABASE_URL", dsn, err)
		}
	}
}

// There is no log sink and no direct-to-Kafka publisher any more.
func TestWireEventPublisher_RefusesAnythingButOutboxOverPostgres(t *testing.T) {
	if _, _, _, err := wireEventPublisher(quietLogger(), "kafka:9092", nil, nil, nil); err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("no pool: err = %v, want one naming DATABASE_URL", err)
	}
}

func TestBuildClassification_RefusesWithoutPostgres(t *testing.T) {
	_, err := buildClassification(classificationConfig{groupID: "g", kafkaBrokers: "kafka:9092"}, nil, quietLogger())
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("err = %v, want one naming DATABASE_URL", err)
	}
}

func TestMemoryRepositoriesFixtureHasNoPool(t *testing.T) {
	if r := memoryRepositories(); r.pgPool != nil || r.uow != nil {
		t.Fatal("the fixture must not pretend to have Postgres")
	}
}
