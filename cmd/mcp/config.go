package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
)

// Boot errors of the mandatory configuration (ADR-0044). Each names its
// variable: cmd/mcp reads and writes the same Postgres as cmd/wes, and
// release_next_work publishes through the transactional outbox that cmd/wes's
// relay drains onto real Kafka.
var (
	errNoDatabase = errors.New("DATABASE_URL is required: this context runs only on a real Postgres (ADR-0044), there is no in-memory mode")
	errNoBrokers  = errors.New("KAFKA_BROKERS is required: this context runs only on a real Kafka (ADR-0044), there is no log sink")
)

// checkEventPublisher validates EVENT_PUBLISHER: unset and "kafka" are
// accepted (the variable is kept because the chart and warehouse-infra set it),
// "log" and anything else is a boot error naming the variable.
func checkEventPublisher(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "kafka") {
		return nil
	}
	if strings.EqualFold(raw, "log") {
		return errors.New("EVENT_PUBLISHER=log is not supported: this context publishes to real Kafka (ADR-0044)")
	}
	return fmt.Errorf("EVENT_PUBLISHER=%q is not supported: this context publishes to real Kafka, the only value is kafka (ADR-0044)", raw)
}

// validateBootConfig reads the mandatory environment of cmd/mcp through env and
// reports every problem at once, each naming its variable, so the process fails
// fast at boot, before it opens a connection or binds the listener.
func validateBootConfig(env func(string) string) error {
	var errs []error
	if strings.TrimSpace(env("DATABASE_URL")) == "" {
		errs = append(errs, errNoDatabase)
	}
	if len(brokerList(env("KAFKA_BROKERS"))) == 0 {
		errs = append(errs, errNoBrokers)
	}
	errs = append(errs, checkEventPublisher(env("EVENT_PUBLISHER")))
	if _, err := productclassificationcopy.ParseMode(env("PRODUCT_CLASSIFICATION_MODE")); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	return nil
}
