package main

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
)

// Boot errors of the mandatory configuration (ADR-0044, ADR-0046). Each names
// its variable: cmd/a2a reads and writes the same Postgres as cmd/wes,
// release_next_work publishes through the transactional outbox that cmd/wes's
// relay drains onto real Kafka, and the Agent Card must advertise a URL the
// fleet's agent clients can actually dial.
var (
	errNoDatabase = errors.New("DATABASE_URL is required: this context runs only on a real Postgres (ADR-0044), there is no in-memory mode")
	errNoBrokers  = errors.New("KAFKA_BROKERS is required: this context runs only on a real Kafka (ADR-0044), there is no log sink")
	errNoAgentURL = errors.New("A2A_AGENT_URL is required: the Agent Card must advertise the absolute URL of this agent's JSON-RPC endpoint (ADR-0046), e.g. http://<release>-a2a.<ns>.svc.cluster.local:8093/")
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

// checkAgentURL validates A2A_AGENT_URL: an absolute http(s) URL. The card is
// the discovery contract — a relative or schemeless URL is a misconfiguration
// every client would hit, so it is refused at boot, never silently served.
func checkAgentURL(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errNoAgentURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("A2A_AGENT_URL=%q is not an absolute http(s) URL: the Agent Card must advertise the endpoint clients dial (ADR-0046)", raw)
	}
	return nil
}

// validateBootConfig reads the mandatory environment of cmd/a2a through env
// and reports every problem at once, each naming its variable, so the process
// fails fast at boot, before it opens a connection or binds the listener.
func validateBootConfig(env func(string) string) error {
	var errs []error
	if strings.TrimSpace(env("DATABASE_URL")) == "" {
		errs = append(errs, errNoDatabase)
	}
	if len(brokerList(env("KAFKA_BROKERS"))) == 0 {
		errs = append(errs, errNoBrokers)
	}
	errs = append(errs, checkEventPublisher(env("EVENT_PUBLISHER")))
	errs = append(errs, checkAgentURL(env("A2A_AGENT_URL")))
	if _, err := productclassificationcopy.ParseMode(env("PRODUCT_CLASSIFICATION_MODE")); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	return nil
}
