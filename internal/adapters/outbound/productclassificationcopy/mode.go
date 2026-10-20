package productclassificationcopy

import (
	"fmt"
	"strings"
)

// Mode is a PRODUCT_CLASSIFICATION_MODE value (ADR-0035, ADR-0044).
type Mode string

// ModeKafka reads the local copy fed by product-master's ProductClassified
// events. It is the only mode a binary runs in (ADR-0044): the variable keeps
// its name because infra and the docs set it, but "permissive" is rejected.
const ModeKafka Mode = "kafka"

// ParseMode resolves raw (case-insensitive, surrounding blanks ignored; empty
// means ModeKafka, the default). Every other value is a boot error that names
// the variable: "permissive" and "off" (ADR-0044: this context runs on real
// Kafka), "http" (the retired live lookup against inventory-storage, ADR-0035)
// and anything unknown, so a stale deployment fails loudly instead of silently
// degrading.
func ParseMode(raw string) (Mode, error) {
	switch v := strings.ToLower(strings.TrimSpace(raw)); v {
	case "", string(ModeKafka):
		return ModeKafka, nil
	case "http":
		return "", fmt.Errorf("PRODUCT_CLASSIFICATION_MODE=http was removed (ADR-0035): classification comes from product-master's events; unset it or set kafka (with PRODUCT_CLASSIFICATION_CONSUMER_GROUP)")
	case "permissive", "off":
		return "", fmt.Errorf("PRODUCT_CLASSIFICATION_MODE=%s is not supported: this context runs on real Kafka (ADR-0044); unset it or set kafka", v)
	default:
		return "", fmt.Errorf("PRODUCT_CLASSIFICATION_MODE=%q is not supported: this context runs on real Kafka, the only value is kafka (ADR-0044)", raw)
	}
}
