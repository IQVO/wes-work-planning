package productclassificationcopy

import (
	"fmt"
	"strings"
)

// Mode is a PRODUCT_CLASSIFICATION_MODE value (ADR-0035).
type Mode string

const (
	// ModePermissive reads nothing: every SKU is Known=false. The default.
	ModePermissive Mode = "permissive"
	// ModeKafka reads the local copy fed by product-master's
	// ProductClassified events.
	ModeKafka Mode = "kafka"
)

// ParseMode resolves raw (case-insensitive, surrounding blanks ignored; empty
// means ModePermissive). Every other value is a boot error — including
// "http", the retired live lookup against inventory-storage, so a stale
// deployment fails loudly instead of silently degrading to permissive.
func ParseMode(raw string) (Mode, error) {
	switch v := strings.ToLower(strings.TrimSpace(raw)); v {
	case "", string(ModePermissive):
		return ModePermissive, nil
	case string(ModeKafka):
		return ModeKafka, nil
	case "http":
		return "", fmt.Errorf("PRODUCT_CLASSIFICATION_MODE=http was removed (ADR-0035): classification now comes from product-master's events; set PRODUCT_CLASSIFICATION_MODE=kafka (with PRODUCT_CLASSIFICATION_CONSUMER_GROUP) or permissive")
	default:
		return "", fmt.Errorf("PRODUCT_CLASSIFICATION_MODE=%q is not supported: use kafka or permissive (ADR-0035)", raw)
	}
}
