package productclassificationcopy_test

import (
	"strings"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
)

func TestParseMode(t *testing.T) {
	for _, raw := range []string{"", "  ", "kafka", "KAFKA", " Kafka "} {
		got, err := productclassificationcopy.ParseMode(raw)
		if err != nil || got != productclassificationcopy.ModeKafka {
			t.Fatalf("ParseMode(%q) = %q, %v; want kafka", raw, got, err)
		}
	}
}

func TestParseMode_RejectsPermissiveOffHTTPAndUnknownValues(t *testing.T) {
	for _, raw := range []string{"permissive", " Permissive ", "off", "OFF"} {
		_, err := productclassificationcopy.ParseMode(raw)
		if err == nil || !strings.Contains(err.Error(), "PRODUCT_CLASSIFICATION_MODE") || !strings.Contains(err.Error(), "real Kafka") {
			t.Fatalf("ParseMode(%q) error = %v, want one naming PRODUCT_CLASSIFICATION_MODE and real Kafka", raw, err)
		}
	}
	_, err := productclassificationcopy.ParseMode("http")
	if err == nil || !strings.Contains(err.Error(), "http was removed") {
		t.Fatalf("ParseMode(http) error = %v, want a removal error", err)
	}
	for _, raw := range []string{"HTTP", "rest", "permisive"} {
		if _, err := productclassificationcopy.ParseMode(raw); err == nil {
			t.Fatalf("ParseMode(%q) must be rejected", raw)
		}
	}
}
