package productclassificationcopy_test

import (
	"strings"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/productclassificationcopy"
)

func TestParseMode(t *testing.T) {
	for _, c := range []struct {
		raw  string
		want productclassificationcopy.Mode
	}{
		{"", productclassificationcopy.ModePermissive},
		{"permissive", productclassificationcopy.ModePermissive},
		{" Permissive ", productclassificationcopy.ModePermissive},
		{"kafka", productclassificationcopy.ModeKafka},
		{"KAFKA", productclassificationcopy.ModeKafka},
	} {
		got, err := productclassificationcopy.ParseMode(c.raw)
		if err != nil || got != c.want {
			t.Fatalf("ParseMode(%q) = %q, %v; want %q", c.raw, got, err, c.want)
		}
	}
}

func TestParseMode_RejectsHTTPAndUnknownValues(t *testing.T) {
	_, err := productclassificationcopy.ParseMode("http")
	if err == nil || !strings.Contains(err.Error(), "http was removed") || !strings.Contains(err.Error(), "PRODUCT_CLASSIFICATION_CONSUMER_GROUP") {
		t.Fatalf("ParseMode(http) error = %v, want a removal error naming the replacement", err)
	}
	for _, raw := range []string{"HTTP", "rest", "permisive"} {
		if _, err := productclassificationcopy.ParseMode(raw); err == nil {
			t.Fatalf("ParseMode(%q) must be rejected", raw)
		}
	}
}
