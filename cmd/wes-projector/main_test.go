package main

import (
	"errors"
	"testing"
)

func TestRequireBrokers(t *testing.T) {
	got, err := requireBrokers(" a:1, ,b:2 ,")
	if err != nil || len(got) != 2 || got[0] != "a:1" || got[1] != "b:2" {
		t.Fatalf("requireBrokers = %v, %v", got, err)
	}
	for _, raw := range []string{"", "   ", " , "} {
		if _, err := requireBrokers(raw); !errors.Is(err, errMissingBrokers) {
			t.Fatalf("requireBrokers(%q) err = %v, want errMissingBrokers (no localhost:9092 default)", raw, err)
		}
	}
}

// The error names the variable.
func TestMissingConfigErrorsNameTheirVariable(t *testing.T) {
	for _, tc := range []struct {
		err  error
		name string
	}{{errMissingBrokers, "KAFKA_BROKERS"}, {errMissingAnalyticsURL, "ANALYTICS_DATABASE_URL"}} {
		if got := tc.err.Error(); len(got) < len(tc.name) || got[:len(tc.name)] != tc.name {
			t.Fatalf("%q must start by naming %s", got, tc.name)
		}
	}
}
