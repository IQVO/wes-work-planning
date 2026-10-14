package main

import (
	"testing"
	"time"
)

// ADR-0040: RELOCATION_CPT_HORIZON is a Go duration; unset, malformed or
// non-positive values fall back to 24h and never fail the boot.
func TestRelocationCPTHorizon(t *testing.T) {
	cases := []struct {
		value string
		want  time.Duration
	}{
		{"", 24 * time.Hour},
		{"6h", 6 * time.Hour},
		{"90m", 90 * time.Minute},
		{"soon", 24 * time.Hour},
		{"0s", 24 * time.Hour},
		{"-1h", 24 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(relocationCPTHorizonEnv, tc.value)
			if got := relocationCPTHorizon(); got != tc.want {
				t.Fatalf("relocationCPTHorizon(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
