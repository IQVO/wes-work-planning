package main

import (
	"testing"
	"time"
)

func TestRetentionEnv(t *testing.T) {
	const key = "WES_TEST_RETENTION"
	const fallback = 24 * time.Hour
	cases := []struct {
		name string
		set  bool
		val  string
		want time.Duration
	}{
		{"unset uses the fallback", false, "", fallback},
		{"empty uses the fallback", true, "", fallback},
		{"valid duration", true, "48h", 48 * time.Hour},
		{"explicit zero disables", true, "0", 0},
		{"explicit 0s disables", true, "0s", 0},
		{"negative uses the fallback", true, "-5h", fallback},
		{"garbage uses the fallback", true, "soon", fallback},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(key, tc.val)
			}
			if got := retentionEnv(key, fallback); got != tc.want {
				t.Fatalf("retentionEnv = %v, want %v", got, tc.want)
			}
		})
	}
}

// In-memory mode (no Postgres pool) has nothing to sweep.
func TestNewHousekeeper_NilWithoutPool(t *testing.T) {
	if h := newHousekeeper(nil, nil); h != nil {
		t.Fatalf("newHousekeeper(nil pool) = %v, want nil", h)
	}
	// With no housekeeper, start/stop is a harmless no-op.
	(&serving{}).startHousekeeper()()
}
