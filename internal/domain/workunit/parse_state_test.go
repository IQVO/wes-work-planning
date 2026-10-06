package workunit

import (
	"errors"
	"testing"
)

func TestParseState(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    State
		wantErr error
	}{
		{"pending", "Pending", Pending, nil},
		{"released", "Released", Released, nil},
		{"completed", "Completed", Completed, nil},
		{"empty", "", Pending, ErrUnknownState},
		{"unknown", "Cancelled", Pending, ErrUnknownState},
		{"unknown sentinel string is not a valid stored state", "Unknown", Pending, ErrUnknownState},
		{"wrong case", "released", Pending, ErrUnknownState},
		{"surrounding space", " Released ", Pending, ErrUnknownState},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseState(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got error %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// Every state the aggregate can be in must round-trip through the persisted
// string form, or a legitimate row would start failing to rehydrate.
func TestParseState_RoundTripsEveryState(t *testing.T) {
	for _, s := range []State{Pending, Released, Completed} {
		got, err := ParseState(s.String())
		if err != nil || got != s {
			t.Fatalf("ParseState(%q) = %v, %v; want %v, nil", s.String(), got, err, s)
		}
	}
}
