package release

import (
	"errors"
	"testing"
)

func TestParseFeedMode(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    FeedMode
		wantErr error
	}{
		{"release fed", "ReleaseFed", ReleaseFed, nil},
		{"flow fed", "FlowFed", FlowFed, nil},
		{"empty", "", ReleaseFed, ErrUnknownFeedMode},
		{"unknown", "BatchFed", ReleaseFed, ErrUnknownFeedMode},
		{"unknown sentinel string", "Unknown", ReleaseFed, ErrUnknownFeedMode},
		{"wrong case", "flowfed", ReleaseFed, ErrUnknownFeedMode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFeedMode(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got error %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFeedMode_String_RoundTripsThroughParse(t *testing.T) {
	for _, m := range []FeedMode{ReleaseFed, FlowFed} {
		got, err := ParseFeedMode(m.String())
		if err != nil || got != m {
			t.Fatalf("ParseFeedMode(%q) = %v, %v; want %v, nil", m.String(), got, err, m)
		}
	}
	if got := FeedMode(99).String(); got != "Unknown" {
		t.Fatalf("got %q, want Unknown", got)
	}
}

func TestParseEntryState(t *testing.T) {
	tests := []struct {
		name         string
		in           string
		wantReleased bool
		wantComplete bool
		wantErr      error
	}{
		{"pending", "pending", false, false, nil},
		{"released", "released", true, false, nil},
		{"completed", "completed", true, true, nil},
		{"empty", "", false, false, ErrUnknownEntryState},
		{"unknown", "cancelled", false, false, ErrUnknownEntryState},
		{"wrong case", "Released", false, false, ErrUnknownEntryState},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isReleased, isCompleted, err := ParseEntryState(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got error %v, want %v", err, tt.wantErr)
			}
			if isReleased != tt.wantReleased || isCompleted != tt.wantComplete {
				t.Fatalf("got (released=%v, completed=%v), want (%v, %v)", isReleased, isCompleted, tt.wantReleased, tt.wantComplete)
			}
		})
	}
}
