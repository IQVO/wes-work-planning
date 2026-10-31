package productclassificationcopy

import "testing"

func TestSanitizeLog(t *testing.T) {
	if got := sanitizeLog("a\r\nb\nc"); got != "abc" {
		t.Fatalf("sanitizeLog = %q, want abc", got)
	}
}
