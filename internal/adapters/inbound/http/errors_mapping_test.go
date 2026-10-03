package http

import (
	"errors"
	"fmt"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
)

// A WorkPool optimistic-concurrency conflict that outlived the use case's
// retry budget is a transient 409 the caller can retry, never a 500.
// Observed live: POST /paths/pick/release answered 500 "concurrent
// modification" under a busy floor.
func TestWriteError_ConcurrentModificationIs409(t *testing.T) {
	err := fmt.Errorf("release: %w", ports.ErrConcurrentModification)
	if got := statusFor(err); got != nethttp.StatusConflict {
		t.Fatalf("statusFor = %d, want 409", got)
	}
	rr := httptest.NewRecorder()
	writeError(rr, httptest.NewRequest(nethttp.MethodPost, "/paths/pick/release", nil), err)
	if rr.Code != nethttp.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "/concurrent-modification") {
		t.Fatalf("problem type missing: %s", rr.Body.String())
	}
	if errors.Is(err, ports.ErrNotFound) {
		t.Fatal("sanity")
	}
}
