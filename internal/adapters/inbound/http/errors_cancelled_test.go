package http

import (
	"fmt"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0040: a Cancelled unit (a superseded slot plan's move) refused by a
// lifecycle transition is a 409 work-unit-cancelled, never a 500.
func TestWriteError_CancelledWorkUnitIs409(t *testing.T) {
	err := fmt.Errorf("release: %w", workunit.ErrCancelled)
	if got := statusFor(err); got != nethttp.StatusConflict {
		t.Fatalf("statusFor = %d, want 409", got)
	}
	rr := httptest.NewRecorder()
	writeError(rr, httptest.NewRequest(nethttp.MethodPost, "/paths/RELOCATE/release", nil), err)
	if rr.Code != nethttp.StatusConflict || !strings.Contains(rr.Body.String(), "/work-unit-cancelled") {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
}
