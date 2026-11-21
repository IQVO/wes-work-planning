package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
)

// replayClass says what the Idempotency-Key middleware does with a problem
// answer when the same request is retried under the same key
// (docs/docs/adr/0050-every-problem-type-has-a-replay-class.md; the fleet rule
// .claude/rules/fleet/idempotency-and-outbox.md). It is declared ON the
// catalogue entry of each problem type, next to its slug and title; there is
// no default and no list of slugs in the middleware.
type replayClass string

const (
	// classFinal: the answer is a function of the request alone. Stored and
	// replayed.
	classFinal replayClass = "FINAL"
	// classOutcome: the command COMMITTED state and then answered an error
	// that must be remembered. Stored, and its writes commit with the stored
	// answer (one transaction).
	classOutcome replayClass = "OUTCOME"
	// classRetryable: nothing committed and the answer depends on state that
	// can change (an optimistic-lock conflict, a referenced fact not known
	// yet, an aggregate state not reached yet, a dependency not ready). Not
	// stored: the transaction rolls back, key row included, and a retry
	// executes again.
	classRetryable replayClass = "RETRYABLE"
)

// validReplayClass reports whether c is one of the three classes; "" is
// "unclassified".
func validReplayClass(c replayClass) bool {
	return c == classFinal || c == classOutcome || c == classRetryable
}

// problemType is one problem type of the catalogue as the tests and the ADR
// table see it.
type problemType struct {
	Slug   string
	Status int
	Class  replayClass
}

// standaloneProblem is a problem type written outside problemCatalog: the
// middlewares' and the 410-moved routes' own answers, the 5xx fallback, the
// readiness 503 and the reports reader's own answers (a read-only binary,
// never behind the middleware). Each still declares exactly one replay class
// (ADR 0050) so no problem type of the service is unclassified. Several share
// a catalogue slug (not-found, malformed-request-body) and are deduped by
// allProblemTypes.
type standaloneProblem struct {
	slug   string
	status int
	class  replayClass
}

// standaloneProblems are the problem types written through writeProblem (or
// writeGone) directly rather than through a typed error of problemCatalog.
var standaloneProblems = []standaloneProblem{
	{slug: "internal-error", status: http.StatusInternalServerError, class: classRetryable},
	{slug: "not-found", status: http.StatusNotFound, class: classRetryable},
	{slug: "malformed-request-body", status: http.StatusBadRequest, class: classFinal},
	{slug: "charge-forecast-moved", status: http.StatusGone, class: classFinal},
	{slug: "shift-plan-moved", status: http.StatusGone, class: classFinal},
	{slug: "dependency-unavailable", status: http.StatusServiceUnavailable, class: classRetryable},
	{slug: "invalid-report-query", status: http.StatusBadRequest, class: classFinal},
	{slug: "report-store-error", status: http.StatusInternalServerError, class: classRetryable},
}

// allProblemTypes is the whole catalogue — every typed error's problem (with
// the status statusFor gives it and the class its entry declares), then the
// standalone problems — one entry per (slug, status).
func allProblemTypes() []problemType {
	seen := map[string]bool{}
	var out []problemType
	add := func(slug string, status int, class replayClass) {
		key := slug + "/" + strconv.Itoa(status)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, problemType{Slug: slug, Status: status, Class: class})
	}
	for _, e := range problemCatalog {
		add(e.slug, statusFor(e.err), e.class)
	}
	for _, p := range standaloneProblems {
		add(p.slug, p.status, p.class)
	}
	return out
}

// classFor is the replay class of err: the class its catalogue entry
// declares, or classRetryable for the internal-error fallback (a 5xx is never
// stored, so the class is nominal).
func classFor(err error) replayClass {
	for _, e := range problemCatalog {
		if errors.Is(err, e.err) {
			return e.class
		}
	}
	return classRetryable
}

// replayMarker is implemented by the capturing writer the idempotency
// middleware hands the handler. The production error writer marks it with the
// class of the problem it answers.
type replayMarker interface {
	markReplayClass(replayClass)
}

// markReplayClass marks w when it is the middleware's capture; any other
// writer (a route without the middleware, a test recorder) has nothing to
// mark.
func markReplayClass(w http.ResponseWriter, class replayClass) {
	if m, ok := w.(replayMarker); ok {
		m.markReplayClass(class)
	}
}

// keyCapture is the recorder the idempotency middleware runs the handler
// against: an httptest.ResponseRecorder that also carries the replay class
// the production error writer marked on it (ADR 0050).
type keyCapture struct {
	*httptest.ResponseRecorder
	class replayClass
}

func (c *keyCapture) markReplayClass(class replayClass) { c.class = class }

// storable reports whether the captured answer is final for the request: a
// 2xx is stored, a 5xx never is, and every other answer is stored only when
// its problem type declares FINAL or OUTCOME. A refusal that declares no
// class (or a misspelt one) is released like RETRYABLE: a released refusal
// costs one more execution, a wrongly stored one pins a stale refusal for the
// TTL (the housekeeper keeps rows 24h).
func (c *keyCapture) storable() bool {
	switch {
	case c.Code >= http.StatusOK && c.Code < http.StatusMultipleChoices:
		return true
	case c.Code >= http.StatusInternalServerError:
		return false
	default:
		return c.class == classFinal || c.class == classOutcome
	}
}
