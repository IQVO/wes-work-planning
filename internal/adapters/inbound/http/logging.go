package http

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// sanitizeForLog strips CR/LF from an attacker-controlled value (here,
// the raw request path, both the logged field and routePattern's 404
// fallback) before it is written to a log line. Without this, a crafted
// path segment containing an encoded newline could forge a fake log
// entry that appears to be a separate, legitimate line (CWE-117 log
// injection) once the log record reaches a downstream viewer/aggregator
// that doesn't preserve slog's JSON string escaping.
func sanitizeForLog(s string) string {
	return strings.NewReplacer("\n", "", "\r", "").Replace(s)
}

// RequestLogger returns chi middleware that logs each request at Info level
// (Error for 5xx) with method, route pattern, status, duration, response
// size, and request ID. It logs through the request context, so the
// telemetry slog handler can attach the trace_id/span_id of the span
// otelchi opened for this request.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()

			next.ServeHTTP(ww, r)

			attrs := []any{
				"method", r.Method,
				"path", sanitizeForLog(r.URL.Path),
				"route", routePattern(r),
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
				"bytes", ww.BytesWritten(),
				"request_id", middleware.GetReqID(r.Context()),
			}

			if ww.Status() >= http.StatusInternalServerError {
				logger.ErrorContext(r.Context(), "http request", attrs...)
			} else {
				logger.InfoContext(r.Context(), "http request", attrs...)
			}
		})
	}
}

// routePattern returns the matched chi route template (e.g.
// /paths/{pathId}/work-releases) rather than the raw path, keeping the log field
// low-cardinality and aligned with the span name otelchi produces. The
// fallback for an unmatched request is sanitized too -- it never went
// through a route's own value objects, so it's still attacker-controlled
// input at this point.
func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := rctx.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	return sanitizeForLog(r.URL.Path)
}
