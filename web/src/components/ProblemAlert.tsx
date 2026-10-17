import type { ApiError } from "../api";

/**
 * Renders a failed call's RFC 7807 problem verbatim: the problem's own
 * `title`, its HTTP `status`, and its `detail` word for word, plus the error
 * type slug and `instance` so the operator can point at the exact failing
 * request. Mirrors inbound-receiving's ProblemAlert.
 */
export function ProblemAlert({ error }: { error: ApiError }) {
  return (
    <div
      role="alert"
      style={{
        color: "var(--wh-color-status-danger)",
        background: "var(--wh-color-status-danger-bg)",
        borderRadius: "var(--wh-radius-md)",
        padding: "10px 14px",
        fontSize: "var(--wh-font-size-sm)",
        display: "flex",
        flexDirection: "column",
        gap: 4,
      }}
    >
      <div>
        <strong>{error.title}</strong>
        {error.status > 0 && <span style={{ opacity: 0.8 }}> (HTTP {error.status})</span>}
      </div>
      {error.detail && <div>{error.detail}</div>}
      {(error.slug || error.problem?.instance) && (
        <div style={{ fontFamily: "var(--wh-font-mono)", fontSize: "var(--wh-font-size-xs)", opacity: 0.85 }}>
          {error.slug && <span>{error.slug}</span>}
          {error.slug && error.problem?.instance && <span> · </span>}
          {error.problem?.instance && <span>{error.problem.instance}</span>}
        </div>
      )}
    </div>
  );
}
