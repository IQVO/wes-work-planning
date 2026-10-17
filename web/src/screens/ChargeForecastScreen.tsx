import { useRef, useState, type FormEvent } from "react";
import { WES_API_BASE } from "../config";
import { newIdempotencyKey, receiveChargeForecast, toApiError, type ApiError } from "../api";
import { validateChargeBuckets, validatePathId } from "../validation";
import type { BacklogSnapshot, ChargeForecastResponse } from "../types";
import { Card, StatusPill, useFetch } from "@warehouse/ui-kit";
import { FormRow, SubmitButton, TextField, InlineSuccess } from "../components/formkit";
import { ProblemAlert } from "../components/ProblemAlert";

const pathInputStyle = {
  flex: 1,
  maxWidth: 360,
  padding: "10px 12px",
  borderRadius: "var(--wh-radius-md)",
  border: "1px solid var(--wh-color-border)",
  background: "var(--wh-color-bg-sunken)",
  color: "var(--wh-color-text)",
  fontFamily: "var(--wh-font-mono)",
  fontSize: "var(--wh-font-size-sm)",
} as const;

const buttonStyle = {
  padding: "10px 18px",
  borderRadius: "var(--wh-radius-md)",
  border: "none",
  background: "var(--wh-color-accent)",
  color: "#fff",
  fontWeight: 600,
  fontSize: "var(--wh-font-size-sm)",
  cursor: "pointer",
} as const;

const factsStyle = {
  display: "grid",
  gridTemplateColumns: "max-content 1fr",
  gap: "var(--wh-space-2) var(--wh-space-4)",
  margin: 0,
  fontSize: "var(--wh-font-size-sm)",
} as const;

function Facts({ items }: { items: { label: string; value: string | number }[] }) {
  return (
    <dl style={factsStyle}>
      {items.map((item) => (
        <div key={item.label} style={{ display: "contents" }}>
          <dt style={{ color: "var(--wh-color-text-muted)" }}>{item.label}</dt>
          <dd style={{ margin: 0 }}>{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}

interface BucketRow {
  cpt: string;
  quantity: string;
}

function emptyRow(): BucketRow {
  return { cpt: "", quantity: "" };
}

/**
 * Charge-forecast intake -- the planner screen for POST /paths/{pathId}/charge
 * (the ReceiveChargeForecast use case): the volume due by CPT for a process
 * path, one bucket per exact CPT timestamp. A manual forecast replaces any
 * existing forecast for the path.
 *
 * Idempotency (order-management's convention, ADR-0022 client side): every
 * submission carries a caller-minted Idempotency-Key (a v4 UUID). The key is
 * minted on the first submit and kept while the outcome is unknown (no answer
 * arrived), so a retry cannot double-record; it is dropped once the service
 * answers (success or problem) or when the form is edited -- an edited form is
 * a different request, and reusing a key with a different body is answered
 * 422 idempotency-key-reused where the store enforces keys.
 *
 * The intake's effect is visible in the telemetry read the remote already
 * renders (the forecast's volume flows into backlog/WIP once work is
 * enqueued and released against it) -- rendered below the form for the same
 * path; the Dashboard sub-nav carries the rebalance view.
 */
export function ChargeForecastScreen() {
  const [pathQuery, setPathQuery] = useState("");
  const [pathId, setPathId] = useState<string | null>(null);

  const [rows, setRows] = useState<BucketRow[]>([emptyRow()]);
  // Idempotency-Key of the CURRENT submission. Minted on the first click and
  // kept while the outcome is unknown; dropped on answer or on any form edit.
  const submissionKey = useRef<string | null>(null);

  const [submitting, setSubmitting] = useState(false);
  const [errors, setErrors] = useState<string[]>([]);
  const [apiError, setApiError] = useState<ApiError | null>(null);
  const [recorded, setRecorded] = useState<ChargeForecastResponse | null>(null);

  // The existing forecast-derived read view, same telemetry fetch the
  // dashboard renders -- the charge's volume surfaces here once work is
  // enqueued and released against it (no dedicated GET exists).
  const telemetryUrl = pathId
    ? `${WES_API_BASE}/paths/${encodeURIComponent(pathId)}/telemetry`
    : null;
  const { data: telemetry, loading: telemetryLoading, error: telemetryError } =
    useFetch<BacklogSnapshot>(telemetryUrl);

  function startNewSubmission() {
    submissionKey.current = null;
  }

  function updateRow(index: number, patch: Partial<BucketRow>) {
    startNewSubmission();
    setRows((prev) => prev.map((r, i) => (i === index ? { ...r, ...patch } : r)));
  }

  function addRow() {
    startNewSubmission();
    setRows((prev) => [...prev, emptyRow()]);
  }

  function removeRow(index: number) {
    startNewSubmission();
    setRows((prev) => (prev.length > 1 ? prev.filter((_, i) => i !== index) : prev));
  }

  function onSubmitPath(e: FormEvent) {
    e.preventDefault();
    const trimmed = pathQuery.trim();
    if (trimmed) setPathId(trimmed);
  }

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (!pathId) return;
    setErrors([]);
    setApiError(null);
    setRecorded(null);

    const path = validatePathId(pathId);
    const buckets = validateChargeBuckets(rows);
    const clientErrors = [
      ...(path.ok ? [] : path.errors),
      ...(buckets.ok ? [] : buckets.errors),
    ];
    if (!path.ok || !buckets.ok) {
      setErrors(clientErrors);
      return;
    }

    setSubmitting(true);
    const idempotencyKey = (submissionKey.current ??= newIdempotencyKey());
    try {
      const response = await receiveChargeForecast(
        path.value,
        { buckets: buckets.value },
        idempotencyKey,
      );
      // The service answered: that outcome is final, so a further submit is a
      // genuinely new forecast and must carry a fresh key.
      startNewSubmission();
      setRecorded(response);
      setRows([emptyRow()]);
    } catch (err) {
      const error = toApiError(err);
      if (error.status > 0) {
        // Answered (problem or not): final under this key -- a retry would
        // only replay it. A retry becomes a new submission.
        startNewSubmission();
      }
      // status 0: no answer arrived, the forecast may or may not exist. The
      // key is kept so the retry reuses it.
      setApiError(error);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <div>
        <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>Charge forecast intake</h1>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>
          wes-work-planning · POST /paths/&#123;pathId&#125;/charge · volume due by CPT, recorded
          manually by the planner
        </p>
      </div>

      <form onSubmit={onSubmitPath} style={{ display: "flex", gap: "var(--wh-space-2)" }}>
        <input
          value={pathQuery}
          onChange={(e) => setPathQuery(e.target.value)}
          placeholder="Path ID (e.g. pick-a)"
          style={pathInputStyle}
        />
        <button type="submit" style={buttonStyle}>
          Look up
        </button>
      </form>

      <Card title="Forecast buckets">
        <form onSubmit={onSubmit} style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-4)" }}>
          {rows.map((row, i) => (
            <FormRow key={i}>
              <TextField
                label={i === 0 ? `CPT (UTC)${rows.length > 1 ? " · bucket 1" : ""}` : `CPT (UTC) · bucket ${i + 1}`}
                type="datetime-local"
                value={row.cpt}
                onChange={(v) => updateRow(i, { cpt: v })}
                required
              />
              <TextField
                label={i === 0 ? `Quantity${rows.length > 1 ? " · bucket 1" : ""}` : `Quantity · bucket ${i + 1}`}
                type="number"
                value={row.quantity}
                onChange={(v) => updateRow(i, { quantity: v })}
                placeholder="e.g. 4200"
                required
                hint="Non-negative integer: units due by this CPT."
              />
              <SubmitButton type="button" onClick={() => removeRow(i)} disabled={rows.length <= 1}>
                Remove
              </SubmitButton>
            </FormRow>
          ))}
          <div>
            <SubmitButton type="button" onClick={addRow}>
              Add bucket
            </SubmitButton>
          </div>
          {errors.length > 0 && (
            <div
              role="alert"
              style={{
                color: "var(--wh-color-status-danger)",
                background: "var(--wh-color-status-danger-bg)",
                borderRadius: "var(--wh-radius-md)",
                padding: "8px 12px",
                fontSize: "var(--wh-font-size-sm)",
                display: "flex",
                flexDirection: "column",
                gap: 2,
              }}
            >
              {errors.map((error) => (
                <div key={error}>{error}</div>
              ))}
            </div>
          )}
          {apiError && <ProblemAlert error={apiError} />}
          <div>
            <SubmitButton disabled={!pathId || submitting}>
              {submitting ? "Submitting…" : "Submit forecast"}
            </SubmitButton>
          </div>
          <p style={{ color: "var(--wh-color-text-faint)", fontSize: "var(--wh-font-size-xs)", margin: 0 }}>
            Replaces any existing forecast for the path. Sent with a per-submission
            Idempotency-Key: a retry after a lost answer reuses the same key and can never
            double-record.
          </p>
        </form>
      </Card>

      {recorded && (
        <Card
          title={`Charge forecast recorded · ${recorded.pathId}`}
          actions={<StatusPill status={`${recorded.totalQuantity} units`} tone="success" />}
        >
          <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-4)" }}>
            <InlineSuccess message={`Recorded. POST /paths/${recorded.pathId}/charge answered 201.`} />
            <Facts
              items={[
                { label: "Total quantity", value: recorded.totalQuantity },
                { label: "Buckets", value: recorded.buckets.length },
                { label: "Received at", value: recorded.receivedAt },
              ]}
            />
            <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-2)" }}>
              {recorded.buckets.map((bucket) => (
                <div
                  key={bucket.cpt}
                  style={{
                    display: "flex",
                    gap: "var(--wh-space-5)",
                    fontSize: "var(--wh-font-size-sm)",
                    fontFamily: "var(--wh-font-mono)",
                  }}
                >
                  <span>{bucket.cpt}</span>
                  <span>{bucket.quantity}</span>
                </div>
              ))}
            </div>
          </div>
        </Card>
      )}

      {pathId && (
        <Card
          title={`Telemetry · ${pathId}`}
          actions={
            telemetry ? (
              <StatusPill
                status={telemetry.overAlarmThreshold ? "Over threshold" : "Normal"}
                tone={telemetry.overAlarmThreshold ? "warning" : "success"}
              />
            ) : undefined
          }
        >
          {telemetryLoading && !telemetry && !telemetryError && (
            <div style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
              Loading…
            </div>
          )}
          {telemetryError && (
            <div style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
              No telemetry for this path yet ({telemetryError.message}). The forecast's volume
              shows up here once work is enqueued and released against it.
            </div>
          )}
          {telemetry && (
            <Facts
              items={[
                { label: "Backlog depth", value: telemetry.backlogDepth },
                { label: "WIP", value: telemetry.wip },
                { label: "Mode", value: telemetry.mode },
              ]}
            />
          )}
        </Card>
      )}
    </div>
  );
}
