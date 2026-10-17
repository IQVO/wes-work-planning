import { useEffect, useRef, useState, type FormEvent } from "react";
import { WES_API_BASE } from "../config";
import { configurePool, toApiError, type ApiError } from "../api";
import { isPoolMode, validatePathId, validatePoolConfig } from "../validation";
import { POOL_MODES, type BacklogSnapshot, type PoolConfigResponse, type PoolMode } from "../types";
import { Card, StatusPill, useFetch } from "@warehouse/ui-kit";
import { FormRow, SelectField, SubmitButton, TextField, InlineSuccess } from "../components/formkit";
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

/**
 * Pool configuration -- the planner screen for PUT /paths/{pathId}/pool (the
 * ConfigurePool command, ADR-0034): a path's feed mode and WIP limit. The PUT
 * is a snapshot replace and deliberately sends NO Idempotency-Key (the
 * fleet's middleware excludes PUTs; an identical repeat writes nothing).
 *
 * Prefill: the remote's existing read view for a path is GET
 * /paths/{pathId}/telemetry (PlanningScreen's dashboard), so the mode select
 * seeds itself from the live pool's mode. No read exposes the current WIP
 * limit, so the limit is always typed -- the field's hint says so.
 */
export function PoolConfigScreen() {
  const [pathQuery, setPathQuery] = useState("");
  const [pathId, setPathId] = useState<string | null>(null);

  const [mode, setMode] = useState<PoolMode>("ReleaseFed");
  const [wipLimit, setWipLimit] = useState("");
  const modeTouched = useRef(false);

  const [saving, setSaving] = useState(false);
  const [errors, setErrors] = useState<string[]>([]);
  const [apiError, setApiError] = useState<ApiError | null>(null);
  const [configured, setConfigured] = useState<PoolConfigResponse | null>(null);

  // The existing read view: same telemetry fetch the dashboard renders, used
  // here to prefill the mode and show the pool's live occupancy.
  const telemetryUrl = pathId
    ? `${WES_API_BASE}/paths/${encodeURIComponent(pathId)}/telemetry`
    : null;
  const { data: telemetry, loading: telemetryLoading, error: telemetryError } =
    useFetch<BacklogSnapshot>(telemetryUrl);

  // Seed the mode select from the live pool once per looked-up path, unless
  // the planner already chose a mode by hand.
  useEffect(() => {
    if (!modeTouched.current && telemetry && isPoolMode(telemetry.mode)) {
      setMode(telemetry.mode);
    }
  }, [telemetry]);

  function onSubmitPath(e: FormEvent) {
    e.preventDefault();
    const trimmed = pathQuery.trim();
    if (trimmed) setPathId(trimmed);
  }

  function onModeChange(value: PoolMode) {
    modeTouched.current = true;
    setMode(value);
  }

  async function onSave(e: FormEvent) {
    e.preventDefault();
    if (!pathId) return;
    setErrors([]);
    setApiError(null);
    setConfigured(null);

    const path = validatePathId(pathId);
    const config = validatePoolConfig(mode, wipLimit);
    const clientErrors = [
      ...(path.ok ? [] : path.errors),
      ...(config.ok ? [] : config.errors),
    ];
    if (!path.ok || !config.ok) {
      setErrors(clientErrors);
      return;
    }

    setSaving(true);
    try {
      const response = await configurePool(path.value, config.value);
      setConfigured(response);
    } catch (err) {
      setApiError(toApiError(err));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <div>
        <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>Pool configuration</h1>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>
          wes-work-planning · PUT /paths/&#123;pathId&#125;/pool · flow-balance feed mode and WIP
          limit (ADR-0034)
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

      {pathId && (
        <Card
          title={`Current pool · ${pathId}`}
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
              Loading… (a path nobody configured yet has no pool — the telemetry read answers 404
              until work is enqueued)
            </div>
          )}
          {telemetryError && (
            <div style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
              No telemetry for this path yet ({telemetryError.message}); you can still configure
              the pool below — the PUT creates it when none exists.
            </div>
          )}
          {telemetry && (
            <Facts
              items={[
                { label: "Feed mode", value: telemetry.mode },
                { label: "WIP (live)", value: telemetry.wip },
                { label: "Backlog depth", value: telemetry.backlogDepth },
              ]}
            />
          )}
        </Card>
      )}

      <Card title="Configure">
        <form onSubmit={onSave} style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-4)" }}>
          <FormRow>
            <SelectField
              label="Feed mode"
              value={mode}
              options={POOL_MODES}
              onChange={onModeChange}
              required
              hint="ReleaseFed enforces the WIP limit; FlowFed stores it but only alarms."
            />
            <TextField
              label="WIP limit"
              type="number"
              value={wipLimit}
              onChange={setWipLimit}
              placeholder="e.g. 200"
              required
              hint="Positive integer. No read exposes the current limit — this PUT replaces the whole configuration. Lowering it below the live WIP never evicts work; releases pause instead."
            />
          </FormRow>
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
            <SubmitButton disabled={!pathId || saving}>
              {saving ? "Saving…" : "Save configuration"}
            </SubmitButton>
          </div>
          <p style={{ color: "var(--wh-color-text-faint)", fontSize: "var(--wh-font-size-xs)", margin: 0 }}>
            Snapshot replace: the same body always yields the same result. Sent without an
            Idempotency-Key (the idempotency middleware excludes PUTs).
          </p>
        </form>
      </Card>

      {configured && (
        <Card
          title={`Pool configured · ${configured.pathId}`}
          actions={<StatusPill status={configured.mode} />}
        >
          <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-4)" }}>
            <InlineSuccess message={`Saved. PUT /paths/${configured.pathId}/pool answered 200.`} />
            <Facts
              items={[
                { label: "Feed mode", value: configured.mode },
                { label: "WIP limit", value: configured.wipLimit },
                { label: "WIP (live)", value: configured.wip },
                { label: "Backlog depth", value: configured.backlogDepth },
              ]}
            />
            {configured.wip > configured.wipLimit && (
              <div style={{ color: "var(--wh-color-status-warning)", fontSize: "var(--wh-font-size-sm)" }}>
                WIP is above the new limit: nothing was evicted, releases answer 409
                wip-limit-reached until completions bring WIP strictly below it.
              </div>
            )}
          </div>
        </Card>
      )}
    </div>
  );
}
