import { useEffect, useState, type FormEvent } from "react";
import { fetchWorkUnitsByReference, toApiError, type ApiError } from "../api";
import { WES_API_BASE } from "../config";
import type { BacklogSnapshot, RebalanceRecommendation, WorkUnit } from "../types";
import { Card, StatusPill, DataTable, useFetch } from "@warehouse/ui-kit";

const inputStyle = {
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

const statLabelStyle = {
  fontSize: "var(--wh-font-size-xs)",
  color: "var(--wh-color-text-faint)",
  textTransform: "uppercase",
  letterSpacing: "0.04em",
  fontWeight: 600,
} as const;

const statValueStyle = {
  fontSize: "var(--wh-font-size-2xl)",
  fontWeight: 600,
} as const;

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div>
      <div style={statLabelStyle}>{label}</div>
      <div style={statValueStyle}>{value}</div>
    </div>
  );
}

/**
 * Path telemetry/rebalance dashboard + work-unit-by-reference search, in
 * one screen (wes-work-planning does not yet expose a paths list endpoint,
 * so -- same pattern as order-mgmt-mfe's OrdersScreen -- this is scoped to
 * what actually exists today: look up one path id and/or one reference at
 * a time; a fleet-wide paths overview is a fast-follow, not a blocker).
 */
export function PlanningScreen() {
  const [pathQuery, setPathQuery] = useState("");
  const [pathId, setPathId] = useState<string | null>(null);

  const [referenceQuery, setReferenceQuery] = useState("");
  const [reference, setReference] = useState<string | null>(null);

  const telemetryUrl = pathId
    ? `${WES_API_BASE}/paths/${encodeURIComponent(pathId)}/telemetry`
    : null;
  const { data: telemetry, loading: telemetryLoading, error: telemetryError } =
    useFetch<BacklogSnapshot>(telemetryUrl);

  const rebalanceUrl = pathId
    ? `${WES_API_BASE}/paths/${encodeURIComponent(pathId)}/rebalance`
    : null;
  const { data: rebalance, loading: rebalanceLoading, error: rebalanceError } =
    useFetch<RebalanceRecommendation>(rebalanceUrl);

  const [workUnits, setWorkUnits] = useState<WorkUnit[] | null>(null);
  const [workUnitsLoading, setWorkUnitsLoading] = useState(false);
  const [workUnitsError, setWorkUnitsError] = useState<ApiError | null>(null);
  useEffect(() => {
    if (!reference) return;
    let cancelled = false;
    setWorkUnitsLoading(true);
    setWorkUnitsError(null);
    fetchWorkUnitsByReference(reference)
      .then((rows) => !cancelled && setWorkUnits(rows))
      .catch((err) => !cancelled && setWorkUnitsError(toApiError(err)))
      .finally(() => !cancelled && setWorkUnitsLoading(false));
    return () => {
      cancelled = true;
    };
  }, [reference]);

  function onSubmitPath(e: FormEvent) {
    e.preventDefault();
    const trimmed = pathQuery.trim();
    if (trimmed) setPathId(trimmed);
  }

  function onSubmitReference(e: FormEvent) {
    e.preventDefault();
    const trimmed = referenceQuery.trim();
    if (trimmed) setReference(trimmed);
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <div>
        <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>Planning</h1>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>
          wes-work-planning · continuous release, flow balancing, work-pool telemetry
        </p>
      </div>

      <form onSubmit={onSubmitPath} style={{ display: "flex", gap: "var(--wh-space-2)" }}>
        <input
          value={pathQuery}
          onChange={(e) => setPathQuery(e.target.value)}
          placeholder="Path ID (e.g. pick-a)"
          style={inputStyle}
        />
        <button type="submit" style={buttonStyle}>
          Look up
        </button>
      </form>

      {telemetryError && (
        <Card>
          <div style={{ color: "var(--wh-color-status-danger)" }}>{telemetryError.message}</div>
        </Card>
      )}

      {pathId && (telemetry || telemetryLoading) && !telemetryError && (
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
          {telemetryLoading && !telemetry ? (
            <div style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
              Loading…
            </div>
          ) : (
            telemetry && (
              <div style={{ display: "flex", gap: "var(--wh-space-6)" }}>
                <Stat label="Backlog depth" value={telemetry.backlogDepth} />
                <Stat label="WIP" value={telemetry.wip} />
                <Stat label="Mode" value={telemetry.mode} />
              </div>
            )
          )}
        </Card>
      )}

      {rebalanceError && (
        <Card>
          <div style={{ color: "var(--wh-color-status-danger)" }}>{rebalanceError.message}</div>
        </Card>
      )}

      {pathId && (rebalance || rebalanceLoading) && !rebalanceError && (
        <Card
          title={`Rebalance recommendation · ${pathId}`}
          actions={rebalance ? <StatusPill status={rebalance.action} /> : undefined}
        >
          {rebalanceLoading && !rebalance ? (
            <div style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
              Loading…
            </div>
          ) : (
            rebalance && (
              <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-4)" }}>
                <div style={{ display: "flex", gap: "var(--wh-space-6)" }}>
                  <Stat label="Backlog depth" value={rebalance.backlogDepth} />
                  <Stat label="WIP" value={rebalance.wip} />
                </div>
                {rebalance.laborPlan && (
                  <div
                    style={{
                      borderTop: "1px solid var(--wh-color-border-subtle)",
                      paddingTop: "var(--wh-space-4)",
                      display: "flex",
                      gap: "var(--wh-space-6)",
                      fontSize: "var(--wh-font-size-sm)",
                      color: "var(--wh-color-text-muted)",
                    }}
                  >
                    <span>Planned heads: {rebalance.laborPlan.plannedHeads}</span>
                    <span>Planned rate: {rebalance.laborPlan.plannedRate}/hr</span>
                    <span>Planned hours: {rebalance.laborPlan.plannedHours}</span>
                  </div>
                )}
              </div>
            )
          )}
        </Card>
      )}

      <div style={{ borderTop: "1px solid var(--wh-color-border-subtle)", paddingTop: "var(--wh-space-5)" }}>
        <h2 style={{ fontSize: "var(--wh-font-size-lg)", margin: "0 0 var(--wh-space-3)" }}>
          Work units by reference
        </h2>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 0, marginBottom: "var(--wh-space-3)" }}>
          Look up every work unit enqueued against an order-line reference (cross-service lookup for order-management).
        </p>

        <form onSubmit={onSubmitReference} style={{ display: "flex", gap: "var(--wh-space-2)" }}>
          <input
            value={referenceQuery}
            onChange={(e) => setReferenceQuery(e.target.value)}
            placeholder="Reference (e.g. order-line-1)"
            style={inputStyle}
          />
          <button type="submit" style={buttonStyle}>
            Search
          </button>
        </form>

        {workUnitsError && (
          <Card padded={false}>
            <div style={{ padding: "var(--wh-space-5)", color: "var(--wh-color-status-danger)" }}>
              {workUnitsError.message}
            </div>
          </Card>
        )}

        {reference && !workUnitsError && (
          <div style={{ marginTop: "var(--wh-space-4)" }}>
            <Card padded={false}>
              <DataTable
                rowKey={(u) => u.id}
                rows={workUnits ?? []}
                loading={workUnitsLoading}
                emptyState={<span>No work units found for reference &ldquo;{reference}&rdquo;.</span>}
                columns={[
                  { key: "id", header: "Work unit ID", render: (u) => u.id },
                  { key: "pathId", header: "Path", render: (u) => u.pathId },
                  { key: "cpt", header: "CPT", render: (u) => u.cpt },
                  { key: "sku", header: "SKU", render: (u) => u.sku ?? "—" },
                  { key: "giftWrap", header: "Gift wrap", render: (u) => (u.giftWrap ? "Yes" : "") },
                  {
                    key: "state",
                    header: "State",
                    render: (u) => <StatusPill status={u.state} size="sm" />,
                  },
                  { key: "releasedAt", header: "Released", render: (u) => u.releasedAt ?? "—" },
                  { key: "completedAt", header: "Completed", render: (u) => u.completedAt ?? "—" },
                ]}
              />
            </Card>
          </div>
        )}
      </div>
    </div>
  );
}
