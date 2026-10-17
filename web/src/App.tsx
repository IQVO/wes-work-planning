import { NavLink, Outlet, Route, Routes } from "react-router-dom";
import { PlanningScreen } from "./screens/PlanningScreen";
import { PoolConfigScreen } from "./screens/PoolConfigScreen";
import { ChargeForecastScreen } from "./screens/ChargeForecastScreen";

const SUB_NAV = [
  { to: ".", label: "Dashboard", end: true },
  { to: "pool", label: "Pool configuration", end: true },
  { to: "charge", label: "Charge forecast", end: true },
];

const linkStyle = ({ isActive }: { isActive: boolean }) => ({
  display: "inline-flex",
  padding: "6px 12px",
  borderRadius: "var(--wh-radius-pill)",
  fontSize: "var(--wh-font-size-sm)",
  fontWeight: isActive ? 600 : 500,
  color: isActive ? "var(--wh-color-text)" : "var(--wh-color-text-muted)",
  background: isActive ? "var(--wh-color-accent-muted)" : "transparent",
  textDecoration: "none",
});

/** The sub-nav, rendered by a layout route with the path "/".
 *
 *  The console mounts this component inside its own `<Route
 *  path="/planning/*">`. A relative `<NavLink>` rendered straight in that
 *  splat route resolves against the FULL current URL (react-router 7 uses the
 *  last path-contributing match's pathname, splat included), so from
 *  /planning/charge the link `pool` would point at /planning/charge/pool. A
 *  pathless layout route does not help (react-router drops pathless routes
 *  from relative resolution); a layout route WITH path "/" does: its own
 *  match is the mount point, under any prefix and standalone at "/".
 *  (Same fix as the inbound-receiving remote.) */
function PlanningLayout() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <nav aria-label="Planning sections" style={{ display: "flex", gap: "var(--wh-space-2)" }}>
        {SUB_NAV.map((item) => (
          <NavLink key={item.to} to={item.to} end={item.end} style={linkStyle}>
            {item.label}
          </NavLink>
        ))}
      </nav>
      <Outlet />
    </div>
  );
}

/** Exposed as planning_mfe/App via Module Federation. Routed under
 *  /planning/* by the shell -- uses relative routes so this component
 *  works identically mounted under a prefix (in the shell) or at /
 *  (standalone dev, see main.tsx).
 *
 *  - (index): the existing telemetry/rebalance dashboard + work-unit search.
 *  - pool: configure a path's work pool (PUT /paths/{pathId}/pool).
 *  - charge: the planner's manual charge-forecast intake (POST
 *    /paths/{pathId}/charge, Idempotency-Key per submission). */
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<PlanningLayout />}>
        <Route index element={<PlanningScreen />} />
        <Route path="pool" element={<PoolConfigScreen />} />
        <Route path="charge" element={<ChargeForecastScreen />} />
      </Route>
    </Routes>
  );
}
