-- ADR-0019: outcome of reconciling our committed PathPlan against the labor
-- plan Workforce committed. Both columns are NULL until a comparison has been
-- computed for the path (no committed PathPlan yet = nothing to compare).
-- drift_heads is signed (observed - ours) and may be 0 (the plans agree);
-- drift_detected_at is set only when drift_heads <> 0.
ALTER TABLE labor_plan_view
    ADD COLUMN drift_heads       INTEGER,
    ADD COLUMN drift_detected_at TIMESTAMPTZ;
