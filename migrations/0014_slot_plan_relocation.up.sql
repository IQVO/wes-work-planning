-- ADR-0040: slot-plan relocation work. Additive only: every new work_units
-- column is NOT NULL with a zero-value default (or nullable), so every
-- existing row and every writer that predates RELOCATE work keeps working
-- (the 0003 / 0008 / 0011 discipline).
ALTER TABLE work_units ADD COLUMN relocation_plan_id TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN relocation_move_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE work_units ADD COLUMN relocation_kind TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN from_slot TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN to_slot TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN blocked_by TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN cancelled_at TIMESTAMPTZ;

-- Supersession reads a site's Pending RELOCATE units; completion reads the
-- units waiting on the completed one. Both are partial: order work never
-- enters either index.
CREATE INDEX idx_work_units_relocate_site_state ON work_units (site_id, state) WHERE work_kind = 'RELOCATE';
CREATE INDEX idx_work_units_blocked_by ON work_units (blocked_by) WHERE blocked_by <> '';

-- Every approved slot plan this context applied (or found stale), so a newer
-- plan supersedes an older one whatever order the events arrive in.
CREATE TABLE slot_plans (
    plan_id            TEXT PRIMARY KEY,
    site_id            TEXT NOT NULL,
    approved_at        TIMESTAMPTZ NOT NULL,
    supersedes_plan_id TEXT NOT NULL DEFAULT '',
    move_count         INTEGER NOT NULL,
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_slot_plans_site_approved ON slot_plans (site_id, approved_at DESC, plan_id DESC);
