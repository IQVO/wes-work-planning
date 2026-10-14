DROP TABLE IF EXISTS slot_plans;
DROP INDEX IF EXISTS idx_work_units_blocked_by;
DROP INDEX IF EXISTS idx_work_units_relocate_site_state;
ALTER TABLE work_units
    DROP COLUMN IF EXISTS cancelled_at,
    DROP COLUMN IF EXISTS blocked_by,
    DROP COLUMN IF EXISTS to_slot,
    DROP COLUMN IF EXISTS from_slot,
    DROP COLUMN IF EXISTS relocation_kind,
    DROP COLUMN IF EXISTS relocation_move_seq,
    DROP COLUMN IF EXISTS relocation_plan_id;
