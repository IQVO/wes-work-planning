ALTER TABLE shift_plans
    DROP COLUMN IF EXISTS handle_cube_mm3,
    DROP COLUMN IF EXISTS handle_weight_g,
    DROP COLUMN IF EXISTS handle_unit;
ALTER TABLE work_units
    DROP COLUMN IF EXISTS case_break_pick_task_id,
    DROP COLUMN IF EXISTS case_break_loose_bin,
    DROP COLUMN IF EXISTS case_break_lpn,
    DROP COLUMN IF EXISTS pick_from_bin,
    DROP COLUMN IF EXISTS pick_unit;
DROP TABLE IF EXISTS product_pack_level_copy;
