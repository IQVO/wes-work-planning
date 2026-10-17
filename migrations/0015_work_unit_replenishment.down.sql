ALTER TABLE work_units
    DROP COLUMN IF EXISTS replenishment_priority,
    DROP COLUMN IF EXISTS replenishment_to_bin,
    DROP COLUMN IF EXISTS replenishment_from_bin,
    DROP COLUMN IF EXISTS replenishment_request_id;
