-- ADR-0042: pick-face replenishment work. Additive only: every new work_units
-- column is NOT NULL with a zero-value default, so every existing row and
-- every writer that predates REPLENISH work keeps working (the 0003 / 0008 /
-- 0011 / 0014 discipline). The SKU and the quantity of a replenishment are
-- the unit's existing sku and quantity columns; a REPLENISH row is told apart
-- by work_kind = 'REPLENISH'.
ALTER TABLE work_units ADD COLUMN replenishment_request_id TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN replenishment_from_bin TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN replenishment_to_bin TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN replenishment_priority TEXT NOT NULL DEFAULT '';
