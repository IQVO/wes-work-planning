-- ADR-0045: pack-aware work. Additive only (the 0003 / 0008 / 0011 / 0014 / 0015
-- discipline): every existing row and every writer that predates it keeps working.

-- 1. Local copy of product-master's pack levels (ProductPackLevelDeclared /
-- Retired / Measured on warehouse.product-master.events), keyed by 14-digit GTIN
-- (a GTIN identifies exactly one (sku, level) fleet-wide). A message is applied
-- only when its version is greater than the stored one (the version is the
-- product aggregate's, so it only grows). A retired level keeps its row with
-- effective_to set. Read by the work-unit enqueue, the charge forecast and the
-- shift plan through ports.ProductClassificationLookup (Each / Case facts).
CREATE TABLE product_pack_level_copy (
    gtin             TEXT PRIMARY KEY CHECK (gtin ~ '^[0-9]{14}$'),
    sku              TEXT NOT NULL,
    level            TEXT NOT NULL CHECK (level IN ('EACH', 'INNER', 'CASE', 'PALLET')),
    child_gtin       TEXT NULL CHECK (child_gtin IS NULL OR child_gtin ~ '^[0-9]{14}$'),
    qty_of_child     INTEGER NULL,
    units_per_pack   INTEGER NOT NULL CHECK (units_per_pack >= 1),
    length_mm        INTEGER NULL,
    width_mm         INTEGER NULL,
    height_mm        INTEGER NULL,
    weight_g         INTEGER NULL,
    measured_length_mm INTEGER NULL,
    measured_width_mm  INTEGER NULL,
    measured_height_mm INTEGER NULL,
    measured_weight_g  INTEGER NULL,
    is_orderable     BOOLEAN NOT NULL,
    is_despatch_unit BOOLEAN NOT NULL,
    effective_from   TIMESTAMPTZ NOT NULL,
    effective_to     TIMESTAMPTZ NULL,
    version          BIGINT NOT NULL CHECK (version >= 1),
    updated_at       TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_product_pack_level_copy_active
    ON product_pack_level_copy (sku, level) WHERE effective_to IS NULL;

-- 2. Pack-aware work units: the pick plan (CASE_PICK / EACH_PICK; the SKU and the
-- quantity in EACHES are the unit's existing sku and quantity columns, the kind is
-- work_kind) and the sealed-case break (BREAK_CASE).
ALTER TABLE work_units ADD COLUMN pick_unit TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN pick_from_bin TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN case_break_lpn TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN case_break_loose_bin TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN case_break_pick_task_id TEXT NOT NULL DEFAULT '';

-- 3. Travel estimate: the load of ONE handle of the pick unit a shift plan staffs
-- (one case for CASE_PICK). NULL handle_unit = none recorded; NULL weight / cube =
-- unknown (never zero-filled).
ALTER TABLE shift_plans ADD COLUMN handle_unit TEXT NULL;
ALTER TABLE shift_plans ADD COLUMN handle_weight_g BIGINT NULL;
ALTER TABLE shift_plans ADD COLUMN handle_cube_mm3 BIGINT NULL;
