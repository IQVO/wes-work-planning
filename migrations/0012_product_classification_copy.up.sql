-- ADR-0035: this context's local copy of product-master's ProductClassified
-- events (warehouse.product-master.events). One row per SKU, overwritten
-- only by a message whose version is greater than the stored one; there is
-- no "unclassify" event in v1, so rows are never deleted. Read by the
-- WorkReleased encoder through ports.ProductClassificationLookup.
CREATE TABLE product_classification_copy (
    sku               TEXT PRIMARY KEY,
    handling_tags     TEXT[] NOT NULL DEFAULT '{}',
    temperature_class TEXT NULL,
    dot_hazard_class  SMALLINT NULL,
    version           BIGINT NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL
);
