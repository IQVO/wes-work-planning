-- ADR-0033: additive transfer metadata on work_units. Every column is
-- NOT NULL with a safe zero-value default, so every existing
-- order-driven row — and every INSERT that does not know about transfer
-- work — keeps working unchanged (the same discipline as 0003 sku /
-- 0008 gift_wrap).
ALTER TABLE work_units ADD COLUMN transfer_ref TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN work_kind TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN site_id TEXT NOT NULL DEFAULT '';
ALTER TABLE work_units ADD COLUMN quantity INTEGER NOT NULL DEFAULT 0;
