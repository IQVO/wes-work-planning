-- ADR-0010: gift_wrap is a caller-stated WorkReleased characteristic. It was
-- carried on the WorkUnit aggregate but never persisted, so every unit read
-- back from Postgres (WorkReleased encoding, GET /work-units) reported false.
ALTER TABLE work_units ADD COLUMN gift_wrap BOOLEAN NOT NULL DEFAULT false;
