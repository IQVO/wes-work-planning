-- ADR-0036: the order line number a work unit was created for, stored
-- explicitly (it was only embedded in the "<order>-line-<n>" id). Nullable
-- and additive: NULL means "unknown" (every row that predates this column,
-- every transfer unit, every REST-enqueued unit that gave none), so every
-- existing INSERT keeps working unchanged.
ALTER TABLE work_units ADD COLUMN line_no INTEGER;
