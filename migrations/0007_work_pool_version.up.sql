-- Optimistic concurrency for the WorkPool aggregate. WorkPoolRepo.Save
-- rewrites every entry of a pool; without a version guard two concurrent
-- saves (a release and an enqueue, or two releases) each wrote back the
-- pool they loaded, silently reverting the other's change -- observed live
-- as work units Completed in work_units but still 'pending' in
-- work_pool_entries, wedging every later release on that path with
-- 409 work-unit-already-released.
ALTER TABLE work_pools ADD COLUMN version BIGINT NOT NULL DEFAULT 1;
