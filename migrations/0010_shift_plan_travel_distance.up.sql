-- ADR-0017: optional travel-distance hint stamped on a PathPlan at commit time.
-- All three columns are NULL when no hint was recorded (no location codes
-- supplied, lookup permissive/unavailable, unknown or cross-zone pair).
-- travel_distance_m is NULL <=> hint not known (0 metres is a valid distance);
-- travel_distance_estimated is only meaningful when travel_distance_m is set.
ALTER TABLE shift_plans
    ADD COLUMN travel_distance_m         DOUBLE PRECISION,
    ADD COLUMN travel_distance_estimated BOOLEAN;
