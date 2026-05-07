-- ════════════════════════════════════════════════════════════════════════════
-- Custom test: assert_trips_not_future
-- Verifies no trip has a pickup_at timestamp in the future.
--
-- A PASSING test returns 0 rows. Any rows returned = test failure.
--
-- Migration note: Identical business logic to Part 1 (Snowflake).
-- ClickHouse dialect: now() replaces CURRENT_TIMESTAMP() — same semantics.
-- This test replaces Part 1's dbt_expectations range test on pickup_at.
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    trip_id,
    pickup_at,
    now() AS current_time
FROM {{ ref('fact_trips') }} FINAL
-- FINAL forces synchronous deduplication — required for ReplacingMergeTree correctness
WHERE pickup_at > now()
