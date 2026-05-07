-- ════════════════════════════════════════════════════════════════════════════
-- Custom test: assert_revenue_positive
-- Verifies all fare amounts are non-negative (no refunds or negative fares).
--
-- A PASSING test returns 0 rows. Any rows returned = test failure.
--
-- Migration note: In Snowflake Part 1, this was covered by dbt_expectations:
--   dbt_expectations.expect_column_values_to_be_between:
--     min_value: 0
--     max_value: 1000
-- dbt_expectations is not compatible with dbt-clickhouse, so this custom SQL
-- test replaces it. The max_value bound is intentionally omitted — unusually
-- high fares are valid data (e.g., long airport trips with surge pricing).
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    trip_id,
    total_amount_usd,
    fare_amount_usd
FROM {{ ref('fact_trips') }} FINAL
-- FINAL forces synchronous deduplication — required for ReplacingMergeTree correctness
WHERE total_amount_usd < 0
   OR fare_amount_usd  < 0
