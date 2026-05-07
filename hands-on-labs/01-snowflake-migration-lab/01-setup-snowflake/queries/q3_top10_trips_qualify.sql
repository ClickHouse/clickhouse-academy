-- Q3: Top 10 Trips per Borough using QUALIFY
-- Migration note: For this lab, QUALIFY is treated as a dialect gap requiring a subquery rewrite.
--                 (ClickHouse does support QUALIFY, but the subquery pattern is portable across all SQL engines. https://clickhouse.com/docs/sql-reference/statements/select/qualify)

USE WAREHOUSE ANALYTICS_WH;
USE DATABASE NYC_TAXI_DB;

-- Snowflake version: QUALIFY filters window function inline
SELECT
    trip_id,
    pickup_at,
    pickup_borough,
    total_amount_usd,
    tip_amount_usd,
    trip_distance_miles,
    ROW_NUMBER() OVER (
        PARTITION BY pickup_borough
        ORDER BY total_amount_usd DESC
    )                                                               AS rank_in_borough
FROM ANALYTICS.FACT_TRIPS
WHERE pickup_at::DATE = CURRENT_DATE() - 1
QUALIFY rank_in_borough <= 10
ORDER BY pickup_borough, rank_in_borough;

-- ClickHouse equivalent: subquery rewrite (portable across all SQL engines)
-- SELECT trip_id, pickup_at, pickup_borough, total_amount_usd, tip_amount_usd, trip_distance_miles, rn AS rank_in_borough
-- FROM (
--     SELECT
--         trip_id, pickup_at, pickup_borough, total_amount_usd, tip_amount_usd, trip_distance_miles,
--         row_number() OVER (
--             PARTITION BY pickup_borough
--             ORDER BY total_amount_usd DESC
--         ) AS rn
--     FROM analytics.fact_trips
--     WHERE toDate(pickup_at) = today() - 1
-- )
-- WHERE rn <= 10
-- ORDER BY pickup_borough, rn;
