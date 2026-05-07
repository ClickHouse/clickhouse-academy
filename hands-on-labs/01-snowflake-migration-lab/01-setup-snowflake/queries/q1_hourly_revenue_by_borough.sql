-- Q1: Hourly Revenue by Borough
-- Used by: Operations dashboard, runs every 15 minutes
-- Migration note: DATE_TRUNC is supported in ClickHouse with same syntax
--                 NULLIF works in ClickHouse too
--                 DATEADD → use pickup_at >= now() - INTERVAL 7 DAY in ClickHouse

USE WAREHOUSE ANALYTICS_WH;
USE DATABASE NYC_TAXI_DB;

SELECT
    DATE_TRUNC('hour', pickup_at)                                   AS hour_bucket,
    pickup_borough,
    COUNT(*)                                                        AS trip_count,
    SUM(total_amount_usd)                                           AS total_revenue,
    AVG(tip_amount_usd / NULLIF(fare_amount_usd, 0))               AS avg_tip_rate,
    AVG(trip_distance_miles)                                        AS avg_distance_miles
FROM ANALYTICS.FACT_TRIPS
WHERE pickup_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND pickup_borough IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, total_revenue DESC;

-- ClickHouse equivalent:
-- SELECT
--     toStartOfHour(pickup_at)                                      AS hour_bucket,
--     pickup_borough,
--     count()                                                       AS trip_count,
--     sum(total_amount_usd)                                         AS total_revenue,
--     avg(tip_amount_usd / nullIf(fare_amount_usd, 0))             AS avg_tip_rate,
--     avg(trip_distance_miles)                                      AS avg_distance_miles
-- FROM analytics.fact_trips
-- WHERE pickup_at >= now() - INTERVAL 7 DAY
--   AND pickup_borough != ''
-- GROUP BY hour_bucket, pickup_borough
-- ORDER BY hour_bucket DESC, total_revenue DESC;
