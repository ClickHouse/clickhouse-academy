-- Q2: Rolling 7-Day Average Trip Distance
-- Used by: Executive weekly report
-- Migration note: Window frame syntax (ROWS BETWEEN) is nearly identical in ClickHouse
--                 Nested aggregate window function (AVG(AVG(...))) works in ClickHouse too

USE WAREHOUSE ANALYTICS_WH;
USE DATABASE NYC_TAXI_DB;

SELECT
    pickup_at::DATE                                                 AS trip_date,
    COUNT(*)                                                        AS daily_trip_count,
    AVG(trip_distance_miles)                                        AS daily_avg_distance,
    AVG(AVG(trip_distance_miles)) OVER (
        ORDER BY pickup_at::DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                                               AS rolling_7d_avg_distance,
    SUM(total_amount_usd)                                           AS daily_revenue,
    SUM(SUM(total_amount_usd)) OVER (
        ORDER BY pickup_at::DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                                               AS rolling_7d_revenue
FROM ANALYTICS.FACT_TRIPS
GROUP BY 1
ORDER BY 1 DESC
LIMIT 365;

-- ClickHouse equivalent (syntax nearly identical):
-- SELECT
--     toDate(pickup_at)                                             AS trip_date,
--     count()                                                       AS daily_trip_count,
--     avg(trip_distance_miles)                                      AS daily_avg_distance,
--     avg(avg(trip_distance_miles)) OVER (
--         ORDER BY trip_date
--         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
--     )                                                             AS rolling_7d_avg_distance
-- FROM analytics.fact_trips
-- GROUP BY trip_date
-- ORDER BY trip_date DESC
-- LIMIT 365;
