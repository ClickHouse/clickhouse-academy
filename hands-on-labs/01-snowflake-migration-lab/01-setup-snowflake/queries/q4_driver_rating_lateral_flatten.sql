-- Q4: Driver Rating Distribution from VARIANT Column
-- Migration note: LATERAL FLATTEN has no direct equivalent in ClickHouse.
--                 Options: (1) use JSONExtract functions on the raw JSON column
--                          (2) pre-flatten during migration (preferred for performance)

USE WAREHOUSE ANALYTICS_WH;
USE DATABASE NYC_TAXI_DB;

-- Snowflake version: LATERAL FLATTEN on VARIANT
SELECT
    ROUND(TRIP_METADATA:driver.rating::FLOAT, 1)                   AS rating_bucket,
    COUNT(*)                                                        AS trip_count,
    AVG(TOTAL_AMOUNT)                                               AS avg_fare,
    AVG(DATEDIFF('minute', PICKUP_DATETIME, DROPOFF_DATETIME))      AS avg_duration_minutes
FROM RAW.TRIPS_RAW
WHERE TRIP_METADATA:driver IS NOT NULL
  AND TRIP_METADATA:driver.rating IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ClickHouse equivalent (using JSONExtractFloat on String/JSON column):
-- SELECT
--     round(JSONExtractFloat(trip_metadata, 'driver', 'rating'), 1)  AS rating_bucket,
--     count()                                                         AS trip_count,
--     avg(total_amount)                                               AS avg_fare,
--     avg(dateDiff('minute', pickup_datetime, dropoff_datetime))      AS avg_duration_minutes
-- FROM raw.trips_raw
-- WHERE JSONHas(trip_metadata, 'driver')
--   AND JSONExtractFloat(trip_metadata, 'driver', 'rating') > 0
-- GROUP BY rating_bucket
-- ORDER BY rating_bucket;
--
-- OR if pre-flattened into typed columns (recommended):
-- SELECT round(driver_rating, 1) AS rating_bucket, count(), avg(total_amount_usd), avg(duration_minutes)
-- FROM analytics.fact_trips
-- WHERE driver_rating > 0
-- GROUP BY rating_bucket ORDER BY rating_bucket;
