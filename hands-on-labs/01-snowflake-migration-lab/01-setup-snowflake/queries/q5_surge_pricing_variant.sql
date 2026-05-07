-- Q5: Surge Pricing Impact Analysis
-- Migration note: Colon-path VARIANT access → JSONExtractFloat in ClickHouse
--                 CASE/WHEN logic is identical

USE WAREHOUSE ANALYTICS_WH;
USE DATABASE NYC_TAXI_DB;

-- Snowflake version: colon-path VARIANT access
SELECT
    CASE
        WHEN TRIP_METADATA:app.surge_multiplier::FLOAT >= 2.0 THEN 'High Surge (2x+)'
        WHEN TRIP_METADATA:app.surge_multiplier::FLOAT >= 1.5 THEN 'Medium Surge (1.5–2x)'
        WHEN TRIP_METADATA:app.surge_multiplier::FLOAT > 1.0  THEN 'Low Surge (1–1.5x)'
        ELSE 'No Surge (1x)'
    END                                                             AS surge_category,
    COUNT(*)                                                        AS trip_count,
    ROUND(AVG(TOTAL_AMOUNT), 2)                                     AS avg_total_fare,
    ROUND(AVG(FARE_AMOUNT), 2)                                      AS avg_base_fare,
    ROUND(AVG(PASSENGER_COUNT), 1)                                  AS avg_passengers,
    ROUND(AVG(TRIP_DISTANCE), 2)                                    AS avg_distance_miles
FROM RAW.TRIPS_RAW
WHERE TRIP_METADATA:app.surge_multiplier IS NOT NULL
GROUP BY 1
ORDER BY AVG(TRIP_METADATA:app.surge_multiplier::FLOAT) DESC;

-- ClickHouse equivalent:
-- SELECT
--     CASE
--         WHEN JSONExtractFloat(trip_metadata, 'app', 'surge_multiplier') >= 2.0 THEN 'High Surge (2x+)'
--         WHEN JSONExtractFloat(trip_metadata, 'app', 'surge_multiplier') >= 1.5 THEN 'Medium Surge (1.5–2x)'
--         WHEN JSONExtractFloat(trip_metadata, 'app', 'surge_multiplier') > 1.0  THEN 'Low Surge (1–1.5x)'
--         ELSE 'No Surge (1x)'
--     END                                                           AS surge_category,
--     count()                                                       AS trip_count,
--     round(avg(total_amount), 2)                                   AS avg_total_fare
-- FROM raw.trips_raw
-- WHERE JSONHas(trip_metadata, 'app')
-- GROUP BY surge_category
-- ORDER BY avg(JSONExtractFloat(trip_metadata, 'app', 'surge_multiplier')) DESC;
