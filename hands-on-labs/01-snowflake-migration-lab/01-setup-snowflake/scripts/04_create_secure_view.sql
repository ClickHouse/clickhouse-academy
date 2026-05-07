-- ============================================================
-- Script 04: Create Secure View (Data Sharing Simulation)
-- Simulates Snowflake Data Sharing with an external consumer
-- In ClickHouse, reproduced using views or row-level security
-- ============================================================

USE DATABASE NYC_TAXI_DB;
USE SCHEMA ANALYTICS;

-- Secure view exposes only non-PII trip-level aggregates
-- A real data share would use Snowflake Secure Data Sharing
CREATE OR REPLACE SECURE VIEW ANALYTICS.SHARED_TRIP_SUMMARY
COMMENT = 'External consumer view — simulates Snowflake Data Share'
AS
SELECT
    DATE_TRUNC('day', pickup_at)            AS trip_date,
    pickup_borough,
    dropoff_borough,
    payment_type,
    COUNT(*)                                AS trip_count,
    ROUND(SUM(total_amount_usd), 2)         AS total_revenue,
    ROUND(AVG(trip_distance_miles), 2)      AS avg_distance_miles,
    ROUND(AVG(duration_minutes), 1)         AS avg_duration_minutes,
    ROUND(AVG(tip_amount_usd / NULLIF(fare_amount_usd, 0)), 3) AS avg_tip_rate
FROM ANALYTICS.FACT_TRIPS
WHERE
    pickup_at IS NOT NULL
    AND total_amount_usd > 0
GROUP BY 1, 2, 3, 4;

-- Grant ANALYST_ROLE access to the secure view
GRANT SELECT ON VIEW ANALYTICS.SHARED_TRIP_SUMMARY TO ROLE ANALYST_ROLE;
