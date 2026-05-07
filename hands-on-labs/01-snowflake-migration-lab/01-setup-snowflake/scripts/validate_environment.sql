-- ============================================================
-- validate_environment.sql
-- Run after full setup to confirm environment is healthy
-- All checks should return non-zero counts
-- ============================================================

USE WAREHOUSE ANALYTICS_WH;
USE DATABASE NYC_TAXI_DB;

-- 1. Schema presence
SELECT 'schemas' AS check_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE CATALOG_NAME = 'NYC_TAXI_DB'
  AND SCHEMA_NAME IN ('RAW', 'STAGING', 'ANALYTICS');

-- 2. Table presence
SELECT 'tables' AS check_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) >= 6 THEN 'PASS' ELSE 'FAIL' END AS status
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'NYC_TAXI_DB';

-- 3. Raw data volume
SELECT 'trips_raw_rows' AS check_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) > 1000000 THEN 'PASS' ELSE 'FAIL — data seeding may have failed' END AS status
FROM RAW.TRIPS_RAW;

-- 4. VARIANT column populated
SELECT 'variant_metadata' AS check_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM RAW.TRIPS_RAW
WHERE TRIP_METADATA IS NOT NULL
LIMIT 1;

-- 5. Dimension tables
SELECT 'dim_payment_type' AS check_name, COUNT(*) AS count,
    CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ANALYTICS.DIM_PAYMENT_TYPE
UNION ALL
SELECT 'dim_vendor', COUNT(*),
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END
FROM ANALYTICS.DIM_VENDOR
UNION ALL
SELECT 'dim_taxi_zones', COUNT(*),
    CASE WHEN COUNT(*) > 200 THEN 'PASS' ELSE 'FAIL — zone data not loaded' END
FROM ANALYTICS.DIM_TAXI_ZONES;

-- 6. dbt models (run after dbt run)
SELECT 'fact_trips' AS check_name, COUNT(*) AS count,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL — run dbt first' END AS status
FROM ANALYTICS.FACT_TRIPS
UNION ALL
SELECT 'dim_date', COUNT(*),
    CASE WHEN COUNT(*) > 3000 THEN 'PASS' ELSE 'FAIL' END
FROM ANALYTICS.DIM_DATE
UNION ALL
SELECT 'agg_hourly_zone_trips', COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
FROM ANALYTICS.AGG_HOURLY_ZONE_TRIPS;

-- 7. Stream existence (INFORMATION_SCHEMA has no STREAMS view; use SHOW + RESULT_SCAN)
SHOW STREAMS LIKE 'TRIPS_CDC_STREAM' IN SCHEMA NYC_TAXI_DB.RAW;
SELECT 'trips_cdc_stream' AS check_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL — run 03_create_streams_tasks.sql' END AS status
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- 8. Sample query sanity check
SELECT 'sample_query' AS check_name,
    COUNT(DISTINCT pickup_borough) AS distinct_boroughs,
    CASE WHEN COUNT(DISTINCT pickup_borough) >= 5 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ANALYTICS.FACT_TRIPS
LIMIT 1000000;
