-- ============================================================
-- 02_validate_parity.sql
-- Manual parity validation queries for ClickHouse SQL console.
-- Run these after the Python migration script to confirm data integrity.
-- ============================================================
-- Connect to your ClickHouse Cloud service before running these queries.
-- First-run step: USE nyc_taxi_ch;

-- 1. Row count comparison (run both and compare)
-- In ClickHouse SQL console:
SELECT 'trips_raw' AS table_name, count() AS row_count
FROM nyc_taxi_ch.trips_raw;

-- In Snowflake (for reference):
-- SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.TRIPS_RAW;

-- 2. Date range check — should span 2019–2022
SELECT
    min(toDate(pickup_at)) AS earliest_trip,
    max(toDate(pickup_at)) AS latest_trip,
    count()                AS total_rows
FROM nyc_taxi_ch.trips_raw;

-- 3. trip_metadata JSON populated?
SELECT
    countIf(trip_metadata != '') AS with_metadata,
    countIf(trip_metadata  = '') AS empty_metadata,
    count()                      AS total
FROM nyc_taxi_ch.trips_raw;

-- 4. Driver rating spot check (should be ~1.0–5.0)
SELECT
    round(JSONExtractFloat(trip_metadata, 'driver', 'rating'), 1) AS rating,
    count() AS trips
FROM nyc_taxi_ch.trips_raw
WHERE JSONExtractFloat(trip_metadata, 'driver', 'rating') > 0
GROUP BY rating
ORDER BY rating;

-- 5. Borough distribution (after dbt run — fact_trips)
SELECT pickup_borough, count() AS trips
FROM analytics.fact_trips FINAL
GROUP BY pickup_borough
ORDER BY trips DESC;

-- OPTIONAL: Run only after CDC connector is active (Step 6 in setup.sh)
-- 6. CDC stream health — new trips appearing?
-- Run twice 5 minutes apart. The count should increase if CDC is running.
SELECT count() AS total_trips, max(ingested_at) AS latest_ingest
FROM nyc_taxi_ch.trips_raw;
