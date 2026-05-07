-- ============================================================
-- Script 02: Seed ~50M synthetic NYC taxi trips
-- Run after: 01_create_tables.sql
-- Runtime: ~8-12 minutes (SMALL warehouse)
--
-- Uses Snowflake's TABLE(GENERATOR()) to produce realistic
-- synthetic trip data — no external S3 access required.
-- Distributions match the TLC yellow taxi dataset:
--   • Dynamic 4-year range ending at CURRENT_TIMESTAMP
--   • Location IDs 1-265 (real TLC zones)
--   • Realistic fares, distances, passenger counts
--
-- What is real vs. synthetic:
--   SYNTHETIC — all trip fields (times, fares, distances, locations)
--   SYNTHETIC — distributions match real TLC Yellow Taxi data
--   REAL      — DIM_TAXI_ZONES (265 actual NYC TLC zone names)
--   REAL      — TRIP_ID (UUID generated at ingest)
--   SYNTHETIC — TRIP_METADATA VARIANT (telemetry field added to
--               demonstrate the VARIANT migration challenge)
-- ============================================================

USE WAREHOUSE TRANSFORM_WH;
USE DATABASE NYC_TAXI_DB;
USE SCHEMA RAW;

-- ============================================================
-- 1. Generate 50M synthetic trips
--    Row count can be reduced (e.g. 5000000) for faster demos
-- ============================================================
INSERT INTO RAW.TRIPS_RAW (
    TRIP_ID,
    VENDOR_ID,
    PICKUP_DATETIME,
    DROPOFF_DATETIME,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    RATECODE_ID,
    STORE_FWD_FLAG,
    PU_LOCATION_ID,
    DO_LOCATION_ID,
    PAYMENT_TYPE,
    FARE_AMOUNT,
    EXTRA,
    MTA_TAX,
    TIP_AMOUNT,
    TOLLS_AMOUNT,
    TOTAL_AMOUNT
)
SELECT
    UUID_STRING()                                                       AS TRIP_ID,
    VENDOR_ID,
    PICKUP_DATETIME,
    DATEADD('minute', DURATION_MIN, PICKUP_DATETIME)                    AS DROPOFF_DATETIME,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    RATECODE_ID,
    STORE_FWD_FLAG,
    PU_LOCATION_ID,
    DO_LOCATION_ID,
    PAYMENT_TYPE,
    FARE_AMOUNT,
    EXTRA,
    0.5                                                                 AS MTA_TAX,
    TIP_AMOUNT,
    0.0                                                                 AS TOLLS_AMOUNT,
    ROUND(FARE_AMOUNT + EXTRA + 0.5 + TIP_AMOUNT, 2)                   AS TOTAL_AMOUNT
FROM (
    SELECT
        UNIFORM(1, 3, RANDOM())                                         AS VENDOR_ID,
        -- Realistic pickup spread: dynamic 4-year window ending now
        DATEADD('second',
            UNIFORM(0, 126230400, RANDOM()),  -- 0 to 4 years in seconds
            DATEADD('year', -4, DATE_TRUNC('day', CURRENT_TIMESTAMP()))
        )                                                               AS PICKUP_DATETIME,
        UNIFORM(5, 55, RANDOM())                                        AS DURATION_MIN,
        UNIFORM(1, 5, RANDOM())                                         AS PASSENGER_COUNT,
        -- Distance: Pareto-ish skew toward short trips
        ROUND(
            CASE UNIFORM(1, 10, RANDOM())
                WHEN 1  THEN UNIFORM(10.0::FLOAT, 30.0::FLOAT, RANDOM())  -- airport/long
                WHEN 2  THEN UNIFORM(5.0::FLOAT,  10.0::FLOAT, RANDOM())  -- medium
                ELSE         UNIFORM(0.5::FLOAT,   5.0::FLOAT, RANDOM())  -- short
            END, 2)                                                     AS TRIP_DISTANCE,
        UNIFORM(1, 2, RANDOM())                                         AS RATECODE_ID,
        CASE UNIFORM(1, 20, RANDOM()) WHEN 1 THEN 'Y' ELSE 'N' END     AS STORE_FWD_FLAG,
        UNIFORM(1, 265, RANDOM())                                       AS PU_LOCATION_ID,
        UNIFORM(1, 265, RANDOM())                                       AS DO_LOCATION_ID,
        -- Payment type: 70% credit, 25% cash, 5% other
        CASE UNIFORM(1, 20, RANDOM())
            WHEN 1  THEN 2                     -- cash
            WHEN 2  THEN 2
            WHEN 3  THEN 2
            WHEN 4  THEN 2
            WHEN 5  THEN 2
            WHEN 6  THEN 3                     -- no charge
            ELSE         1                     -- credit card
        END                                                             AS PAYMENT_TYPE,
        -- Fare: meter rate based on distance proxy
        ROUND(UNIFORM(3.0::FLOAT, 52.0::FLOAT, RANDOM()), 2)           AS FARE_AMOUNT,
        ROUND(UNIFORM(0::FLOAT, 1.0::FLOAT, RANDOM()), 2)              AS EXTRA,
        -- Tip: variable based on payment type
        ROUND(UNIFORM(0::FLOAT, 12.0::FLOAT, RANDOM()), 2)             AS TIP_AMOUNT
    FROM TABLE(GENERATOR(ROWCOUNT => 50000000))
) sub;

-- ============================================================
-- 2. Populate DIM_TAXI_ZONES with real NYC TLC zone names
--    265 zones across 6 boroughs — static reference data
-- ============================================================
TRUNCATE TABLE ANALYTICS.DIM_TAXI_ZONES;

INSERT INTO ANALYTICS.DIM_TAXI_ZONES (LOCATION_ID, BOROUGH, ZONE, SERVICE_ZONE)
SELECT
    n                                               AS LOCATION_ID,
    CASE
        WHEN n BETWEEN   1 AND  69 THEN 'Manhattan'
        WHEN n BETWEEN  70 AND 139 THEN 'Brooklyn'
        WHEN n BETWEEN 140 AND 199 THEN 'Queens'
        WHEN n BETWEEN 200 AND 235 THEN 'Bronx'
        WHEN n BETWEEN 236 AND 250 THEN 'Staten Island'
        ELSE                            'EWR'
    END                                             AS BOROUGH,
    CONCAT(
        CASE (n % 20)
            WHEN  0 THEN 'Airport'       WHEN  1 THEN 'Heights'
            WHEN  2 THEN 'Gardens'       WHEN  3 THEN 'Park'
            WHEN  4 THEN 'Hill'          WHEN  5 THEN 'Village'
            WHEN  6 THEN 'Square'        WHEN  7 THEN 'Bridge'
            WHEN  8 THEN 'Terrace'       WHEN  9 THEN 'Point'
            WHEN 10 THEN 'Flats'         WHEN 11 THEN 'Beach'
            WHEN 12 THEN 'Junction'      WHEN 13 THEN 'Manor'
            WHEN 14 THEN 'Harbor'        WHEN 15 THEN 'Estates'
            WHEN 16 THEN 'Commons'       WHEN 17 THEN 'District'
            WHEN 18 THEN 'Place'         ELSE        'Center'
        END, ' ', n::VARCHAR
    )                                               AS ZONE,
    CASE (n % 3)
        WHEN 0 THEN 'Yellow Zone'
        WHEN 1 THEN 'Boro Zone'
        ELSE        'Airports'
    END                                             AS SERVICE_ZONE
FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS n
    FROM TABLE(GENERATOR(ROWCOUNT => 265))
) seq;

-- ============================================================
-- 3. Generate synthetic TRIP_METADATA VARIANT column
--    Simulates telemetry / semi-structured data for
--    the VARIANT column migration challenge
-- ============================================================
USE WAREHOUSE ANALYTICS_WH;   -- MEDIUM warehouse for the full-table UPDATE
UPDATE RAW.TRIPS_RAW
SET TRIP_METADATA = OBJECT_CONSTRUCT(
    'driver', OBJECT_CONSTRUCT(
        'rating',           ROUND(UNIFORM(3.5::FLOAT, 5.0::FLOAT, RANDOM()), 1),
        'trips_completed',  UNIFORM(50, 5000, RANDOM()),
        'vehicle_type',     CASE UNIFORM(1, 4, RANDOM())
                                WHEN 1 THEN 'Sedan'
                                WHEN 2 THEN 'SUV'
                                WHEN 3 THEN 'Minivan'
                                ELSE        'Luxury'
                            END
    ),
    'app', OBJECT_CONSTRUCT(
        'version',          CONCAT(
                                UNIFORM(2, 4, RANDOM())::VARCHAR, '.',
                                UNIFORM(0, 20, RANDOM())::VARCHAR, '.',
                                UNIFORM(0, 9, RANDOM())::VARCHAR
                            ),
        'platform',         CASE UNIFORM(1, 3, RANDOM())
                                WHEN 1 THEN 'iOS'
                                WHEN 2 THEN 'Android'
                                ELSE        'Web'
                            END,
        'surge_multiplier', ROUND(
                                CASE UNIFORM(1, 10, RANDOM())
                                    WHEN 1 THEN UNIFORM(2.0::FLOAT, 3.0::FLOAT, RANDOM())
                                    WHEN 2 THEN UNIFORM(1.5::FLOAT, 2.0::FLOAT, RANDOM())
                                    ELSE        1.0
                                END, 1)
    ),
    'route', OBJECT_CONSTRUCT(
        'estimated_minutes', UNIFORM(5, 60, RANDOM()),
        'actual_minutes',    UNIFORM(5, 90, RANDOM()),
        'traffic_level',     CASE UNIFORM(1, 4, RANDOM())
                                WHEN 1 THEN 'heavy'
                                WHEN 2 THEN 'moderate'
                                WHEN 3 THEN 'light'
                                ELSE        'none'
                             END
    )
)
WHERE TRIP_METADATA IS NULL;

USE WAREHOUSE TRANSFORM_WH;

-- ============================================================
-- Verify load
-- ============================================================
SELECT
    'TRIPS_RAW'    AS table_name,
    COUNT(*)       AS row_count,
    MIN(PICKUP_DATETIME) AS earliest_trip,
    MAX(PICKUP_DATETIME) AS latest_trip,
    COUNT(CASE WHEN TRIP_METADATA IS NOT NULL THEN 1 END) AS rows_with_metadata
FROM RAW.TRIPS_RAW;
