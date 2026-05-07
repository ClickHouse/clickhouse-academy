-- ============================================================
-- Script 00: Seed taxi_zones in ClickHouse
-- Run before: dbt run (section 7.1)
--
-- Creates and populates default.taxi_zones with 265 synthetic
-- NYC TLC zone records — identical distribution to the Snowflake
-- seed in 01-setup-snowflake/scripts/02_seed_data.sql.
--
-- The zone data is static reference data (265 rows). It is seeded
-- once via this script before the first dbt run.
-- stg_taxi_zones reads from this table via source('raw', 'taxi_zones').
-- Named taxi_zones (not dim_taxi_zones) to avoid collision with analytics.dim_taxi_zones.
-- ============================================================

CREATE TABLE IF NOT EXISTS default.taxi_zones (
    location_id   UInt16,
    borough       String,
    zone          String,
    service_zone  String
)
ENGINE = MergeTree()
ORDER BY (location_id);

TRUNCATE TABLE default.taxi_zones;

INSERT INTO default.taxi_zones (location_id, borough, zone, service_zone)
SELECT
    n AS location_id,
    CASE
        WHEN n BETWEEN   1 AND  69 THEN 'Manhattan'
        WHEN n BETWEEN  70 AND 139 THEN 'Brooklyn'
        WHEN n BETWEEN 140 AND 199 THEN 'Queens'
        WHEN n BETWEEN 200 AND 235 THEN 'Bronx'
        WHEN n BETWEEN 236 AND 250 THEN 'Staten Island'
        ELSE                            'EWR'
    END AS borough,
    concat(
        CASE (n % 20)
            WHEN  0 THEN 'Airport'    WHEN  1 THEN 'Heights'
            WHEN  2 THEN 'Gardens'    WHEN  3 THEN 'Park'
            WHEN  4 THEN 'Hill'       WHEN  5 THEN 'Village'
            WHEN  6 THEN 'Square'     WHEN  7 THEN 'Bridge'
            WHEN  8 THEN 'Terrace'    WHEN  9 THEN 'Point'
            WHEN 10 THEN 'Flats'      WHEN 11 THEN 'Beach'
            WHEN 12 THEN 'Junction'   WHEN 13 THEN 'Manor'
            WHEN 14 THEN 'Harbor'     WHEN 15 THEN 'Estates'
            WHEN 16 THEN 'Commons'    WHEN 17 THEN 'District'
            WHEN 18 THEN 'Place'      ELSE        'Center'
        END,
        ' ',
        toString(n)
    ) AS zone,
    CASE (n % 3)
        WHEN 0 THEN 'Yellow Zone'
        WHEN 1 THEN 'Boro Zone'
        ELSE        'Airports'
    END AS service_zone
FROM (
    SELECT number + 1 AS n
    FROM numbers(265)
);

-- Verify
SELECT count() AS zone_count FROM default.taxi_zones;
-- Expected: 265
