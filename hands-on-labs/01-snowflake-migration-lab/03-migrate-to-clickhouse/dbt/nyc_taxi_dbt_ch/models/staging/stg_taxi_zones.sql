-- ════════════════════════════════════════════════════════════════════════════
-- stg_taxi_zones — Cleaned taxi zone dimension
--
-- Migration note: In Snowflake Part 1, zone data was pre-seeded into
-- NYC_TAXI_DB.ANALYTICS.DIM_TAXI_ZONES via SQL seed scripts, and stg_taxi_zones
-- queried it directly with a fully qualified name:
--   FROM NYC_TAXI_DB.ANALYTICS.DIM_TAXI_ZONES
--
-- In ClickHouse, the zone lookup table is seeded once via scripts/00_seed_zones.sql
-- into default.taxi_zones. We reference it via source('raw', 'taxi_zones').
-- (Named taxi_zones, not dim_taxi_zones, to avoid confusion with analytics.dim_taxi_zones.)
--
-- SQL translation:
--   COALESCE()  →  coalesce()  (same function, lowercase in ClickHouse convention)
--   location_id IS NOT NULL  →  location_id != 0  (Int type, not nullable in MergeTree)
-- ════════════════════════════════════════════════════════════════════════════

{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}

SELECT
    location_id,
    -- Snowflake: COALESCE(BOROUGH, 'Unknown')
    -- ClickHouse: coalesce() works identically; column names are lowercase
    coalesce(borough,      'Unknown') AS borough,
    coalesce(zone,         'Unknown') AS zone,
    coalesce(service_zone, 'Unknown') AS service_zone
FROM {{ source('raw', 'taxi_zones') }}

-- Snowflake: WHERE LOCATION_ID IS NOT NULL
-- ClickHouse: Int column in MergeTree defaults to 0 when missing, not NULL
WHERE location_id != 0
