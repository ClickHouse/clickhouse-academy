{{
  config(
    materialized = 'table',
    engine       = 'MergeTree()',
    order_by     = '(location_id)',
    schema       = 'analytics'
  )
}}

-- ════════════════════════════════════════════════════════════════════════════
-- dim_taxi_zones — Taxi zone dimension (analytics layer)
--
-- Migration note: In Snowflake Part 1, zone data was seeded directly into
-- NYC_TAXI_DB.ANALYTICS.DIM_TAXI_ZONES via SQL seed scripts (01_create_tables.sql).
-- dim_taxi_zones was essentially a pass-through with no transformation.
--
-- In ClickHouse, the source data flows through the staging layer:
--   scripts/00_seed_zones.sql → default.taxi_zones (raw source) → stg_taxi_zones → analytics.dim_taxi_zones
--
-- This model simply promotes the staged/cleaned zones to the analytics schema.
-- No SQL translation required beyond the ref() path change.
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    location_id,
    borough,
    zone,
    service_zone
FROM {{ ref('stg_taxi_zones') }}
