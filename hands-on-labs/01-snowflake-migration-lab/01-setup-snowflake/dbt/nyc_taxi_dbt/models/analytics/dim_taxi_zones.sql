{{
  config(
    materialized = 'table',
    schema       = 'ANALYTICS'
  )
}}

-- Passthrough — data seeded directly by scripts/02_seed_data.sql
-- This model provides dbt lineage and enforces column naming conventions
SELECT
    location_id,
    borough,
    zone,
    service_zone
FROM {{ ref('stg_taxi_zones') }}
