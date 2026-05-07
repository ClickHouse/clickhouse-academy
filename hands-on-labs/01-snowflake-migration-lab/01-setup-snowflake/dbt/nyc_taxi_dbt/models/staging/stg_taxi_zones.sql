{{
  config(
    materialized = 'view',
    schema       = 'STAGING'
  )
}}

-- Zone data is seeded directly into ANALYTICS.DIM_TAXI_ZONES via scripts/02_seed_data.sql
-- This staging view adds light cleaning and serves as the dbt lineage node
SELECT
    LOCATION_ID                       AS location_id,
    COALESCE(BOROUGH, 'Unknown')      AS borough,
    COALESCE(ZONE, 'Unknown')         AS zone,
    COALESCE(SERVICE_ZONE, 'Unknown') AS service_zone
FROM NYC_TAXI_DB.ANALYTICS.DIM_TAXI_ZONES
WHERE LOCATION_ID IS NOT NULL
