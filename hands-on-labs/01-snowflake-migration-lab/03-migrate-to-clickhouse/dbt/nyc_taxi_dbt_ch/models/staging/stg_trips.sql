{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}
{#
  Migration note: schema uses lowercase 'staging' vs Snowflake 'STAGING'.
  ClickHouse identifiers are case-sensitive; dbt-clickhouse lowercases via
  the generate_schema_name macro in macros/generate_schema_name.sql.
#}

-- ════════════════════════════════════════════════════════════════════════════
-- stg_trips — Staging layer: raw → typed, JSON flattened
--
-- SNOWFLAKE → CLICKHOUSE TRANSLATION SUMMARY:
--   VARIANT colon-path  →  JSONExtract*() functions
--   DATEDIFF()          →  dateDiff()  (lowercase 'd')
--   ::FLOAT / ::INTEGER →  removed (JSONExtract returns typed value directly)
--   IS NOT NULL         →  != ''  (ClickHouse MergeTree String cannot be NULL)
--   DROPOFF > PICKUP    →  same (DateTime comparison works identically)
--   schema = 'STAGING'  →  schema = 'staging'
-- ════════════════════════════════════════════════════════════════════════════

WITH source AS (
    SELECT * FROM {{ source('raw', 'trips_raw') }} FINAL
    -- FINAL forces synchronous deduplication of trips_raw (ReplacingMergeTree(_synced_at)).
    -- Without FINAL, duplicate trip_ids from migration retries or producer retries would
    -- propagate into all downstream models (fact_trips, agg_hourly_zone_trips).
    -- Migration note: Snowflake source was source('raw', 'TRIPS_RAW') (uppercase).
    -- ClickHouse table names are case-sensitive; table was created with lowercase name.
),

flattened AS (
    SELECT
        trip_id,
        vendor_id,
        pickup_at,
        dropoff_at,

        -- Snowflake: DATEDIFF('minute', PICKUP_DATETIME, DROPOFF_DATETIME)
        -- ClickHouse: dateDiff() — lowercase 'd', same argument order
        dateDiff('minute', pickup_at, dropoff_at)                           AS duration_minutes,

        passenger_count,
        trip_distance_miles,
        total_amount_usd,
        tip_amount_usd,
        fare_amount_usd,
        extra_amount_usd,
        mta_tax_usd,
        tolls_amount_usd,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        rate_code_id,
        store_fwd_flag,
        ingested_at,

        -- ────────────────────────────────────────────────────────────────────
        -- VARIANT → JSON flattening
        -- Snowflake used colon-path syntax + cast: TRIP_METADATA:driver.rating::FLOAT
        -- ClickHouse uses JSONExtract functions; no cast needed — function returns typed value.
        -- ────────────────────────────────────────────────────────────────────

        -- Snowflake: TRIP_METADATA:driver.rating::FLOAT
        JSONExtractFloat(trip_metadata, 'driver', 'rating')                 AS driver_rating,

        -- Snowflake: TRIP_METADATA:driver.trips_completed::INTEGER
        JSONExtractInt(trip_metadata, 'driver', 'trips_completed')          AS driver_trips_completed,

        -- Snowflake: TRIP_METADATA:driver.vehicle_type::VARCHAR
        JSONExtractString(trip_metadata, 'driver', 'vehicle_type')          AS vehicle_type,

        -- Snowflake: TRIP_METADATA:app.platform::VARCHAR
        JSONExtractString(trip_metadata, 'app', 'platform')                 AS app_platform,

        -- Snowflake: TRIP_METADATA:app.version::VARCHAR
        JSONExtractString(trip_metadata, 'app', 'version')                  AS app_version,

        -- Snowflake: TRIP_METADATA:app.surge_multiplier::FLOAT
        JSONExtractFloat(trip_metadata, 'app', 'surge_multiplier')          AS surge_multiplier,

        -- Snowflake: TRIP_METADATA:route.estimated_minutes::INTEGER
        JSONExtractInt(trip_metadata, 'route', 'estimated_minutes')         AS route_estimated_minutes,

        -- Snowflake: TRIP_METADATA:route.actual_minutes::INTEGER
        JSONExtractInt(trip_metadata, 'route', 'actual_minutes')            AS route_actual_minutes,

        -- Snowflake: TRIP_METADATA:route.traffic_level::VARCHAR
        JSONExtractString(trip_metadata, 'route', 'traffic_level')          AS traffic_level

    FROM source

    -- Migration note: Snowflake used IS NOT NULL for nullable columns.
    -- ClickHouse MergeTree String columns default to '' not NULL, so use != ''.
    -- DateTime columns in ClickHouse cannot be NULL either — use > '1970-01-01'.
    WHERE trip_id    != ''
      AND pickup_at  > toDateTime('1970-01-01 00:00:00')
      AND dropoff_at > toDateTime('1970-01-01 00:00:00')
      -- Snowflake: AND DROPOFF_DATETIME > PICKUP_DATETIME
      -- ClickHouse: same comparison — DateTime supports > operator identically
      AND dropoff_at > pickup_at
)

SELECT * FROM flattened
