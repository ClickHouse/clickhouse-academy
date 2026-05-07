{{
  config(
    materialized = 'ephemeral'
  )
}}
{#
  Migration note: ephemeral materialization is identical in behavior between
  Snowflake and ClickHouse dbt adapters — the model is inlined as a CTE into
  any downstream model that references it. No translation needed here.
#}

-- ════════════════════════════════════════════════════════════════════════════
-- int_trips_enriched — Intermediate: trips joined to all dimension tables
--
-- SNOWFLAKE → CLICKHOUSE TRANSLATION SUMMARY:
--   pickup_at::DATE = d.date_day  →  toDate(pickup_at) = d.date_day
--   Fully qualified Snowflake refs (NYC_TAXI_DB.ANALYTICS.DIM_*)
--     -> dbt ref() expressions pointing to local ClickHouse models
--
-- All join logic and column selection is otherwise identical to Part 1.
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    -- Core trip fields
    t.trip_id,
    t.pickup_at,
    t.dropoff_at,
    t.duration_minutes,
    t.trip_distance_miles,
    t.total_amount_usd,
    t.tip_amount_usd,
    t.fare_amount_usd,
    t.extra_amount_usd,
    t.mta_tax_usd,
    t.tolls_amount_usd,
    t.passenger_count,

    -- JSON-extracted metadata fields (already typed from stg_trips)
    t.driver_rating,
    t.driver_trips_completed,
    t.vehicle_type,
    t.app_platform,
    t.app_version,
    t.surge_multiplier,
    t.traffic_level,
    t.rate_code_id,
    t.store_fwd_flag,
    t.ingested_at,

    -- Zone enrichment — pickup
    pu.borough      AS pickup_borough,
    pu.zone         AS pickup_zone,
    pu.service_zone AS pickup_service_zone,

    -- Zone enrichment — dropoff
    do_.borough      AS dropoff_borough,
    do_.zone         AS dropoff_zone,
    do_.service_zone AS dropoff_service_zone,

    -- Payment type enrichment
    pt.payment_code AS payment_code,
    pt.payment_desc AS payment_type,

    -- Vendor enrichment
    v.vendor_code AS vendor_code,
    v.vendor_name AS vendor_name,

    -- Date dimension enrichment
    d.day_of_week       AS pickup_day_of_week,
    d.day_of_week_num   AS pickup_day_of_week_num,
    d.month_name        AS pickup_month,
    d.quarter_num       AS pickup_quarter,
    d.fiscal_quarter    AS fiscal_quarter,
    d.is_weekend        AS is_weekend,
    d.is_holiday        AS is_holiday,
    d.holiday_name      AS holiday_name

FROM {{ ref('stg_trips') }} t

LEFT JOIN {{ ref('stg_taxi_zones') }} pu
    ON t.pickup_location_id = pu.location_id

LEFT JOIN {{ ref('stg_taxi_zones') }} do_
    ON t.dropoff_location_id = do_.location_id

LEFT JOIN {{ ref('dim_payment_type') }} pt
    ON t.payment_type_id = pt.payment_type_id

LEFT JOIN {{ ref('dim_vendor') }} v
    ON t.vendor_id = v.vendor_id

LEFT JOIN {{ ref('dim_date') }} d
    -- Snowflake: ON date_spine.date_day::DATE = d.date_day
    -- ClickHouse: toDate() extracts the date component from a DateTime column
    ON toDate(t.pickup_at) = d.date_day
