{{
  config(
    materialized = 'ephemeral'
  )
}}

-- Intermediate model: trips joined to all dimension lookups
-- Ephemeral — compiled inline into downstream models, no physical table

SELECT
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
    -- Pickup zone
    pu.borough          AS pickup_borough,
    pu.zone             AS pickup_zone,
    pu.service_zone     AS pickup_service_zone,
    -- Dropoff zone
    do_.borough         AS dropoff_borough,
    do_.zone            AS dropoff_zone,
    do_.service_zone    AS dropoff_service_zone,
    -- Payment
    pt.payment_code     AS payment_code,
    pt.payment_desc     AS payment_type,
    -- Vendor
    v.vendor_code       AS vendor_code,
    v.vendor_name       AS vendor_name,
    -- Date attributes
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

LEFT JOIN NYC_TAXI_DB.ANALYTICS.DIM_PAYMENT_TYPE pt
    ON t.payment_type_id = pt.payment_type_id

LEFT JOIN NYC_TAXI_DB.ANALYTICS.DIM_VENDOR v
    ON t.vendor_id = v.vendor_id

LEFT JOIN NYC_TAXI_DB.ANALYTICS.DIM_DATE d
    ON t.pickup_at::DATE = d.date_day
