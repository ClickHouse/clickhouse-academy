{{
  config(
    materialized        = 'incremental',
    unique_key          = 'trip_id',
    incremental_strategy = 'merge',
    schema              = 'ANALYTICS',
    cluster_by          = ['pickup_at::DATE'],
    tags                = ['daily', 'core']
  )
}}

SELECT
    trip_id,
    pickup_at,
    dropoff_at,
    duration_minutes,
    trip_distance_miles,
    total_amount_usd,
    tip_amount_usd,
    fare_amount_usd,
    extra_amount_usd,
    mta_tax_usd,
    tolls_amount_usd,
    passenger_count,
    driver_rating,
    vehicle_type,
    app_platform,
    surge_multiplier,
    traffic_level,
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    payment_type,
    vendor_name,
    pickup_day_of_week,
    fiscal_quarter,
    is_weekend,
    is_holiday,
    ingested_at
FROM {{ ref('int_trips_enriched') }}

{% if is_incremental() %}
  WHERE pickup_at > (SELECT MAX(pickup_at) FROM {{ this }})
{% endif %}
