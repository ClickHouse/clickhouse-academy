{{
  config(
    materialized = 'view',
    schema       = 'STAGING'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'TRIPS_RAW') }}
),

flattened AS (
    SELECT
        TRIP_ID                                                     AS trip_id,
        VENDOR_ID                                                   AS vendor_id,
        PICKUP_DATETIME                                             AS pickup_at,
        DROPOFF_DATETIME                                            AS dropoff_at,
        DATEDIFF('minute', PICKUP_DATETIME, DROPOFF_DATETIME)       AS duration_minutes,
        PASSENGER_COUNT                                             AS passenger_count,
        TRIP_DISTANCE                                               AS trip_distance_miles,
        TOTAL_AMOUNT                                                AS total_amount_usd,
        TIP_AMOUNT                                                  AS tip_amount_usd,
        FARE_AMOUNT                                                 AS fare_amount_usd,
        EXTRA                                                       AS extra_amount_usd,
        MTA_TAX                                                     AS mta_tax_usd,
        TOLLS_AMOUNT                                                AS tolls_amount_usd,
        PU_LOCATION_ID                                              AS pickup_location_id,
        DO_LOCATION_ID                                              AS dropoff_location_id,
        PAYMENT_TYPE                                                AS payment_type_id,
        RATECODE_ID                                                 AS rate_code_id,
        STORE_FWD_FLAG                                              AS store_fwd_flag,
        INGESTED_AT                                                 AS ingested_at,
        -- VARIANT column flattened to typed columns
        -- Migration note: Snowflake colon-path syntax → ClickHouse JSONExtractFloat/String
        TRIP_METADATA:driver.rating::FLOAT                         AS driver_rating,
        TRIP_METADATA:driver.trips_completed::INTEGER              AS driver_trips_completed,
        TRIP_METADATA:driver.vehicle_type::VARCHAR                 AS vehicle_type,
        TRIP_METADATA:app.platform::VARCHAR                        AS app_platform,
        TRIP_METADATA:app.version::VARCHAR                         AS app_version,
        TRIP_METADATA:app.surge_multiplier::FLOAT                  AS surge_multiplier,
        TRIP_METADATA:route.estimated_minutes::INTEGER             AS route_estimated_minutes,
        TRIP_METADATA:route.actual_minutes::INTEGER                AS route_actual_minutes,
        TRIP_METADATA:route.traffic_level::VARCHAR                 AS traffic_level
    FROM source
    WHERE TRIP_ID IS NOT NULL
      AND PICKUP_DATETIME IS NOT NULL
      AND DROPOFF_DATETIME IS NOT NULL
      AND DROPOFF_DATETIME > PICKUP_DATETIME  -- exclude negative-duration trips
)

SELECT * FROM flattened
