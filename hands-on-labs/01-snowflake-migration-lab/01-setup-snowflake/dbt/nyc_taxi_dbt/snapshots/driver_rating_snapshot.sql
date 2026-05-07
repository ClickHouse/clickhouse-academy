{% snapshot driver_rating_snapshot %}

{{
    config(
        target_schema = 'STAGING',
        unique_key    = 'trip_id',
        strategy      = 'check',
        check_cols    = ['driver_rating', 'vehicle_type'],
        invalidate_hard_deletes = True
    )
}}

-- SCD Type 2: tracks changes to driver rating and vehicle type over time
-- Demonstrates how Snowflake snapshot patterns translate to ClickHouse
-- ClickHouse equivalent: ReplacingMergeTree with version column
SELECT
    trip_id,
    driver_rating,
    vehicle_type,
    app_platform,
    CURRENT_TIMESTAMP() AS snapshot_at
FROM {{ ref('stg_trips') }}
WHERE driver_rating IS NOT NULL

{% endsnapshot %}
