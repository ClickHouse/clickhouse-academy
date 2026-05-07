{{
  config(
    materialized        = 'incremental',
    unique_key          = ['hour_bucket', 'zone_id'],
    incremental_strategy = 'merge',
    schema              = 'ANALYTICS',
    tags                = ['hourly', 'aggregate'],
    post_hook           = "ALTER TABLE {{ this }} CLUSTER BY (hour_bucket)"
  )
}}

-- Migration note: This MERGE incremental strategy is one of the most challenging
-- translation problems. ClickHouse has no native MERGE.
-- ClickHouse equivalent: ReplacingMergeTree with _version column + FINAL in queries
-- OR: explicit INSERT + DELETE using CollapsingMergeTree

SELECT
    DATE_TRUNC('hour', pickup_at)   AS hour_bucket,
    pickup_location_id              AS zone_id,
    COUNT(*)                        AS trips,
    SUM(total_amount_usd)           AS revenue,
    AVG(trip_distance_miles)        AS avg_distance,
    CURRENT_TIMESTAMP()             AS updated_at

FROM {{ ref('stg_trips') }}

{% if is_incremental() %}
  WHERE pickup_at >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
{% endif %}

GROUP BY 1, 2
