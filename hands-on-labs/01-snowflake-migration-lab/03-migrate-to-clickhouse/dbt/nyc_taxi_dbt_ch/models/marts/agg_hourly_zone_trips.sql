{{
  config(
    materialized         = 'incremental',
    unique_key           = ['hour_bucket', 'zone_id'],
    incremental_strategy = 'delete_insert',
    engine               = 'ReplacingMergeTree(updated_at)',
    order_by             = '(hour_bucket, zone_id)',
    schema               = 'analytics'
  )
}}
{#
  Migration note: MERGE INTO → ReplacingMergeTree + delete_insert
  See fact_trips.sql for full explanation of this pattern.
  For aggregates, the delete_insert strategy:
    1. Deletes all rows whose (hour_bucket, zone_id) appear in the new batch
    2. Inserts the freshly recomputed aggregates for those keys
  This correctly handles re-aggregation of partial hours at the window boundary.
#}

-- ════════════════════════════════════════════════════════════════════════════
-- agg_hourly_zone_trips — Pre-aggregated hourly trip metrics per zone
--
-- SNOWFLAKE → CLICKHOUSE TRANSLATION SUMMARY:
--   DATE_TRUNC('hour', pickup_at)           →  toStartOfHour(pickup_at)
--   DATEADD('hour', -2, CURRENT_TIMESTAMP()) →  now() - INTERVAL 2 HOUR
--   CURRENT_TIMESTAMP()                     →  now()
--   COUNT(*)                                →  count()
--   SUM() / AVG()                           →  sum() / avg()  (same, lowercase)
--   incremental_strategy='merge'            →  incremental_strategy='delete_insert'
--   MERGE INTO ... WHEN MATCHED THEN UPDATE →  ReplacingMergeTree(updated_at)
--
-- WHY delete_insert IS CORRECT HERE:
--   Snowflake's MERGE INTO found matching (hour_bucket, zone_id) rows and updated
--   the aggregate columns in place. ClickHouse tables are immutable on disk —
--   you cannot UPDATE in place. delete_insert achieves the same semantic:
--   delete stale aggregates for the affected keys, then insert fresh ones.
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    -- Snowflake: DATE_TRUNC('hour', pickup_at)
    -- ClickHouse: toStartOfHour() — purpose-built function, equivalent result
    toStartOfHour(pickup_at)    AS hour_bucket,

    pickup_location_id          AS zone_id,

    -- Snowflake: COUNT(*)
    -- ClickHouse: count() — the * is optional and conventional to omit
    count()                     AS trips,

    sum(total_amount_usd)       AS revenue,
    avg(trip_distance_miles)    AS avg_distance,

    -- Snowflake: CURRENT_TIMESTAMP()
    -- ClickHouse: now() — returns current DateTime, same semantics
    now()                       AS updated_at

-- Source: stg_trips directly (bypasses int_trips_enriched for performance)
-- This model only needs pickup_at, pickup_location_id, total_amount_usd, trip_distance_miles.
-- Using the staging view avoids the 10-way join in int_trips_enriched for this aggregate.
-- Compare to fact_trips, which uses int_trips_enriched for the full denormalized row.
FROM {{ ref('stg_trips') }}

{% if is_incremental() %}
  -- Snowflake: WHERE pickup_at >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
  -- ClickHouse: interval arithmetic uses INTERVAL keyword with unit noun
  -- 'day'/DATEADD → today() - INTERVAL N DAY  |  'hour' → now() - INTERVAL N HOUR
  WHERE pickup_at >= now() - INTERVAL 2 HOUR
{% endif %}

GROUP BY 1, 2
