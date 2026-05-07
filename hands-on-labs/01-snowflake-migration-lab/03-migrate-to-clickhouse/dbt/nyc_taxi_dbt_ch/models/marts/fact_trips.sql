{{
  config(
    materialized         = 'incremental',
    unique_key           = 'trip_id',
    incremental_strategy = 'delete_insert',
    engine               = 'ReplacingMergeTree(updated_at)',
    order_by             = '(toStartOfMonth(pickup_at), pickup_at, trip_id)',
    schema               = 'analytics'
  )
}}
{#
  Migration note: Snowflake used incremental_strategy='merge' which maps to MERGE INTO.
  ClickHouse has no MERGE INTO statement. Instead:
    1. The table engine ReplacingMergeTree(updated_at) tracks a version column.
    2. On background merges, ClickHouse keeps only the row with the highest updated_at
       per (order_by key). This replaces Snowflake MERGE INTO WHEN MATCHED THEN UPDATE.
    3. delete_insert is the dbt-clickhouse incremental strategy that:
       a. DELETEs rows where unique_key matches the incoming batch
       b. INSERTs the full new batch
       This is the correct strategy for ReplacingMergeTree in dbt.
    4. At query time, use SELECT ... FINAL to force immediate deduplication
       (e.g., SELECT * FROM fact_trips FINAL). Without FINAL, duplicate rows
       from in-flight merges may appear briefly.

  Migration note: Snowflake used cluster_by=['pickup_at::DATE'] for physical clustering.
  ClickHouse uses order_by as the primary key AND physical sort order — no separate
  cluster_by concept. The order_by tuple defines the sparse primary index.
#}

-- ┌─────────────────────────────────────────────────────────────────┐
-- │  Migration Note: Snowflake MERGE INTO → ClickHouse Strategy     │
-- │                                                                 │
-- │  Snowflake: MERGE INTO ... WHEN MATCHED THEN UPDATE            │
-- │  ClickHouse: dbt delete_insert incremental strategy            │
-- │                                                                 │
-- │  How delete_insert works:                                       │
-- │    1. dbt deletes rows matching unique_key (trip_id) from the   │
-- │       target table for the new batch                            │
-- │    2. dbt inserts the new batch                                 │
-- │    This is the primary mechanism ensuring correctness.          │
-- │                                                                 │
-- │  Role of ReplacingMergeTree(updated_at):                       │
-- │    - It is a SAFETY NET, not the primary deduplication path     │
-- │    - If a delete_insert run is interrupted mid-flight, the      │
-- │      ReplacingMergeTree engine will deduplicate duplicates      │
-- │      during the next background merge, keeping the row with     │
-- │      the highest updated_at value                               │
-- │    - ClickHouse merges are EVENTUAL (async background process)  │
-- │                                                                 │
-- │  For analytical queries requiring point-in-time correctness:   │
-- │    SELECT ... FROM analytics.fact_trips FINAL                   │
-- │    FINAL forces synchronous deduplication at query time.        │
-- │    It adds latency but guarantees no duplicate trip_ids.        │
-- └─────────────────────────────────────────────────────────────────┘

-- ════════════════════════════════════════════════════════════════════════════
-- fact_trips — Central fact table, 50M rows, one row per trip
--
-- SNOWFLAKE → CLICKHOUSE TRANSLATION SUMMARY:
--   MERGE INTO (incremental)           →  ReplacingMergeTree + delete_insert
--   incremental_strategy='merge'       →  incremental_strategy='delete_insert'
--   cluster_by=['pickup_at::DATE']     →  order_by='(toStartOfMonth(pickup_at), pickup_at, trip_id)'
--   CURRENT_TIMESTAMP()                →  now()
--   MAX(updated_at) incremental filter  →  max(updated_at) (lowercase)
--   now() AS updated_at                →  version column for ReplacingMergeTree
-- ════════════════════════════════════════════════════════════════════════════

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
    ingested_at,
    -- Snowflake: now() AS updated_at  (same function name, different case convention)
    -- ClickHouse: now() returns DateTime — used as the version column by ReplacingMergeTree.
    -- Rows with a higher updated_at value win during background deduplication merges.
    now() AS updated_at

FROM {{ ref('int_trips_enriched') }}

{% if is_incremental() %}
  -- High-watermark on updated_at (not pickup_at) so that fare adjustments are captured.
  -- A corrected trip re-inserts the same trip_id with the same pickup_at but a newer
  -- updated_at (set to now() on every insert). A pickup_at watermark would silently
  -- miss those corrections — the pickup time never changes.
  WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
