{{
  config(
    materialized = 'materialized_view',
    schema       = 'analytics',
    engine       = 'ReplacingMergeTree(refreshed_at)',
    order_by     = '(snapshot_key)',
  )
}}
-- ┌─────────────────────────────────────────────────────────────────┐
-- │  ClickHouse-Exclusive Feature: REFRESHABLE Materialized View    │
-- │                                                                 │
-- │  Snowflake equivalent: None. Closest is a Snowflake Task that   │
-- │  runs a stored procedure on a schedule — but that's 30+ lines   │
-- │  of TASK DDL. In ClickHouse, one ALTER TABLE statement does it. │
-- │                                                                 │
-- │  Why REFRESHABLE, not a standard trigger-based MV?             │
-- │  Standard ClickHouse MVs fire on INSERT and see only the batch  │
-- │  being inserted — not the full table. They cannot compute        │
-- │  lifetime aggregates like total_trips or avg fare correctly.     │
-- │  REFRESHABLE MVs do a full re-scan on a schedule (23.4+).       │
-- │                                                                 │
-- │  After dbt run, enable periodic refresh with:                   │
-- │    ALTER TABLE analytics.mv_live_trip_feed                      │
-- │      MODIFY REFRESH EVERY 30 SECOND;                            │
-- │                                                                 │
-- │  Query the latest snapshot:                                     │
-- │    SELECT * FROM analytics.mv_live_trip_feed FINAL              │
-- │    ORDER BY refreshed_at DESC LIMIT 1;                          │
-- └─────────────────────────────────────────────────────────────────┘
SELECT
    1                                                    AS snapshot_key,   -- fixed key so FINAL deduplicates to 1 row
    now()                                                AS refreshed_at,
    count()                                              AS total_trips,
    countIf(pickup_at >= now() - INTERVAL 1 HOUR)       AS trips_last_hour,
    countIf(toDate(pickup_at) = today())                 AS trips_today,
    round(avg(total_amount_usd), 2)                      AS avg_fare_usd,
    round(sumIf(total_amount_usd, toDate(pickup_at) = today()), 2) AS revenue_today
FROM {{ ref('fact_trips') }}
