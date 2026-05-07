-- Q6: Incremental Hourly Zone Aggregation (MERGE)
-- Run by dbt every hour via Snowflake Task
-- Migration note: No MERGE in ClickHouse. Options:
--   (1) ReplacingMergeTree — engine handles dedup on OPTIMIZE or with FINAL
--   (2) INSERT + DELETE pattern with AggregatingMergeTree
--   (3) CollapsingMergeTree for explicit sign-based cancellation

USE WAREHOUSE TRANSFORM_WH;
USE DATABASE NYC_TAXI_DB;

-- Snowflake MERGE (Snowflake-specific)
MERGE INTO ANALYTICS.AGG_HOURLY_ZONE_TRIPS AS target
USING (
    SELECT
        DATE_TRUNC('hour', pickup_at)   AS hour_bucket,
        pickup_location_id              AS zone_id,
        COUNT(*)                        AS trips,
        SUM(total_amount_usd)           AS revenue,
        AVG(trip_distance_miles)        AS avg_distance
    FROM ANALYTICS.FACT_TRIPS
    WHERE pickup_at >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
) AS source
ON  target.hour_bucket = source.hour_bucket
AND target.zone_id     = source.zone_id
WHEN MATCHED THEN UPDATE SET
    target.trips        = source.trips,
    target.revenue      = source.revenue,
    target.avg_distance = source.avg_distance,
    target.updated_at   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (hour_bucket, zone_id, trips, revenue, avg_distance)
VALUES
    (source.hour_bucket, source.zone_id, source.trips,
     source.revenue, source.avg_distance);

-- ClickHouse equivalent using ReplacingMergeTree:
-- Table DDL:
--   CREATE TABLE analytics.agg_hourly_zone_trips (
--       hour_bucket   DateTime,
--       zone_id       UInt32,
--       trips         UInt64,
--       revenue       Float64,
--       avg_distance  Float64,
--       updated_at    DateTime DEFAULT now(),
--       _version      UInt64   DEFAULT toUnixTimestamp(now())
--   ) ENGINE = ReplacingMergeTree(_version)
--   ORDER BY (hour_bucket, zone_id);
--
-- Insert/upsert:
--   INSERT INTO analytics.agg_hourly_zone_trips
--   SELECT
--       toStartOfHour(pickup_at), pickup_location_id,
--       count(), sum(total_amount_usd), avg(trip_distance_miles),
--       now(), toUnixTimestamp(now())
--   FROM analytics.fact_trips
--   WHERE pickup_at >= now() - INTERVAL 2 HOUR
--   GROUP BY 1, 2;
--
-- Query with dedup (FINAL forces merge):
--   SELECT hour_bucket, zone_id, trips, revenue
--   FROM analytics.agg_hourly_zone_trips FINAL
--   ORDER BY hour_bucket DESC;
