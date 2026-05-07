-- Q7: Consume CDC Stream for Incremental ClickHouse Sync
-- Source-side mechanism for live CDC migration track
-- In the migration lab, this stream is retired at cutover — live writes go directly
-- to ClickHouse via the post-cutover producer (scripts/03_cutover.sh).
-- Migration note: Snowflake Streams have no native ClickHouse equivalent.
--                 ClickHouse equivalent: direct producer writes post-cutover,
--                                        or Debezium → Kafka → ClickHouse for real CDC.

USE WAREHOUSE TRANSFORM_WH;
USE DATABASE NYC_TAXI_DB;

-- Read all pending changes from the CDC stream
-- METADATA$ columns are Snowflake-specific stream metadata fields
SELECT
    METADATA$ACTION                                                 AS cdc_action,    -- 'INSERT' or 'DELETE'
    METADATA$ISUPDATE                                               AS is_update,     -- TRUE for UPDATE events (shown as DELETE+INSERT pair)
    METADATA$ROW_ID                                                 AS row_id,
    TRIP_ID,
    VENDOR_ID,
    PICKUP_DATETIME,
    DROPOFF_DATETIME,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    PU_LOCATION_ID,
    DO_LOCATION_ID,
    PAYMENT_TYPE,
    FARE_AMOUNT,
    TOTAL_AMOUNT,
    TIP_AMOUNT,
    TRIP_METADATA
FROM NYC_TAXI_DB.RAW.TRIPS_CDC_STREAM
WHERE METADATA$ACTION = 'INSERT'
ORDER BY PICKUP_DATETIME DESC
LIMIT 10000;

-- To process the full change feed for ClickHouse sync:
-- 1. SELECT all rows from stream (this consumes the stream)
-- 2. INSERT rows where METADATA$ACTION = 'INSERT' into ClickHouse via the producer
-- 3. For UPDATE events: rows come as DELETE + INSERT pair; handle in ClickHouse
--    using ReplacingMergeTree or by processing sign column

-- Show current stream lag (useful for monitoring sync health)
SELECT SYSTEM$STREAM_BACKLOG_SIZE('NYC_TAXI_DB.RAW.TRIPS_CDC_STREAM') AS stream_backlog_bytes;
