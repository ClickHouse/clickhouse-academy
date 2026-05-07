# Streams and tasks are applied in a targeted second `terraform apply` inside setup.sh,
# AFTER 01_create_tables.sql has created TRIPS_RAW.  They cannot be part of the initial
# apply because the stream requires the table to exist first.
#
# setup.sh handles the correct ordering automatically.
# To apply manually:
#   terraform apply \
#     -target=snowflake_execute.trips_cdc_stream \
#     -target=snowflake_execute.cdc_consume_task  \
#     -target=snowflake_execute.hourly_agg_task   \
#     -auto-approve

# ---------------------------------------------------------------------------
# CDC stream on TRIPS_RAW
# Captures all DML changes (INSERT/UPDATE/DELETE).
# In the migration lab, this stream is retired at cutover — live writes go directly to ClickHouse.
# ---------------------------------------------------------------------------
resource "snowflake_execute" "trips_cdc_stream" {
  execute = <<-SQL
    CREATE OR REPLACE STREAM NYC_TAXI_DB.RAW.TRIPS_CDC_STREAM
      ON TABLE NYC_TAXI_DB.RAW.TRIPS_RAW
      APPEND_ONLY = FALSE
      COMMENT = 'CDC stream for incremental ClickHouse sync — migration lab';
  SQL
  revert     = "DROP STREAM IF EXISTS NYC_TAXI_DB.RAW.TRIPS_CDC_STREAM"
  depends_on = [snowflake_schema.raw]
}

# ---------------------------------------------------------------------------
# Task 1: consume CDC stream every 5 minutes
# Created SUSPENDED — resume after setup:
#   ALTER TASK NYC_TAXI_DB.RAW.CDC_CONSUME_TASK RESUME;
#
# In the migration lab this query shows the source-side CDC mechanism.
# Migration challenge: Snowflake Tasks have no native equivalent in
# ClickHouse — replaced by the post-cutover producer (direct writes) or Debezium for real CDC.
# ---------------------------------------------------------------------------
resource "snowflake_execute" "cdc_consume_task" {
  execute = <<-SQL
    CREATE OR REPLACE TASK NYC_TAXI_DB.RAW.CDC_CONSUME_TASK
      WAREHOUSE = TRANSFORM_WH
      SCHEDULE  = 'USING CRON */5 * * * * UTC'
      COMMENT   = 'Consumes TRIPS_CDC_STREAM every 5 min — retired at cutover in migration lab'
    AS
      SELECT
        METADATA$ACTION   AS cdc_action,
        METADATA$ISUPDATE AS is_update,
        METADATA$ROW_ID   AS row_id,
        TRIP_ID,
        PICKUP_DATETIME,
        TOTAL_AMOUNT,
        TRIP_METADATA
      FROM NYC_TAXI_DB.RAW.TRIPS_CDC_STREAM
      WHERE METADATA$ACTION = 'INSERT'
      ORDER BY PICKUP_DATETIME;
  SQL
  revert     = "DROP TASK IF EXISTS NYC_TAXI_DB.RAW.CDC_CONSUME_TASK"
  depends_on = [snowflake_execute.trips_cdc_stream]
}

# ---------------------------------------------------------------------------
# Task 2: refresh hourly zone aggregates via MERGE (runs every hour)
# Created SUSPENDED — resume after dbt run:
#   ALTER TASK NYC_TAXI_DB.STAGING.HOURLY_AGG_TASK RESUME;
#
# Migration challenge: ClickHouse has no MERGE statement.
# ClickHouse equivalent: ReplacingMergeTree + INSERT (no explicit MERGE needed).
# ---------------------------------------------------------------------------
resource "snowflake_execute" "hourly_agg_task" {
  execute = <<-SQL
    CREATE OR REPLACE TASK NYC_TAXI_DB.STAGING.HOURLY_AGG_TASK
      WAREHOUSE = TRANSFORM_WH
      SCHEDULE  = 'USING CRON 0 * * * * UTC'
      COMMENT   = 'Refreshes AGG_HOURLY_ZONE_TRIPS every hour via MERGE — migration challenge: no MERGE in ClickHouse'
    AS
      MERGE INTO NYC_TAXI_DB.ANALYTICS.AGG_HOURLY_ZONE_TRIPS AS target
      USING (
        SELECT
          DATE_TRUNC('hour', pickup_at) AS hour_bucket,
          pickup_location_id            AS zone_id,
          COUNT(*)                      AS trips,
          SUM(total_amount_usd)         AS revenue,
          AVG(trip_distance_miles)      AS avg_distance
        FROM NYC_TAXI_DB.ANALYTICS.FACT_TRIPS
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
  SQL
  revert     = "DROP TASK IF EXISTS NYC_TAXI_DB.STAGING.HOURLY_AGG_TASK"
  depends_on = [snowflake_execute.trips_cdc_stream]
}
