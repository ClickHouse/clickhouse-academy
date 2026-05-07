-- ============================================================
-- 02_query_history.sql — Manual query history fallback
--
-- Run this in the Snowflake UI if 01_profile_snowflake.sh cannot
-- access ACCOUNT_USAGE (requires ACCOUNTADMIN + 1-3hr lag).
--
-- Usage:
--   1. Open Snowflake UI → Worksheets
--   2. Set role to ACCOUNTADMIN, warehouse to ANALYTICS_WH
--   3. Run each query block below
--   4. Paste results into profile_report.md Section 2
-- ============================================================

-- ── Query 1: Top queries by total elapsed time ───────────────
-- Use to identify the most expensive query patterns in the last 7 days.
-- These are the queries whose filter columns should drive your ORDER BY design.

SELECT
    query_type,
    LEFT(query_text, 300)                                 AS query_preview,
    execution_status,
    COUNT(*)                                              AS executions,
    ROUND(AVG(total_elapsed_time))                        AS avg_ms,
    ROUND(MAX(total_elapsed_time))                        AS max_ms,
    ROUND(MIN(total_elapsed_time))                        AS min_ms,
    ROUND(SUM(bytes_scanned) / 1024 / 1024 / 1024, 2)   AS total_gb_scanned,
    ROUND(AVG(rows_produced))                             AS avg_rows_returned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'NYC_TAXI_DB'
  AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
  AND query_type NOT IN ('SHOW', 'DESCRIBE', 'USE', 'SET', 'COMMIT', 'BEGIN_TRANSACTION')
  AND total_elapsed_time > 100  -- filter out sub-100ms metadata queries
GROUP BY 1, 2, 3
ORDER BY SUM(total_elapsed_time) DESC
LIMIT 20;


-- ── Query 2: Filter column frequency ─────────────────────────
-- Identify which columns appear most often in WHERE clauses.
-- The most-filtered columns should be first in ORDER BY.
-- Note: This is an approximation — ACCOUNT_USAGE stores query text, not parsed ASTs.

SELECT
    CASE
        WHEN LOWER(query_text) LIKE '%pickup_at%'           THEN 'pickup_at'
        WHEN LOWER(query_text) LIKE '%pickup_datetime%'     THEN 'pickup_datetime'
        WHEN LOWER(query_text) LIKE '%pickup_location_id%'  THEN 'pickup_location_id'
        WHEN LOWER(query_text) LIKE '%dropoff_location_id%' THEN 'dropoff_location_id'
        WHEN LOWER(query_text) LIKE '%vendor_id%'           THEN 'vendor_id'
        WHEN LOWER(query_text) LIKE '%payment_type%'        THEN 'payment_type'
        WHEN LOWER(query_text) LIKE '%trip_id%'             THEN 'trip_id'
        ELSE 'other'
    END AS filter_column,
    COUNT(*) AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'NYC_TAXI_DB'
  AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
  AND query_type = 'SELECT'
  AND LOWER(query_text) LIKE '%where%'
GROUP BY 1
ORDER BY 2 DESC;


-- ── Query 3: Queries using Snowflake-specific constructs ──────
-- Find queries that use constructs requiring ClickHouse translation.

SELECT
    CASE
        WHEN LOWER(query_text) LIKE '%qualify%'           THEN 'QUALIFY'
        WHEN LOWER(query_text) LIKE '%lateral flatten%'   THEN 'LATERAL FLATTEN'
        WHEN LOWER(query_text) LIKE '%merge into%'        THEN 'MERGE INTO'
        WHEN LOWER(query_text) LIKE '%::%'                THEN 'VARIANT colon-path'
        WHEN LOWER(query_text) LIKE '%metadata$%'         THEN 'Stream METADATA$'
        ELSE 'other'
    END AS construct,
    COUNT(*) AS query_count,
    ROUND(AVG(total_elapsed_time)) AS avg_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'NYC_TAXI_DB'
  AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
  AND (
      LOWER(query_text) LIKE '%qualify%'
      OR LOWER(query_text) LIKE '%lateral flatten%'
      OR LOWER(query_text) LIKE '%merge into%'
      OR query_text LIKE '%::%'
      OR LOWER(query_text) LIKE '%metadata$%'
  )
GROUP BY 1
ORDER BY 2 DESC;


-- ── Query 4: Warehouse utilization ───────────────────────────
-- Understand which warehouses are doing what work.
-- Relevant for Part 3 cost comparison.

SELECT
    warehouse_name,
    query_type,
    COUNT(*) AS query_count,
    ROUND(SUM(total_elapsed_time) / 1000 / 60, 1) AS total_minutes,
    ROUND(AVG(total_elapsed_time)) AS avg_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'NYC_TAXI_DB'
  AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY SUM(total_elapsed_time) DESC;


-- ── Query 5: Table access frequency ──────────────────────────
-- Which tables are accessed most often? Guides migration wave priority.

SELECT
    ao.object_name                         AS table_name,
    ao.object_schema                       AS schema_name,
    COUNT(DISTINCT ah.query_id)            AS distinct_queries,
    ROUND(AVG(qh.total_elapsed_time))      AS avg_query_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah
JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON ah.query_id = qh.query_id,
LATERAL FLATTEN(input => ah.base_objects_accessed) ao
WHERE qh.database_name = 'NYC_TAXI_DB'
  AND qh.start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND ao.value:objectDomain::STRING = 'Table'
GROUP BY 1, 2
ORDER BY 3 DESC;
