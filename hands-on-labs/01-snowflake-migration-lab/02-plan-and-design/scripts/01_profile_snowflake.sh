#!/usr/bin/env bash
# ============================================================
# 01_profile_snowflake.sh — Profile the Part 1 Snowflake environment
#
# Generates profile_report.md with four sections:
#   1. Object Inventory (tables, views, streams, tasks)
#   2. Query Workload (top 10 queries by total elapsed time)
#   3. Table Statistics (row counts, date ranges, null rates)
#   4. Schema Compatibility Gaps (auto-detected migration challenges)
#
# Prerequisites:
#   - snowsql installed and on PATH
#   - Part 1 environment running
#   - SNOWFLAKE_ORG, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD set
#
# Usage:
#   source ../01-setup-snowflake/.env
#   ./scripts/01_profile_snowflake.sh
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="${SCRIPT_DIR}/.."
OUTPUT_FILE="${MODULE_DIR}/profile_report.md"

BOLD="\033[1m"; RESET="\033[0m"
BLUE="\033[1;34m"; GREEN="\033[1;32m"; YELLOW="\033[1;33m"; RED="\033[1;31m"

log()  { echo -e "\n${BLUE}${BOLD}▶ $*${RESET}"; }
ok()   { echo -e "${GREEN}${BOLD}✓ $*${RESET}"; }
warn() { echo -e "${YELLOW}  ⚠ $*${RESET}"; }
die()  { echo -e "\n${RED}${BOLD}✗ ERROR: $*${RESET}\n"; exit 1; }

# ── Detect snowsql ────────────────────────────────────────────
SNOWSQL=""
if command -v snowsql >/dev/null 2>&1; then
  SNOWSQL="snowsql"
elif [[ -f "/Applications/SnowSQL.app/Contents/MacOS/snowsql" ]]; then
  SNOWSQL="/Applications/SnowSQL.app/Contents/MacOS/snowsql"
else
  die "snowsql not found. Install from https://docs.snowflake.com/en/user-guide/snowsql-install-config"
fi

# ── Check required env vars ───────────────────────────────────
for v in SNOWFLAKE_ORG SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD; do
  [[ -z "${!v:-}" ]] && die "Missing required env var: $v. Source Part 1's .env first."
done

SNOWFLAKE_ACCOUNT_ID="${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}"

# ── Helper: run SnowSQL query, return result ──────────────────
run_snowsql() {
  local role="${1}"; local warehouse="${2}"; local query="${3}"
  SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL}" \
    -a "${SNOWFLAKE_ACCOUNT_ID}" \
    -u "${SNOWFLAKE_USER}" \
    --rolename "${role}" \
    --warehouse "${warehouse}" \
    -q "${query}" \
    --option output_format=plain \
    --option friendly=false \
    --option timing=false \
    2>/dev/null || echo "(query failed)"
}

echo -e "\n${BLUE}${BOLD}Part 2: Snowflake Profiling Script${RESET}"
echo    "────────────────────────────────────"
echo    "  Output: ${OUTPUT_FILE}"

# ── Start writing report ──────────────────────────────────────
REPORT_DATE=$(date -u "+%Y-%m-%d %H:%M UTC")

cat > "${OUTPUT_FILE}" <<HEADER
# Snowflake Profile Report

Generated: ${REPORT_DATE}
Source: NYC_TAXI_DB (${SNOWFLAKE_ACCOUNT_ID})

This report was generated automatically by \`scripts/01_profile_snowflake.sh\`.
Use it to complete the worksheets and fill in \`migration-plan.md\`.

---

HEADER

# ── Section 1: Object Inventory ──────────────────────────────
log "Section 1: Object Inventory"

cat >> "${OUTPUT_FILE}" <<'MD'
## Section 1: Object Inventory

### Tables and Views

MD

TABLES_RESULT=$(run_snowsql "ANALYST_ROLE" "ANALYTICS_WH" "
SELECT
    t.table_schema,
    t.table_name,
    t.table_type,
    COALESCE(t.row_count, 0)         AS row_count,
    COALESCE(t.bytes, 0)             AS size_bytes,
    CASE
      WHEN t.row_count > 10000000  THEN 'C - Large (>10M rows)'
      WHEN t.row_count > 100000    THEN 'B - Medium (100K-10M rows)'
      WHEN t.row_count > 0         THEN 'A - Small (<100K rows)'
      ELSE 'D - Unknown / empty'
    END                              AS complexity_grade,
    t.comment
FROM NYC_TAXI_DB.INFORMATION_SCHEMA.TABLES t
WHERE t.table_schema IN ('RAW', 'STAGING', 'ANALYTICS')
ORDER BY t.table_schema, t.table_type, t.table_name;
")

echo "${TABLES_RESULT}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

# Check for VARIANT columns
cat >> "${OUTPUT_FILE}" <<'MD'

### VARIANT Columns (require JSONExtract* translation)

MD

VARIANT_RESULT=$(run_snowsql "ANALYST_ROLE" "ANALYTICS_WH" "
SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type
FROM NYC_TAXI_DB.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema IN ('RAW', 'STAGING', 'ANALYTICS')
  AND c.data_type = 'VARIANT'
ORDER BY c.table_schema, c.table_name, c.column_name;
")

echo "${VARIANT_RESULT}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

# Streams and Tasks (requires ACCOUNTADMIN)
cat >> "${OUTPUT_FILE}" <<'MD'

### Streams and Tasks

> Note: SHOW STREAMS and SHOW TASKS require ACCOUNTADMIN. If the output below is empty,
> run these commands manually in the Snowflake UI:
>   SHOW STREAMS IN DATABASE NYC_TAXI_DB;
>   SHOW TASKS IN DATABASE NYC_TAXI_DB;

MD

STREAMS_RESULT=$(run_snowsql "ACCOUNTADMIN" "ANALYTICS_WH" "SHOW STREAMS IN DATABASE NYC_TAXI_DB;" 2>/dev/null || echo "(requires ACCOUNTADMIN — run manually in Snowflake UI)")
echo "**Streams:**" >> "${OUTPUT_FILE}"
echo "${STREAMS_RESULT}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

TASKS_RESULT=$(run_snowsql "ACCOUNTADMIN" "ANALYTICS_WH" "SHOW TASKS IN DATABASE NYC_TAXI_DB;" 2>/dev/null || echo "(requires ACCOUNTADMIN — run manually in Snowflake UI)")
echo "**Tasks:**" >> "${OUTPUT_FILE}"
echo "${TASKS_RESULT}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

ok "Section 1 written"

# ── Section 2: Query Workload ─────────────────────────────────
log "Section 2: Query Workload (ACCOUNT_USAGE — may have 1-3hr lag)"

cat >> "${OUTPUT_FILE}" <<'MD'

---

## Section 2: Query Workload

Top 10 queries by total elapsed time over the last 7 days.

> If ACCOUNT_USAGE is unavailable (requires ACCOUNTADMIN + 1-3hr propagation delay),
> run `scripts/02_query_history.sql` manually in the Snowflake UI and paste results here.

MD

QUERY_HISTORY=$(run_snowsql "ACCOUNTADMIN" "ANALYTICS_WH" "
SELECT
    LEFT(query_text, 200)                                  AS query_preview,
    execution_status,
    COUNT(*)                                               AS executions,
    ROUND(AVG(total_elapsed_time))                        AS avg_ms,
    ROUND(MAX(total_elapsed_time))                        AS max_ms,
    ROUND(SUM(bytes_scanned) / 1024 / 1024 / 1024, 2)   AS total_gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'NYC_TAXI_DB'
  AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
  AND query_type NOT IN ('SHOW', 'DESCRIBE', 'USE', 'SET')
GROUP BY 1, 2
ORDER BY SUM(total_elapsed_time) DESC
LIMIT 10;
" 2>/dev/null || echo "(ACCOUNT_USAGE unavailable — run scripts/02_query_history.sql in Snowflake UI)")

echo "${QUERY_HISTORY}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

ok "Section 2 written"

# ── Section 3: Table Statistics ───────────────────────────────
log "Section 3: Table Statistics"

cat >> "${OUTPUT_FILE}" <<'MD'

---

## Section 3: Table Statistics

MD

# trips_raw stats
cat >> "${OUTPUT_FILE}" <<'MD'
### RAW.TRIPS_RAW

MD

TRIPS_RAW_STATS=$(run_snowsql "ANALYST_ROLE" "ANALYTICS_WH" "
SELECT
    COUNT(*)                                           AS total_rows,
    MIN(pickup_datetime)                               AS earliest_pickup,
    MAX(pickup_datetime)                               AS latest_pickup,
    ROUND(100.0 * SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)       AS pct_null_trip_id,
    ROUND(100.0 * SUM(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_pickup,
    ROUND(100.0 * SUM(CASE WHEN trip_metadata IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)  AS pct_null_metadata,
    ROUND(100.0 * SUM(CASE WHEN trip_metadata IS NOT NULL AND trip_metadata != 'null' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_populated_variant,
    COUNT(DISTINCT vendor_id)                         AS distinct_vendors,
    COUNT(DISTINCT pickup_location_id)                AS distinct_pickup_zones
FROM NYC_TAXI_DB.RAW.TRIPS_RAW;
")

echo "${TRIPS_RAW_STATS}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

# fact_trips stats
cat >> "${OUTPUT_FILE}" <<'MD'

### ANALYTICS.FACT_TRIPS

MD

FACT_TRIPS_STATS=$(run_snowsql "ANALYST_ROLE" "ANALYTICS_WH" "
SELECT
    COUNT(*)                                             AS total_rows,
    MIN(pickup_at)                                       AS earliest_pickup,
    MAX(pickup_at)                                       AS latest_pickup,
    ROUND(AVG(fare_amount), 2)                          AS avg_fare,
    ROUND(AVG(trip_distance), 2)                        AS avg_distance,
    ROUND(100.0 * SUM(CASE WHEN driver_rating IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_driver_rating
FROM NYC_TAXI_DB.ANALYTICS.FACT_TRIPS;
")

echo "${FACT_TRIPS_STATS}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

# agg_hourly stats
cat >> "${OUTPUT_FILE}" <<'MD'

### ANALYTICS.AGG_HOURLY_ZONE_TRIPS

MD

AGG_STATS=$(run_snowsql "ANALYST_ROLE" "ANALYTICS_WH" "
SELECT
    COUNT(*)                      AS total_rows,
    MIN(hour_bucket)              AS earliest_bucket,
    MAX(hour_bucket)              AS latest_bucket,
    COUNT(DISTINCT zone_id)       AS distinct_zones,
    ROUND(SUM(trip_count))        AS total_trips_recorded
FROM NYC_TAXI_DB.ANALYTICS.AGG_HOURLY_ZONE_TRIPS;
")

echo "${AGG_STATS}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

ok "Section 3 written"

# ── Section 4: Schema Compatibility Gaps ─────────────────────
log "Section 4: Schema Compatibility Gaps"

cat >> "${OUTPUT_FILE}" <<'MD'

---

## Section 4: Schema Compatibility Gaps

Auto-detected patterns that require migration attention.
Each gap maps to a worksheet or reference document for remediation.

MD

# Check for QUALIFY in stored procedures / views
cat >> "${OUTPUT_FILE}" <<'MD'
### Gap 1: QUALIFY Clauses

Searching view definitions for QUALIFY usage...

MD

QUALIFY_CHECK=$(run_snowsql "ANALYST_ROLE" "ANALYTICS_WH" "
SELECT
    table_schema,
    table_name,
    'Contains QUALIFY clause' AS gap_type,
    'Rewrite as subquery — see worksheets/03_schema_translation.md' AS remediation
FROM NYC_TAXI_DB.INFORMATION_SCHEMA.VIEWS
WHERE UPPER(view_definition) LIKE '%QUALIFY%'
ORDER BY table_schema, table_name;
")

echo "${QUALIFY_CHECK}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

cat >> "${OUTPUT_FILE}" <<'MD'

> Additionally, Q3 in `01-setup-snowflake/queries/` uses QUALIFY explicitly.
> ClickHouse has no QUALIFY — rewrite as subquery wrapping ROW_NUMBER().
> See: `docs/snowflake_vs_clickhouse.md` Gap 1.

### Gap 2: VARIANT Columns

MD

VARIANT_GAPS=$(run_snowsql "ANALYST_ROLE" "ANALYTICS_WH" "
SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    'VARIANT column' AS gap_type,
    'Store as String in ClickHouse; use JSONExtract* at query time' AS remediation
FROM NYC_TAXI_DB.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema IN ('RAW', 'STAGING', 'ANALYTICS')
  AND c.data_type = 'VARIANT'
ORDER BY c.table_schema, c.table_name;
")

echo "${VARIANT_GAPS}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

cat >> "${OUTPUT_FILE}" <<'MD'

### Gap 3: Snowflake Streams (CDC)

MD

STREAM_GAPS=$(run_snowsql "ACCOUNTADMIN" "ANALYTICS_WH" "
SELECT
    s.name                                              AS stream_name,
    s.source_name                                       AS source_table,
    'Snowflake Stream' AS gap_type,
    'Replace with producer cutover to ClickHouse (scripts/03_cutover.sh)' AS remediation
FROM TABLE(NYC_TAXI_DB.INFORMATION_SCHEMA.STREAMS_IN_SCHEMA('RAW')) s;
" 2>/dev/null || echo "(requires ACCOUNTADMIN — check manually: SHOW STREAMS IN SCHEMA NYC_TAXI_DB.RAW)")

echo "${STREAM_GAPS}" >> "${OUTPUT_FILE}"
echo "" >> "${OUTPUT_FILE}"

cat >> "${OUTPUT_FILE}" <<'MD'

### Gap 4: MERGE INTO Patterns

MERGE INTO does not exist in ClickHouse. dbt-clickhouse uses `delete_insert` incremental strategy
as the equivalent. Snowflake tasks using MERGE INTO must be rewritten.

Affected objects (based on known lab setup):
- `HOURLY_AGG_TASK` — uses MERGE INTO AGG_HOURLY_ZONE_TRIPS → rewrite as dbt incremental model
- `CDC_CONSUME_TASK` — uses MERGE INTO from stream → retired after producer cutover; live writes go directly to ClickHouse

See: `docs/snowflake_vs_clickhouse.md` Gap 4.

### Gap 5: Snowflake Tasks

Tasks are Snowflake's scheduled execution mechanism. ClickHouse has no equivalent.

Replacements:
- Tasks running dbt models → run dbt on your own schedule (cron, Airflow, dbt Cloud)
- Tasks consuming Streams → retired after producer cutover (live writes go directly to ClickHouse)
- REFRESHABLE MATERIALIZED VIEW in ClickHouse replaces Snowflake scheduled tasks that recalculate aggregates

### Gap 6: Date Function Differences

Minor syntax differences — all mechanical substitutions.
See the full translation table in `docs/snowflake_vs_clickhouse.md` Section 2, Gap 6.

Key substitutions needed in this workload:
- `DATE_TRUNC('hour', pickup_at)` → `toStartOfHour(pickup_at)`
- `DATEADD('day', -7, CURRENT_DATE)` → `today() - 7` or `addDays(today(), -7)`
- `DATEDIFF('minute', pickup_at, dropoff_at)` → `dateDiff('minute', pickup_at, dropoff_at)`

---

## Summary

| Gap | Objects Affected | Priority |
|-----|-----------------|----------|
| QUALIFY clause | Q3 query | High — breaks at parse time |
| VARIANT columns | TRIPS_RAW.TRIP_METADATA | High — all JSON queries affected |
| Snowflake Streams | TRIPS_CDC_STREAM | High — retired after producer cutover to ClickHouse |
| MERGE INTO | HOURLY_AGG_TASK, CDC_CONSUME_TASK | High — tasks must be rewritten |
| Snowflake Tasks | CDC_CONSUME_TASK, HOURLY_AGG_TASK | Medium — no ClickHouse equivalent |
| Date functions | Q1, Q3, Q4 queries | Low — mechanical substitutions |

MD

ok "Section 4 written"

# ── Done ──────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}════════════════════════════════════════════${RESET}"
echo -e "${GREEN}${BOLD}  Profile report complete.${RESET}"
echo ""
echo "  Output: ${OUTPUT_FILE}"
echo ""
echo "  Next steps:"
echo "  1. Review profile_report.md"
echo "  2. Work through the worksheets:"
echo "     worksheets/01_mergetree_engine_selection.md"
echo "     worksheets/02_sort_key_design.md"
echo "     worksheets/03_schema_translation.md"
echo "     worksheets/04_migration_wave_plan.md"
echo "  3. Fill in migration-plan.md"
echo -e "${GREEN}${BOLD}════════════════════════════════════════════${RESET}"
