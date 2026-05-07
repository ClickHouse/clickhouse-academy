#!/usr/bin/env bash
# ============================================================
# run_benchmark.sh
# Side-by-side benchmark: 7 queries × Snowflake vs ClickHouse
# Runs each query 3 times and reports the median latency.
#
# Standalone usage:
#   source .env && source .clickhouse_state
#   bash scripts/run_benchmark.sh
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(dirname "${SCRIPT_DIR}")"

# Source environment files
if [[ -f "${LAB_DIR}/.env" ]]; then
  set -a; source "${LAB_DIR}/.env"; set +a
fi
if [[ -f "${LAB_DIR}/.clickhouse_state" ]]; then
  set -a; source "${LAB_DIR}/.clickhouse_state"; set +a
fi

# ---------------------------------------------------------------------------
# Colors & log helpers
# ---------------------------------------------------------------------------
BOLD="\033[1m";    RESET="\033[0m"
GREEN="\033[1;32m"; RED="\033[1;31m"; YELLOW="\033[1;33m"; BLUE="\033[1;34m"

info() { echo -e "${BLUE}  ${*}${RESET}"; }
ok()   { echo -e "${GREEN}  ✓ ${*}${RESET}"; }
warn() { echo -e "${YELLOW}  ⚠ ${*}${RESET}"; }
die()  { echo -e "${RED}${BOLD}  ✗ ${*}${RESET}" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Validate required vars
# ---------------------------------------------------------------------------
: "${CLICKHOUSE_HOST:?CLICKHOUSE_HOST is not set. Source .clickhouse_state first.}"
: "${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD is not set.}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8443}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"

: "${SNOWFLAKE_ORG:?SNOWFLAKE_ORG is not set. Source .env first.}"
: "${SNOWFLAKE_ACCOUNT:?SNOWFLAKE_ACCOUNT is not set.}"
: "${SNOWFLAKE_USER:?SNOWFLAKE_USER is not set.}"
: "${SNOWFLAKE_PASSWORD:?SNOWFLAKE_PASSWORD is not set.}"

# ---------------------------------------------------------------------------
# Locate snowsql
# ---------------------------------------------------------------------------
SNOWSQL_CMD=""
if   command -v snowsql >/dev/null 2>&1;                          then SNOWSQL_CMD="snowsql"
elif [[ -x "/Applications/SnowSQL.app/Contents/MacOS/snowsql" ]]; then SNOWSQL_CMD="/Applications/SnowSQL.app/Contents/MacOS/snowsql"
fi
[[ -n "${SNOWSQL_CMD}" ]] || die "snowsql not found. Install from https://docs.snowflake.com/en/user-guide/snowsql-install-config"

# ---------------------------------------------------------------------------
# Portable millisecond timestamp (macOS-safe)
# ---------------------------------------------------------------------------
ms_now() {
  python3 -c "import time; print(int(time.time() * 1000))"
}

# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------
time_query_snowflake() {
  local query="$1"
  local start end
  start=$(ms_now)
  SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
    --accountname "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
    --username "${SNOWFLAKE_USER}" \
    --rolename ANALYST_ROLE \
    -q "USE WAREHOUSE ANALYTICS_WH; ${query}" \
    --option output_format=plain --option friendly=false >/dev/null 2>&1
  end=$(ms_now)
  echo $(( end - start ))
}

time_query_clickhouse() {
  local query="$1"
  local start end
  start=$(ms_now)
  curl -s --fail \
    "https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" \
    -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    --data-urlencode "query=${query}" \
    --data-urlencode "database=nyc_taxi_ch" >/dev/null 2>&1
  end=$(ms_now)
  echo $(( end - start ))
}

# ---------------------------------------------------------------------------
# Median of 3 values
# ---------------------------------------------------------------------------
median_of_3() {
  local a=$1 b=$2 c=$3
  if   (( a <= b && b <= c )); then echo "$b"
  elif (( a <= c && c <= b )); then echo "$c"
  elif (( b <= a && a <= c )); then echo "$a"
  elif (( b <= c && c <= a )); then echo "$c"
  elif (( c <= a && a <= b )); then echo "$a"
  else echo "$b"
  fi
}

# ---------------------------------------------------------------------------
# Format milliseconds as human-readable seconds string (e.g. "4.2s")
# ---------------------------------------------------------------------------
fmt_ms() {
  local ms=$1
  local sec=$(( ms / 1000 ))
  local frac=$(( (ms % 1000) / 100 ))
  echo "${sec}.${frac}s"
}

# ---------------------------------------------------------------------------
# Run one query on both systems, 3 times each, return median
# Sets globals: LAST_SF_MS  LAST_CH_MS  LAST_SF_SKIPPED
# ---------------------------------------------------------------------------
LAST_SF_MS=0
LAST_CH_MS=0
LAST_SF_SKIPPED=false

benchmark_query() {
  local label="$1"
  local sf_query="$2"
  local ch_query="$3"
  local allow_sf_fail="${4:-false}"

  LAST_SF_SKIPPED=false

  info "  Running ${label} on Snowflake (3 runs)…"
  if [[ "${allow_sf_fail}" == "true" ]]; then
    SF_R1=0; SF_R2=0; SF_R3=0
    if ! SF_R1=$(time_query_snowflake "${sf_query}" 2>/dev/null); then
      LAST_SF_MS=0; LAST_SF_SKIPPED=true
      warn "  ${label} Snowflake query failed — marking as N/A"
    else
      SF_R2=$(time_query_snowflake "${sf_query}" 2>/dev/null) || SF_R2=${SF_R1}
      SF_R3=$(time_query_snowflake "${sf_query}" 2>/dev/null) || SF_R3=${SF_R2}
      LAST_SF_MS=$(median_of_3 "${SF_R1}" "${SF_R2}" "${SF_R3}")
    fi
  else
    SF_R1=$(time_query_snowflake "${sf_query}")
    SF_R2=$(time_query_snowflake "${sf_query}")
    SF_R3=$(time_query_snowflake "${sf_query}")
    LAST_SF_MS=$(median_of_3 "${SF_R1}" "${SF_R2}" "${SF_R3}")
  fi

  info "  Running ${label} on ClickHouse (3 runs)…"
  CH_R1=$(time_query_clickhouse "${ch_query}")
  CH_R2=$(time_query_clickhouse "${ch_query}")
  CH_R3=$(time_query_clickhouse "${ch_query}")
  LAST_CH_MS=$(median_of_3 "${CH_R1}" "${CH_R2}" "${CH_R3}")
}

# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------

SF_Q1="SELECT DATE_TRUNC('hour', pickup_at) AS hour_bucket, pickup_borough, COUNT(*) AS trips, SUM(total_amount_usd) AS revenue FROM NYC_TAXI_DB.ANALYTICS.FACT_TRIPS WHERE pickup_at >= DATEADD('day', -7, CURRENT_TIMESTAMP()) AND pickup_borough IS NOT NULL GROUP BY 1, 2 ORDER BY 1 DESC, revenue DESC LIMIT 100"
CH_Q1="SELECT toStartOfHour(pickup_at) AS hour_bucket, pickup_borough, count() AS trips, sum(total_amount_usd) AS revenue FROM analytics.fact_trips FINAL WHERE pickup_at >= now() - INTERVAL 7 DAY AND pickup_borough != '' GROUP BY hour_bucket, pickup_borough ORDER BY hour_bucket DESC, revenue DESC LIMIT 100"

SF_Q2="SELECT pickup_at::DATE AS trip_date, COUNT(*) AS trips, AVG(AVG(trip_distance_miles)) OVER (ORDER BY pickup_at::DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_avg FROM NYC_TAXI_DB.ANALYTICS.FACT_TRIPS GROUP BY 1 ORDER BY 1 DESC LIMIT 365"
CH_Q2="SELECT toDate(pickup_at) AS trip_date, count() AS trips, avg(avg(trip_distance_miles)) OVER (ORDER BY trip_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_avg FROM analytics.fact_trips FINAL GROUP BY trip_date ORDER BY trip_date DESC LIMIT 365"

SF_Q3="SELECT trip_id, pickup_borough, total_amount_usd, ROW_NUMBER() OVER (PARTITION BY pickup_borough ORDER BY total_amount_usd DESC) AS rn FROM NYC_TAXI_DB.ANALYTICS.FACT_TRIPS WHERE pickup_at::DATE = CURRENT_DATE() - 1 QUALIFY rn <= 10"
CH_Q3="SELECT trip_id, pickup_borough, total_amount_usd, rn FROM (SELECT trip_id, pickup_borough, total_amount_usd, row_number() OVER (PARTITION BY pickup_borough ORDER BY total_amount_usd DESC) AS rn FROM analytics.fact_trips FINAL WHERE toDate(pickup_at) = today() - 1) WHERE rn <= 10"

SF_Q4="SELECT ROUND(TRIP_METADATA:driver.rating::FLOAT, 1) AS rating_bucket, COUNT(*) AS trips, AVG(TOTAL_AMOUNT) AS avg_fare FROM NYC_TAXI_DB.RAW.TRIPS_RAW WHERE TRIP_METADATA:driver.rating IS NOT NULL GROUP BY 1 ORDER BY 1"
CH_Q4="SELECT round(JSONExtractFloat(trip_metadata, 'driver', 'rating'), 1) AS rating_bucket, count() AS trips, avg(total_amount) AS avg_fare FROM nyc_taxi_ch.trips_raw WHERE JSONExtractFloat(trip_metadata, 'driver', 'rating') > 0 GROUP BY rating_bucket ORDER BY rating_bucket"

SF_Q5="SELECT CASE WHEN TRIP_METADATA:app.surge_multiplier::FLOAT >= 2.0 THEN 'High (2x+)' WHEN TRIP_METADATA:app.surge_multiplier::FLOAT >= 1.5 THEN 'Medium (1.5-2x)' WHEN TRIP_METADATA:app.surge_multiplier::FLOAT > 1.0 THEN 'Low (1-1.5x)' ELSE 'No Surge' END AS surge_cat, COUNT(*) AS trips, ROUND(AVG(TOTAL_AMOUNT), 2) AS avg_fare FROM NYC_TAXI_DB.RAW.TRIPS_RAW GROUP BY 1 ORDER BY 2 DESC"
CH_Q5="SELECT CASE WHEN JSONExtractFloat(trip_metadata,'app','surge_multiplier') >= 2.0 THEN 'High (2x+)' WHEN JSONExtractFloat(trip_metadata,'app','surge_multiplier') >= 1.5 THEN 'Medium (1.5-2x)' WHEN JSONExtractFloat(trip_metadata,'app','surge_multiplier') > 1.0 THEN 'Low (1-1.5x)' ELSE 'No Surge' END AS surge_cat, count() AS trips, round(avg(total_amount),2) AS avg_fare FROM nyc_taxi_ch.trips_raw GROUP BY surge_cat ORDER BY trips DESC"

SF_Q6="SELECT DATE_TRUNC('hour', pickup_at) AS hour_bucket, pickup_location_id AS zone_id, COUNT(*) AS trips, SUM(total_amount_usd) AS revenue FROM NYC_TAXI_DB.ANALYTICS.FACT_TRIPS WHERE pickup_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) GROUP BY 1, 2 ORDER BY 1 DESC, revenue DESC LIMIT 100"
CH_Q6="SELECT toStartOfHour(pickup_at) AS hour_bucket, pickup_location_id AS zone_id, count() AS trips, sum(total_amount_usd) AS revenue FROM analytics.fact_trips FINAL WHERE pickup_at >= now() - INTERVAL 24 HOUR GROUP BY hour_bucket, zone_id ORDER BY hour_bucket DESC, revenue DESC LIMIT 100"

SF_Q7="SELECT METADATA\$ACTION, COUNT(*) AS changes FROM NYC_TAXI_DB.RAW.TRIPS_CDC_STREAM GROUP BY 1"
CH_Q7="SELECT count() AS total_trips, max(ingested_at) AS latest_ingest, countIf(ingested_at >= now() - INTERVAL 5 MINUTE) AS recent_trips FROM nyc_taxi_ch.trips_raw"

# ---------------------------------------------------------------------------
# Query labels / descriptions
# ---------------------------------------------------------------------------
declare -a Q_LABELS=(
  "Q1  Hourly revenue by borough"
  "Q2  Rolling 7-day avg distance"
  "Q3  Top 10 trips (QUALIFY→subquery)"
  "Q4  Driver ratings (JSON flatten)"
  "Q5  Surge pricing (VARIANT)"
  "Q6  Hourly aggregation (MERGE→RMT)"
  "Q7  CDC/live data freshness"
)
declare -a Q_IDS=( Q1 Q2 Q3 Q4 Q5 Q6 Q7 )
declare -a Q_DESC=(
  "Hourly revenue by borough"
  "Rolling 7-day avg distance"
  "Top 10 trips (QUALIFY→subquery)"
  "Driver ratings (JSON flatten)"
  "Surge pricing (VARIANT)"
  "Hourly aggregation (MERGE→RMT)"
  "CDC/live data freshness"
)

DIVIDER="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
SEP="────────────────────────────────────────────────────────────────────────"

echo ""
echo -e "${BOLD}${DIVIDER}${RESET}"
echo -e "${BOLD}  NYC Taxi Lab — Query Benchmark: Snowflake vs ClickHouse${RESET}"
echo -e "${BOLD}  (median of 3 runs each)${RESET}"
echo -e "${BOLD}${DIVIDER}${RESET}"
echo ""
info "Note: Snowflake times include CLI session setup (~1-3s per query). For query-engine-only comparison, use the Snowflake UI Query History."
echo ""

# ---------------------------------------------------------------------------
# Run all 7 benchmarks
# ---------------------------------------------------------------------------

declare -a SF_RESULTS=()
declare -a CH_RESULTS=()
declare -a SF_SKIPPED=()

# Q1
benchmark_query "Q1" "${SF_Q1}" "${CH_Q1}"
SF_RESULTS+=( "${LAST_SF_MS}" ); CH_RESULTS+=( "${LAST_CH_MS}" ); SF_SKIPPED+=( "${LAST_SF_SKIPPED}" )

# Q2
benchmark_query "Q2" "${SF_Q2}" "${CH_Q2}"
SF_RESULTS+=( "${LAST_SF_MS}" ); CH_RESULTS+=( "${LAST_CH_MS}" ); SF_SKIPPED+=( "${LAST_SF_SKIPPED}" )

# Q3
benchmark_query "Q3" "${SF_Q3}" "${CH_Q3}"
SF_RESULTS+=( "${LAST_SF_MS}" ); CH_RESULTS+=( "${LAST_CH_MS}" ); SF_SKIPPED+=( "${LAST_SF_SKIPPED}" )

# Q4
benchmark_query "Q4" "${SF_Q4}" "${CH_Q4}"
SF_RESULTS+=( "${LAST_SF_MS}" ); CH_RESULTS+=( "${LAST_CH_MS}" ); SF_SKIPPED+=( "${LAST_SF_SKIPPED}" )

# Q5
benchmark_query "Q5" "${SF_Q5}" "${CH_Q5}"
SF_RESULTS+=( "${LAST_SF_MS}" ); CH_RESULTS+=( "${LAST_CH_MS}" ); SF_SKIPPED+=( "${LAST_SF_SKIPPED}" )

# Q6
benchmark_query "Q6" "${SF_Q6}" "${CH_Q6}"
SF_RESULTS+=( "${LAST_SF_MS}" ); CH_RESULTS+=( "${LAST_CH_MS}" ); SF_SKIPPED+=( "${LAST_SF_SKIPPED}" )

# Q7 — Snowflake CDC stream may fail if stream is consumed; allow failure
benchmark_query "Q7" "${SF_Q7}" "${CH_Q7}" "true"
SF_RESULTS+=( "${LAST_SF_MS}" ); CH_RESULTS+=( "${LAST_CH_MS}" ); SF_SKIPPED+=( "${LAST_SF_SKIPPED}" )

# ---------------------------------------------------------------------------
# Build result table & CSV
# ---------------------------------------------------------------------------
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV_FILE="${SCRIPT_DIR}/benchmark_results_${TIMESTAMP}.csv"

{
  echo "query,description,snowflake_ms,clickhouse_ms,speedup"
} > "${CSV_FILE}"

echo ""
echo -e "${BOLD}${DIVIDER}${RESET}"
echo -e "${BOLD}  NYC Taxi Lab — Query Benchmark: Snowflake vs ClickHouse${RESET}"
echo -e "${BOLD}  (median of 3 runs each)${RESET}"
echo -e "${BOLD}${DIVIDER}${RESET}"
printf "%-38s  %-12s  %-12s  %s\n" "Query" "Snowflake" "ClickHouse" "Speedup"
echo "${SEP}"

TOTAL_SF=0
TOTAL_CH=0
TOTAL_SF_VALID=0
SF_SKIPPED_ANY=false

for i in 0 1 2 3 4 5 6; do
  sf_ms="${SF_RESULTS[$i]}"
  ch_ms="${CH_RESULTS[$i]}"
  skipped="${SF_SKIPPED[$i]}"
  label="${Q_LABELS[$i]}"
  qid="${Q_IDS[$i]}"
  desc="${Q_DESC[$i]}"

  if [[ "${ch_ms}" -gt 0 ]]; then
    ch_fmt=$(fmt_ms "${ch_ms}")
  else
    ch_fmt="ERR"
  fi

  if [[ "${skipped}" == "true" ]] || [[ "${sf_ms}" -eq 0 ]]; then
    sf_fmt="N/A"
    speedup_str="N/A"
    SF_SKIPPED_ANY=true
    echo "query,${desc},N/A,${ch_ms},N/A" >> "${CSV_FILE}"
    printf "%-38s  %-12s  %-12s  %s\n" "${label}" "${sf_fmt}" "${ch_fmt}" "${speedup_str}"
  else
    sf_fmt=$(fmt_ms "${sf_ms}")
    if [[ "${ch_ms}" -gt 0 ]]; then
      speedup=$(( sf_ms / ch_ms ))
      speedup_str="${speedup}x"
    else
      speedup=0
      speedup_str="N/A"
    fi
    echo "${qid},${desc},${sf_ms},${ch_ms},${speedup}" >> "${CSV_FILE}"
    printf "%-38s  %-12s  %-12s  %s\n" "${label}" "${sf_fmt}" "${ch_fmt}" "${speedup_str}"
    TOTAL_SF=$(( TOTAL_SF + sf_ms ))
    TOTAL_CH=$(( TOTAL_CH + ch_ms ))
    (( TOTAL_SF_VALID++ )) || true
  fi
done

echo "${SEP}"

# Totals row
TOTAL_SF_FMT=$(fmt_ms "${TOTAL_SF}")
TOTAL_CH_FMT=$(fmt_ms "${TOTAL_CH}")
SPEEDUP_NOTE=""
if [[ "${SF_SKIPPED_ANY}" == "true" ]]; then
  # Only sum queries where both systems were measured
  SPEEDUP_NOTE=" (excluding N/A queries)"
fi
if [[ "${TOTAL_CH}" -gt 0 ]]; then
  TOTAL_SPEEDUP=$(( TOTAL_SF / TOTAL_CH ))
  TOTAL_SPEEDUP_STR="${TOTAL_SPEEDUP}x avg${SPEEDUP_NOTE}"
else
  TOTAL_SPEEDUP_STR="N/A"
fi

printf "%-38s  %-12s  %-12s  %s\n" "Total" "${TOTAL_SF_FMT}" "${TOTAL_CH_FMT}" "${TOTAL_SPEEDUP_STR}"
echo -e "${BOLD}${DIVIDER}${RESET}"
echo ""
echo -e "  Results written to: ${BOLD}${CSV_FILE}${RESET}"
echo ""

# Also append totals to CSV
echo "TOTAL,All queries,${TOTAL_SF},${TOTAL_CH},${TOTAL_SPEEDUP:-N/A}" >> "${CSV_FILE}"

ok "Benchmark complete. CSV saved to: ${CSV_FILE}"
echo ""
