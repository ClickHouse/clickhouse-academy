#!/usr/bin/env bash
# ============================================================
# verify_environment.sh
# Comprehensive Snowflake environment verification checks
# Run after setup.sh to validate all objects are in place
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(dirname "${SCRIPT_DIR}")"

# Auto-source .env if present
if [[ -f "${LAB_DIR}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${LAB_DIR}/.env"
  set +a
fi

# Colors for output
BOLD="\033[1m"; RESET="\033[0m"
BLUE="\033[1;34m"; GREEN="\033[1;32m"; YELLOW="\033[1;33m"; RED="\033[1;31m"

check()   { echo -e "${BLUE}✓${RESET} $*"; }
pass()    { echo -e "${GREEN}  ✓ $*${RESET}"; }
fail()    { echo -e "${RED}  ✗ $*${RESET}"; return 1; }
warn()    { echo -e "${YELLOW}  ⚠ $*${RESET}"; }

# Determine SnowSQL command
SNOWSQL_CMD=""
if   command -v snowsql >/dev/null 2>&1;                         then SNOWSQL_CMD="snowsql"
elif [[ -x "/Applications/SnowSQL.app/Contents/MacOS/snowsql" ]]; then SNOWSQL_CMD="/Applications/SnowSQL.app/Contents/MacOS/snowsql"
fi

if [[ -z "${SNOWSQL_CMD}" ]]; then
  echo -e "${RED}${BOLD}ERROR: snowsql not found${RESET}"
  echo "Install SnowSQL: https://docs.snowflake.com/en/user-guide/snowsql-install-config"
  exit 1
fi

# Verify credentials are set
if [[ -z "${SNOWFLAKE_ORG:-}" ]] || [[ -z "${SNOWFLAKE_ACCOUNT:-}" ]] || [[ -z "${SNOWFLAKE_USER:-}" ]]; then
  echo -e "${RED}${BOLD}ERROR: Missing credentials${RESET}"
  echo "Source .env first: source ${LAB_DIR}/.env"
  exit 1
fi

echo -e "\n${BOLD}NYC Taxi Snowflake Lab — Environment Verification${RESET}"
echo "════════════════════════════════════════════════════"
echo ""

# Helper to run snowsql query
run_query() {
  local title="$1" query="$2" role="${3:-SYSADMIN}"
  SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
    -a "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
    -u "${SNOWFLAKE_USER}" \
    --rolename "${role}" \
    -q "USE DATABASE NYC_TAXI_DB; ${query}" \
    --option output_format=plain \
    --option friendly=false 2>/dev/null | awk 'NF && !/^[A-Z]/ && !/Statement/ && !/Row\(s\)/ && !/status/ && !/Time/ && !/^000/ {print; exit}' || echo "ERROR"
}

PASS_COUNT=0
FAIL_COUNT=0

# 1. Database & Schemas
echo -e "${BLUE}1. Database & Schemas${RESET}"
SCHEMAS=$(run_query "schemas" "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN ('RAW', 'STAGING', 'ANALYTICS');" | xargs)
if [[ "${SCHEMAS}" == "3" ]]; then
  pass "NYC_TAXI_DB has 3 schemas (RAW, STAGING, ANALYTICS)"
  ((PASS_COUNT++))
else
  fail "NYC_TAXI_DB missing schemas (expected 3, got ${SCHEMAS})"
  ((FAIL_COUNT++))
fi

# 2. Tables & Row Counts
echo -e "\n${BLUE}2. Tables & Data${RESET}"
TRIPS_RAW=$(run_query "trips_raw" "SELECT COALESCE(row_count, 0) FROM information_schema.tables WHERE table_name = 'TRIPS_RAW' AND table_schema = 'RAW';" | xargs)
if [[ "${TRIPS_RAW}" =~ ^[0-9]+$ ]] && [[ "${TRIPS_RAW}" -gt 0 ]]; then
  pass "TRIPS_RAW: ${TRIPS_RAW} rows"
  ((PASS_COUNT++))
else
  fail "TRIPS_RAW is empty or missing (got: ${TRIPS_RAW})"
  ((FAIL_COUNT++))
fi

FACT_TRIPS=$(run_query "fact_trips" "SELECT COALESCE(row_count, 0) FROM information_schema.tables WHERE table_name = 'FACT_TRIPS' AND table_schema = 'ANALYTICS';" | xargs)
if [[ "${FACT_TRIPS}" =~ ^[0-9]+$ ]] && [[ "${FACT_TRIPS}" -gt 0 ]]; then
  pass "FACT_TRIPS: ${FACT_TRIPS} rows"
  ((PASS_COUNT++))
else
  fail "FACT_TRIPS is empty or missing (got: ${FACT_TRIPS})"
  ((FAIL_COUNT++))
fi

# 3. Dimensions
echo -e "\n${BLUE}3. Dimension Tables${RESET}"
DIMS=$(run_query "dims" "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'ANALYTICS' AND table_name LIKE 'DIM_%';" | xargs)
if [[ "${DIMS}" =~ ^[0-9]+$ ]] && [[ "${DIMS}" -ge 4 ]]; then
  pass "Found ${DIMS} dimension tables"
  ((PASS_COUNT++))
else
  fail "Expected 4+ dimension tables, found ${DIMS}"
  ((FAIL_COUNT++))
fi

# 4. CDC Stream
echo -e "\n${BLUE}4. CDC Stream${RESET}"
STREAM=$(SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
  -a "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
  -u "${SNOWFLAKE_USER}" \
  --rolename SYSADMIN \
  -q "USE DATABASE NYC_TAXI_DB; SHOW STREAMS LIKE 'TRIPS_CDC_STREAM' IN SCHEMA RAW;" \
  --option output_format=plain \
  --option friendly=false 2>/dev/null | grep -c "TRIPS_CDC_STREAM" | xargs)
if [[ "${STREAM}" == "1" ]]; then
  pass "TRIPS_CDC_STREAM exists"
  ((PASS_COUNT++))
else
  fail "TRIPS_CDC_STREAM not found (got: ${STREAM})"
  ((FAIL_COUNT++))
fi

# 5. Tasks Status
echo -e "\n${BLUE}5. Scheduled Tasks${RESET}"
CDC_STATE=$(SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
  -a "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
  -u "${SNOWFLAKE_USER}" \
  --rolename ACCOUNTADMIN \
  -q "SHOW TASKS LIKE 'CDC_CONSUME_TASK' IN SCHEMA NYC_TAXI_DB.RAW;" \
  --option output_format=plain \
  --option friendly=false 2>/dev/null \
  | grep "CDC_CONSUME_TASK" | grep -oE '\b(started|suspended)\b' | head -1 | xargs)
if [[ "${CDC_STATE}" =~ ^(started|suspended)$ ]]; then
  if [[ "${CDC_STATE}" == "started" ]]; then
    pass "CDC_CONSUME_TASK: ${CDC_STATE}"
    ((PASS_COUNT++))
  else
    warn "CDC_CONSUME_TASK is suspended (should be started)"
    ((FAIL_COUNT++))
  fi
else
  fail "CDC_CONSUME_TASK not found or in unknown state: ${CDC_STATE}"
  ((FAIL_COUNT++))
fi

HOURLY_STATE=$(SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
  -a "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
  -u "${SNOWFLAKE_USER}" \
  --rolename ACCOUNTADMIN \
  -q "SHOW TASKS LIKE 'HOURLY_AGG_TASK' IN SCHEMA NYC_TAXI_DB.STAGING;" \
  --option output_format=plain \
  --option friendly=false 2>/dev/null \
  | grep "HOURLY_AGG_TASK" | grep -oE '\b(started|suspended)\b' | head -1 | xargs)
if [[ "${HOURLY_STATE}" =~ ^(started|suspended)$ ]]; then
  if [[ "${HOURLY_STATE}" == "started" ]]; then
    pass "HOURLY_AGG_TASK: ${HOURLY_STATE}"
    ((PASS_COUNT++))
  else
    warn "HOURLY_AGG_TASK is suspended (expected after dbt)"
    ((FAIL_COUNT++))
  fi
else
  fail "HOURLY_AGG_TASK not found or in unknown state: ${HOURLY_STATE}"
  ((FAIL_COUNT++))
fi

# 6. Recent CDC Activity
echo -e "\n${BLUE}6. CDC Activity (Last Hour)${RESET}"
CDC_LATEST=$(SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
  -a "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
  -u "${SNOWFLAKE_USER}" \
  --rolename ACCOUNTADMIN \
  -q "SELECT COALESCE(TO_VARCHAR(CONVERT_TIMEZONE('UTC', MAX(COMPLETED_TIME)), 'YYYY-MM-DD HH24:MI:SS') || ' UTC', 'Never') FROM TABLE(NYC_TAXI_DB.information_schema.task_history(TASK_NAME => 'CDC_CONSUME_TASK', RESULT_LIMIT => 10));" \
  --option output_format=plain \
  --option friendly=false 2>/dev/null \
  | awk 'NF && !/^[A-Z]/ && !/Statement/ && !/Row/ && !/Time/ && !/^000/ {print; exit}' | xargs)
if [[ "${CDC_LATEST}" != "Never" ]] && [[ "${CDC_LATEST}" != "ERROR" ]] && [[ -n "${CDC_LATEST}" ]]; then
  pass "CDC_CONSUME_TASK last ran: ${CDC_LATEST}"
  ((PASS_COUNT++))
else
  warn "CDC_CONSUME_TASK has not run yet (check logs if task is SUSPENDED)"
  ((FAIL_COUNT++))
fi

# 7. Producer Activity
echo -e "\n${BLUE}7. Trip Producer Activity${RESET}"
LATEST_TRIP=$(run_query "latest_trip" "SELECT TO_VARCHAR(MAX(INGESTED_AT), 'YYYY-MM-DD HH24:MI:SS') || ' UTC' FROM RAW.TRIPS_RAW;" | xargs)
if [[ "${LATEST_TRIP}" != "ERROR" ]] && [[ -n "${LATEST_TRIP}" ]] && [[ "${LATEST_TRIP}" != "NULL" ]]; then
  pass "Latest trip inserted: ${LATEST_TRIP}"
  ((PASS_COUNT++))
else
  fail "Could not retrieve latest trip timestamp (got: ${LATEST_TRIP})"
  ((FAIL_COUNT++))
fi

# 8. Superset Health
echo -e "\n${BLUE}8. Superset (BI Layer)${RESET}"
if command -v curl >/dev/null 2>&1; then
  SUPERSET_RESPONSE=$(curl -s http://localhost:8088/health 2>/dev/null || echo "")
  SUPERSET_HEALTH=$(echo "${SUPERSET_RESPONSE}" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('status', 'unknown'))" 2>/dev/null \
    || echo "${SUPERSET_RESPONSE}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')
  if [[ "${SUPERSET_HEALTH}" == "ok" ]]; then
    pass "Superset is healthy (http://localhost:8088)"
    ((PASS_COUNT++))
  else
    warn "Superset is offline or not responding (http://localhost:8088 — expected if Docker is not running)"
    ((FAIL_COUNT++))
  fi
else
  warn "curl not available — skipping Superset health check"
fi

# Summary
echo ""
echo "════════════════════════════════════════════════════"
echo -e "${GREEN}Passed: ${PASS_COUNT}${RESET}  ${RED}Failed: ${FAIL_COUNT}${RESET}"
echo ""

if [[ ${FAIL_COUNT} -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}✓ All verifications passed!${RESET}"
  echo "The lab environment is ready for migration to ClickHouse."
  exit 0
else
  echo -e "${YELLOW}${BOLD}⚠ Some checks failed — see warnings above${RESET}"
  echo "Common issues:"
  echo "  • Tasks are SUSPENDED: Resume with ALTER TASK <name> RESUME;"
  echo "  • Producer not running: Check docker logs nyc_taxi_producer"
  echo "  • Superset offline: Start with: cd superset && docker-compose up -d"
  exit 1
fi
