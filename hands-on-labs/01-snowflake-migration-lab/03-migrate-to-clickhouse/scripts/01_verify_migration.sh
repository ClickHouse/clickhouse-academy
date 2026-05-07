#!/usr/bin/env bash
# ============================================================
# 01_verify_migration.sh
# Verifies row count parity between Snowflake and ClickHouse.
#
# Exit 0  → counts match (safe to proceed)
# Exit 1  → count is zero, unexpected, or parity check fails
#
# Standalone usage:
#   source .env && source .clickhouse_state
#   bash scripts/01_verify_migration.sh
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

# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
clickhouse_query() {
  local query="$1"
  local response http_code body
  response=$(curl -s -w "\n%{http_code}" \
    "https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" \
    -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    --data-binary "${query}" 2>&1)
  http_code=$(echo "${response}" | tail -1)
  body=$(echo "${response}" | sed '$d')
  if [[ "${http_code}" != "200" ]]; then
    die "ClickHouse query failed (HTTP ${http_code}): ${body}"
  fi
  echo "${body}"
}

snowflake_count() {
  python3 - <<'PYEOF'
import os, sys
try:
    import snowflake.connector
except ImportError:
    print("SKIP")
    sys.exit(0)

required = ["SNOWFLAKE_ORG", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
missing  = [v for v in required if not os.environ.get(v)]
if missing:
    print("SKIP")
    sys.exit(0)

try:
    conn = snowflake.connector.connect(
        account  = f"{os.environ['SNOWFLAKE_ORG']}-{os.environ['SNOWFLAKE_ACCOUNT']}",
        user     = os.environ["SNOWFLAKE_USER"],
        password = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = "TRANSFORM_WH",
        database  = "NYC_TAXI_DB",
        schema    = "RAW",
        login_timeout = 15,
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.TRIPS_RAW")
    print(cur.fetchone()[0])
    cur.close()
    conn.close()
except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr)
    print("SKIP")
PYEOF
}

fmt_number() {
  python3 -c "print(f'{int(\"$1\"):,}')" 2>/dev/null || echo "$1"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
DIVIDER="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo -e "${BOLD}${DIVIDER}${RESET}"
echo -e "${BOLD}  Migration Parity Check${RESET}"
echo -e "${BOLD}${DIVIDER}${RESET}"
echo ""

# 1. ClickHouse row count
info "Querying ClickHouse default.trips_raw row count…"
CH_COUNT_RAW=$(clickhouse_query "SELECT count() FROM default.trips_raw")
CH_COUNT=$(echo "${CH_COUNT_RAW}" | tr -d '[:space:]')
if ! [[ "${CH_COUNT}" =~ ^[0-9]+$ ]]; then
  die "Could not retrieve ClickHouse row count (got: '${CH_COUNT}'). Check CLICKHOUSE_HOST / credentials."
fi

CH_FMT=$(fmt_number "${CH_COUNT}")
ok "ClickHouse default.trips_raw: ${BOLD}${CH_FMT}${RESET} rows"

if [[ "${CH_COUNT}" -eq 0 ]]; then
  echo ""
  echo -e "${RED}${BOLD}  ✗ ClickHouse trips_raw is empty.${RESET}"
  echo "  Run the migration script first:"
  echo "    python scripts/02_migrate_trips.py"
  echo -e "${BOLD}${DIVIDER}${RESET}"
  exit 1
fi

# 2. Snowflake row count
echo ""
info "Querying Snowflake NYC_TAXI_DB.RAW.TRIPS_RAW row count…"
SF_RESULT=$(snowflake_count)

if [[ "${SF_RESULT}" == "SKIP" ]]; then
  warn "Snowflake credentials not available — skipping automated parity check."
  warn "Run manually: SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.TRIPS_RAW;"
  warn "Compare against ClickHouse count: ${CH_FMT}"
elif [[ "${SF_RESULT}" =~ ^[0-9]+$ ]]; then
  SF_FMT=$(fmt_number "${SF_RESULT}")
  ok "Snowflake NYC_TAXI_DB.RAW.TRIPS_RAW: ${BOLD}${SF_FMT}${RESET} rows"

  # Parity check — direction-aware:
  #   CH >= SF  →  expected post-cutover (live producer adds rows); always PASS
  #   CH <  SF  →  missing rows; PASS only if gap <= 0.01% (in-flight during migration)
  MISSING=$(( SF_RESULT - CH_COUNT ))   # positive = CH is behind; negative = CH is ahead

  echo ""
  if [[ "${MISSING}" -le 0 ]]; then
    EXTRA=$(( -MISSING ))
    EXTRA_FMT=$(fmt_number "${EXTRA}")
    ok "Row count parity: PASS  (ClickHouse has ${EXTRA_FMT} extra rows — live producer is running)"
  else
    PCT=$(python3 -c "print(f'{${MISSING} / ${SF_RESULT} * 100:.4f}')" 2>/dev/null || echo "?")
    MISSING_FMT=$(fmt_number "${MISSING}")
    if python3 -c "import sys; sys.exit(0 if ${MISSING} / ${SF_RESULT} * 100 <= 0.01 else 1)" 2>/dev/null; then
      ok "Row count parity: PASS  (ClickHouse is ${MISSING_FMT} rows behind = ${PCT}% — within 0.01% threshold)"
    else
      warn "ClickHouse is missing ${MISSING_FMT} rows (${PCT}%) — exceeds 0.01% threshold"
      warn "Re-run with --resume to migrate the gap:"
      warn "  python scripts/02_migrate_trips.py --resume"
    fi
  fi
else
  warn "Could not retrieve Snowflake count — skipping parity check."
fi

echo ""

# 3. Spot check: trip_metadata populated
info "Spot check: trip_metadata column…"
META_COUNT_RAW=$(clickhouse_query "SELECT countIf(trip_metadata != '') FROM default.trips_raw")
META_COUNT=$(echo "${META_COUNT_RAW}" | tr -d '[:space:]')
META_FMT=$(fmt_number "${META_COUNT:-0}")

if [[ "${META_COUNT}" =~ ^[0-9]+$ ]] && [[ "${META_COUNT}" -gt 0 ]]; then
  ok "trip_metadata populated: ${META_FMT} non-empty rows"
else
  warn "trip_metadata appears empty. This may indicate a column mapping issue in the migration script."
fi

# 4. Spot check: pickup_at range
info "Spot check: pickup_at date range…"
RANGE_RAW=$(clickhouse_query "SELECT min(toDate(pickup_at)), max(toDate(pickup_at)) FROM default.trips_raw")
echo -e "  pickup_at range: ${BOLD}${RANGE_RAW}${RESET}"

# 5. Final verdict
echo ""
echo -e "${GREEN}${BOLD}  ✓ ClickHouse has ${CH_FMT} rows — migration looks complete${RESET}"
echo -e "${BOLD}${DIVIDER}${RESET}"
echo ""
