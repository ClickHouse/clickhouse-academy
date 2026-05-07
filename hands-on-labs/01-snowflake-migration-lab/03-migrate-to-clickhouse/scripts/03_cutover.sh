#!/usr/bin/env bash
# ============================================================
# 03_cutover.sh
# Producer cutover: Snowflake → ClickHouse
#
# Steps:
#   1. Confirm intent (type "cutover")
#   2. Stop Snowflake trip producer
#   3. Run final dbt refresh on ClickHouse
#   4. Start ClickHouse producer
#   5. Verify new trips appear in ClickHouse
#
# Standalone usage:
#   source .env && source .clickhouse_state
#   bash scripts/03_cutover.sh
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

DBT_DIR="${DBT_DIR:-${LAB_DIR}/dbt/nyc_taxi_dbt_ch}"

# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------
clickhouse_query() {
  local query="$1"
  curl -s --fail \
    "https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" \
    -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    --data-binary "${query}"
}

DIVIDER="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ---------------------------------------------------------------------------
# Step 0: Confirmation
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}${DIVIDER}${RESET}"
echo -e "${BOLD}  NYC Taxi Lab — Producer Cutover: Snowflake → ClickHouse${RESET}"
echo -e "${BOLD}${DIVIDER}${RESET}"
echo ""
echo -e "${YELLOW}${BOLD}  WARNING: This will stop the Snowflake producer and redirect${RESET}"
echo -e "${YELLOW}${BOLD}  live trip writes to ClickHouse Cloud.${RESET}"
echo ""
echo -e "  To roll back after cutover:"
echo "    1. docker stop nyc_taxi_ch_producer"
echo "    2. docker start nyc_taxi_producer  (restarts Snowflake producer)"
echo ""
printf "  Type %b\"cutover\"%b to confirm, or Ctrl-C to abort: " "${BOLD}" "${RESET}"
read -r CONFIRM

if [[ "${CONFIRM}" != "cutover" ]]; then
  echo ""
  warn "Confirmation not received. Aborting."
  exit 0
fi

echo ""

# ---------------------------------------------------------------------------
# Step 1: Stop Snowflake producer
# ---------------------------------------------------------------------------
echo -e "${BLUE}${BOLD}[1/3] Stopping Snowflake trip producer…${RESET}"
if docker stop nyc_taxi_producer 2>/dev/null; then
  ok "nyc_taxi_producer stopped."
else
  warn "nyc_taxi_producer was not running (may already be stopped). Continuing."
fi
echo ""

# ---------------------------------------------------------------------------
# Step 2: Final dbt run on ClickHouse
# ---------------------------------------------------------------------------
echo -e "${BLUE}${BOLD}[2/3] Running final dbt refresh on ClickHouse…${RESET}"

if [[ ! -d "${DBT_DIR}" ]]; then
  warn "dbt directory not found at ${DBT_DIR} — skipping dbt run."
  warn "Run manually: cd dbt && dbt run --profiles-dir dbt"
else
  if ! command -v dbt >/dev/null 2>&1; then
    warn "dbt not found in PATH — skipping dbt run."
    warn "Run manually: cd ${DBT_DIR} && dbt run --profiles-dir \"${DBT_DIR}\""
  else
    info "Running: dbt run --profiles-dir \"${DBT_DIR}\""
    if (cd "${DBT_DIR}" && dbt run --profiles-dir "${DBT_DIR}"); then
      ok "dbt run completed successfully."
    else
      warn "dbt run exited with errors. Check output above."
      warn "You may need to re-run: cd ${DBT_DIR} && dbt run --profiles-dir \"${DBT_DIR}\""
    fi
  fi
fi
echo ""

# ---------------------------------------------------------------------------
# Step 4: Start ClickHouse producer
# ---------------------------------------------------------------------------
echo -e "${BLUE}${BOLD}[3/3] Starting ClickHouse trip producer…${RESET}"

# Build the ClickHouse producer image if not already present
PRODUCER_DIR="${LAB_DIR}/producer"
if ! docker image inspect nyc_taxi_ch_producer:latest >/dev/null 2>&1; then
  info "Building nyc_taxi_ch_producer image from ${PRODUCER_DIR}…"
  if ! docker build -t nyc_taxi_ch_producer:latest "${PRODUCER_DIR}"; then
    die "Docker build failed. Check ${PRODUCER_DIR}/Dockerfile and ensure Docker is running."
  fi
  ok "Image built: nyc_taxi_ch_producer:latest"
else
  info "Using existing nyc_taxi_ch_producer:latest image."
fi

# Remove any existing stopped container with the same name
if docker inspect nyc_taxi_ch_producer >/dev/null 2>&1; then
  info "Removing existing nyc_taxi_ch_producer container…"
  docker rm -f nyc_taxi_ch_producer >/dev/null 2>&1 || true
fi

# Write credentials to temp file to avoid exposing password in process list
_CH_ENV_FILE=$(mktemp)
chmod 600 "${_CH_ENV_FILE}"
cat > "${_CH_ENV_FILE}" <<EOF
CLICKHOUSE_HOST=${CLICKHOUSE_HOST}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-8443}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
TRIPS_PER_MINUTE=${TRIPS_PER_MINUTE:-60}
BATCH_INTERVAL_SECONDS=${BATCH_INTERVAL_SECONDS:-10}
EOF

if ! docker run -d --name nyc_taxi_ch_producer --restart unless-stopped --env-file "${_CH_ENV_FILE}" nyc_taxi_ch_producer:latest; then
  rm -f "${_CH_ENV_FILE}"
  die "Failed to start ClickHouse producer.\n  To roll back: docker start nyc_taxi_producer"
fi
rm -f "${_CH_ENV_FILE}"

ok "nyc_taxi_ch_producer container started."
echo ""

# ---------------------------------------------------------------------------
# Verification: check that new trips are appearing
# ---------------------------------------------------------------------------
info "Waiting 30 seconds for producer to emit the first batch…"
CH_COUNT_BEFORE=$(clickhouse_query "SELECT count() FROM default.trips_raw" | tr -d '[:space:]')
sleep 30
CH_COUNT_AFTER=$(clickhouse_query "SELECT count() FROM default.trips_raw" | tr -d '[:space:]')

if [[ "${CH_COUNT_AFTER}" =~ ^[0-9]+$ ]] && [[ "${CH_COUNT_BEFORE}" =~ ^[0-9]+$ ]]; then
  NEW_TRIPS=$(( CH_COUNT_AFTER - CH_COUNT_BEFORE ))
  if [[ "${NEW_TRIPS}" -gt 0 ]]; then
    ok "New trips detected: ${NEW_TRIPS} rows added in the last 30 seconds."

    # Run dbt now that live trips are flowing — populates agg_hourly_zone_trips
    # (the earlier dbt run in step 2 ran before the producer started, so the
    # rolling 2-hour aggregation table was still empty at that point)
    echo ""
    echo -e "${BLUE}${BOLD}[+] Refreshing analytics layer with live trip data…${RESET}"
    if [[ -d "${DBT_DIR}" ]] && command -v dbt >/dev/null 2>&1; then
      if (cd "${DBT_DIR}" && dbt run --profiles-dir "${DBT_DIR}"); then
        ok "dbt run complete — agg_hourly_zone_trips is now populated."
      else
        warn "dbt run exited with errors. Run manually:"
        warn "  cd ${DBT_DIR} && dbt run --profiles-dir \"${DBT_DIR}\""
      fi
    else
      warn "dbt not available — run manually to populate agg_hourly_zone_trips:"
      warn "  cd dbt/nyc_taxi_dbt_ch && dbt run"
    fi
  else
    warn "No new trips detected in ClickHouse after 30 seconds."
    warn "Troubleshooting tips:"
    warn "  • Check container logs: docker logs nyc_taxi_ch_producer"
    warn "  • Verify CLICKHOUSE_HOST is reachable from Docker"
    warn "  • Check that the producer image supports CLICKHOUSE_HOST env var"
    warn "  • Try: curl -u default:\${CLICKHOUSE_PASSWORD} https://\${CLICKHOUSE_HOST}:8443/"
  fi
else
  warn "Could not compare trip counts (before: '${CH_COUNT_BEFORE}', after: '${CH_COUNT_AFTER}')"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}${DIVIDER}${RESET}"
echo -e "${BOLD}  Cutover Summary${RESET}"
echo -e "${BOLD}${DIVIDER}${RESET}"
echo ""
echo -e "  ${GREEN}✓${RESET} Snowflake producer stopped:      ${BOLD}nyc_taxi_producer${RESET}"
echo -e "  ${GREEN}✓${RESET} ClickHouse producer running:     ${BOLD}nyc_taxi_ch_producer${RESET}"
echo ""
echo -e "  ${BOLD}Live trip data is now writing directly to ClickHouse Cloud.${RESET}"
echo ""
echo "  Monitor producer:"
echo "    docker logs -f nyc_taxi_ch_producer"
echo ""
info "Verify new trips are arriving in ClickHouse:"
info "  docker logs -f nyc_taxi_ch_producer"
info "  (new trips should appear every ~10 seconds)"
echo ""
echo -e "  ${YELLOW}To roll back (if something goes wrong):${RESET}"
echo "    1. docker stop nyc_taxi_ch_producer"
echo "    2. docker start nyc_taxi_producer  (restarts Snowflake producer)"
echo ""
echo -e "${BOLD}${DIVIDER}${RESET}"
echo ""
