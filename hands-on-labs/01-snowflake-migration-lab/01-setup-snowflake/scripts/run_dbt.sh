#!/usr/bin/env bash
# ============================================================
# run_dbt.sh
# Runs dbt incrementally on a configurable interval.
# Designed to be left running in a terminal during the lab
# so FACT_TRIPS and AGG_HOURLY_ZONE_TRIPS stay current as
# the trip producer inserts new rows.
#
# Usage:
#   ./scripts/run_dbt.sh           # default: every 5 minutes
#   ./scripts/run_dbt.sh --interval 15m
#   ./scripts/run_dbt.sh --once    # run once and exit
#   ./scripts/run_dbt.sh --test    # run tests after each run
# ============================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBT_DIR="${SCRIPT_DIR}/dbt/nyc_taxi_dbt"

# Auto-source .env if present — so you can run scripts directly without
# manually running `source .env` first.
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/.env"
  set +a
fi

# ── defaults ─────────────────────────────────────────────────────────────────
INTERVAL=300   # seconds (5 minutes)
RUN_ONCE=false
RUN_TESTS=false

# ── parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval)
      RAW="$2"; shift 2
      # Accept 30s, 5m, 1h or plain seconds
      if   [[ "$RAW" =~ ^([0-9]+)s$ ]]; then INTERVAL="${BASH_REMATCH[1]}"
      elif [[ "$RAW" =~ ^([0-9]+)m$ ]]; then INTERVAL=$(( BASH_REMATCH[1] * 60 ))
      elif [[ "$RAW" =~ ^([0-9]+)h$ ]]; then INTERVAL=$(( BASH_REMATCH[1] * 3600 ))
      elif [[ "$RAW" =~ ^[0-9]+$ ]];    then INTERVAL="$RAW"
      else echo "Invalid --interval value: $RAW (use 30s, 5m, 1h, or seconds)"; exit 1
      fi ;;
    --once)    RUN_ONCE=true;  shift ;;
    --test)    RUN_TESTS=true; shift ;;
    *) echo "Usage: $0 [--interval 5m] [--once] [--test]"; exit 1 ;;
  esac
done

# ── resolve dbt binary ────────────────────────────────────────────────────────
DBT_CMD=""
if   [[ -x "${SCRIPT_DIR}/.venv/bin/dbt" ]]; then DBT_CMD="${SCRIPT_DIR}/.venv/bin/dbt"
elif command -v dbt >/dev/null 2>&1;          then DBT_CMD="dbt"
fi
if [[ -z "${DBT_CMD}" ]]; then
  echo "ERROR: dbt not found. Run: pip install dbt-snowflake"
  exit 1
fi

# ── colour helpers ────────────────────────────────────────────────────────────
BOLD='\033[1m'; CYAN='\033[0;36m'; GREEN='\033[0;32m'
YELLOW='\033[0;33m'; RED='\033[0;31m'; RESET='\033[0m'

_ts()     { date '+%H:%M:%S'; }
_header() { echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════${RESET}"; echo -e "${BOLD}${CYAN}  $*${RESET}"; echo -e "${BOLD}${CYAN}══════════════════════════════════════════${RESET}"; }
_ok()     { echo -e "${GREEN}  ✓ $*${RESET}"; }
_warn()   { echo -e "${YELLOW}  ⚠ $*${RESET}"; }
_err()    { echo -e "${RED}  ✗ $*${RESET}"; }
_info()   { echo -e "  $*"; }

# ── single dbt run ────────────────────────────────────────────────────────────
run_dbt() {
  local run_num="$1"
  local start_ts; start_ts=$(_ts)
  local start_epoch; start_epoch=$(date +%s)

  _header "dbt run #${run_num}  —  ${start_ts}  $(date '+%Y-%m-%d')"
  _info "Interval: ${INTERVAL}s | Tests: ${RUN_TESTS} | Dir: ${DBT_DIR}"
  echo ""

  cd "${DBT_DIR}"

  # Run incremental models
  echo -e "${BOLD}[ dbt run ]${RESET}"
  if "${DBT_CMD}" run 2>&1; then
    local end_epoch; end_epoch=$(date +%s)
    local elapsed=$(( end_epoch - start_epoch ))
    _ok "Models built successfully in ${elapsed}s"
  else
    local exit_code=$?
    _err "dbt run failed (exit ${exit_code}) — check output above"
    cd "${SCRIPT_DIR}"
    return ${exit_code}
  fi

  # Optionally run tests
  if [[ "${RUN_TESTS}" == "true" ]]; then
    echo ""
    echo -e "${BOLD}[ dbt test ]${RESET}"
    if "${DBT_CMD}" test 2>&1; then
      _ok "All tests passed"
    else
      _warn "Some tests failed — environment still usable, check output above"
    fi
  fi

  local end_epoch; end_epoch=$(date +%s)
  local elapsed=$(( end_epoch - start_epoch ))

  echo ""
  _ok "Run #${run_num} complete in ${elapsed}s"

  if [[ "${RUN_ONCE}" == "false" ]]; then
    echo -e "  Next run at ${BOLD}$(date -v +${INTERVAL}S '+%H:%M:%S' 2>/dev/null || date -d "+${INTERVAL} seconds" '+%H:%M:%S' 2>/dev/null || echo "in ${INTERVAL}s")${RESET}"
  fi

  cd "${SCRIPT_DIR}"
}

# ── entrypoint ────────────────────────────────────────────────────────────────
_header "dbt periodic runner"
_info "Project : ${DBT_DIR}"
_info "dbt     : ${DBT_CMD} ($(${DBT_CMD} --version 2>&1 | grep 'installed' | grep -o '[0-9]*\.[0-9]*\.[0-9]*' | head -1))"
if [[ "${RUN_ONCE}" == "true" ]]; then
  _info "Mode    : single run then exit"
else
  _info "Mode    : loop every ${INTERVAL}s (Ctrl-C to stop)"
fi
_info "Tests   : ${RUN_TESTS}"

RUN_COUNT=0

while true; do
  RUN_COUNT=$(( RUN_COUNT + 1 ))
  run_dbt "${RUN_COUNT}"

  if [[ "${RUN_ONCE}" == "true" ]]; then
    break
  fi

  # Interruptible sleep: show countdown and respond to Ctrl-C immediately
  echo ""
  for (( i=INTERVAL; i>0; i-- )); do
    printf "\r  Sleeping... %3ds remaining (Ctrl-C to stop)" "$i"
    sleep 1
  done
  printf "\r%60s\r" ""  # clear the countdown line
done
