#!/usr/bin/env bash
# ============================================================
# teardown.sh — Destroy all ClickHouse Cloud lab resources
#
# WARNING: This permanently deletes the ClickHouse Cloud service
# and stops all Docker containers. Run only when done with
# the lab or resetting for a new cohort.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="${SCRIPT_DIR}/terraform"
SUPERSET_DIR="${SCRIPT_DIR}/superset"

# Auto-source .env so teardown works without manually running `source .env`
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/.env"
  set +a
fi

# Auto-source .clickhouse_state if present
if [[ -f "${SCRIPT_DIR}/.clickhouse_state" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/.clickhouse_state"
  set +a
fi

log()  { echo -e "\n\033[1;34m>>> $*\033[0m"; }
ok()   { echo -e "\033[1;32m    ✓ $*\033[0m"; }
warn() { echo -e "\033[1;33m    ⚠ $*\033[0m"; }

# Confirm
echo ""
echo "  ╔════════════════════════════════════════════════════════╗"
echo "  ║  WARNING: This will PERMANENTLY DELETE your           ║"
echo "  ║  ClickHouse Cloud service for the NYC Taxi lab.       ║"
echo "  ║  All data and the service will be destroyed.          ║"
echo "  ║  This cannot be undone.                               ║"
echo "  ╚════════════════════════════════════════════════════════╝"
echo ""
echo "  Service: ${CLICKHOUSE_HOST:-<unknown>}"
echo ""
read -r -p "  Type 'destroy' to confirm: " confirmation
if [[ "${confirmation}" != "destroy" ]]; then
  echo "  Teardown cancelled."
  exit 0
fi

log "Stopping ClickHouse trip producer container (if running)..."
docker stop nyc_taxi_ch_producer 2>/dev/null || true
ok "Trip producer stopped (or was not running)."

log "Stopping Apache Superset..."
if [[ -f "${SUPERSET_DIR}/docker-compose.yml" ]] && command -v docker >/dev/null 2>&1; then
  cd "${SUPERSET_DIR}"
  docker compose down -v 2>/dev/null || true
  ok "Superset stopped and volumes removed."
  cd "${SCRIPT_DIR}"
else
  warn "Superset not running or docker not found."
fi

log "Destroying ClickHouse Cloud infrastructure with Terraform..."
cd "${TERRAFORM_DIR}"
terraform destroy -auto-approve -input=false
ok "ClickHouse Cloud service destroyed."
cd "${SCRIPT_DIR}"

# Clean up state file
if [[ -f "${SCRIPT_DIR}/.clickhouse_state" ]]; then
  rm "${SCRIPT_DIR}/.clickhouse_state"
  ok "Removed .clickhouse_state"
fi

echo ""
echo "============================================================"
echo "  Teardown complete. ClickHouse Cloud billing has stopped."
echo ""
echo "  The local dbt project, queries, and scripts remain intact."
echo "  Your Snowflake environment (Part 1) is unaffected."
echo ""
echo "  To re-run the lab: source .env && ./setup.sh"
echo "============================================================"
