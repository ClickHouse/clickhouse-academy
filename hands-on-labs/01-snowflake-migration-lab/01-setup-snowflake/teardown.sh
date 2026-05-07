#!/usr/bin/env bash
# ============================================================
# teardown.sh — Destroy all lab resources
#
# WARNING: This permanently deletes all Snowflake resources
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

log()   { echo -e "\n\033[1;34m>>> $*\033[0m"; }
ok()    { echo -e "\033[1;32m    ✓ $*\033[0m"; }
warn()  { echo -e "\033[1;33m    ⚠ $*\033[0m"; }

# Confirm
echo ""
echo "  ╔════════════════════════════════════════════════════════╗"
echo "  ║  WARNING: This will PERMANENTLY DELETE all Snowflake  ║"
echo "  ║  resources for the NYC Taxi lab environment.           ║"
echo "  ║  All data, tables, warehouses, and roles will be       ║"
echo "  ║  destroyed. This cannot be undone.                     ║"
echo "  ╚════════════════════════════════════════════════════════╝"
echo ""
read -r -p "  Type 'destroy' to confirm: " confirmation
if [[ "${confirmation}" != "destroy" ]]; then
  echo "  Teardown cancelled."
  exit 0
fi

log "Stopping Apache Superset..."
if [[ -f "${SUPERSET_DIR}/docker-compose.yml" ]] && command -v docker >/dev/null 2>&1; then
  cd "${SUPERSET_DIR}"
  docker-compose down -v 2>/dev/null || true
  ok "Superset stopped and volumes removed."
  cd "${SCRIPT_DIR}"
else
  warn "Superset not running or docker not found."
fi

log "Zeroing Time Travel retention on NYC_TAXI_DB..."
# Set DATA_RETENTION_TIME_IN_DAYS = 0 before destroying so Snowflake does not
# hold dropped data in Time Travel (default 1 day) or surface it in the
# Horizon catalog after teardown.  Must run BEFORE terraform destroy.
SNOWSQL_CMD=""
if   command -v snowsql >/dev/null 2>&1;                         then SNOWSQL_CMD="snowsql"
elif [[ -x "/Applications/SnowSQL.app/Contents/MacOS/snowsql" ]]; then SNOWSQL_CMD="/Applications/SnowSQL.app/Contents/MacOS/snowsql"
fi

if [[ -n "${SNOWSQL_CMD}" && -n "${SNOWFLAKE_PASSWORD:-}" ]]; then
  SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
    --accountname "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
    --username    "${SNOWFLAKE_USER}" \
    --rolename    SYSADMIN \
    --option output_format=plain --option friendly=false \
    -q "ALTER DATABASE IF EXISTS NYC_TAXI_DB SET DATA_RETENTION_TIME_IN_DAYS = 0;" \
    2>/dev/null || true
  ok "Time Travel retention zeroed — data will not persist in Horizon catalog."
else
  warn "snowsql not found or credentials missing — skipping Time Travel reset."
  warn "Data may remain visible in Snowflake Horizon for up to 1 day."
fi

log "Destroying Snowflake infrastructure with Terraform..."
cd "${TERRAFORM_DIR}"
if [[ -f "terraform.tfstate" ]]; then
  terraform destroy -auto-approve -input=false
  ok "All Snowflake resources destroyed."
else
  warn "No Terraform state found. Resources may have already been destroyed."
fi
cd "${SCRIPT_DIR}"

echo ""
echo "============================================================"
echo "  Teardown complete. All Snowflake credits have stopped."
echo "  The local dbt project, queries, and scripts remain intact."
echo ""
echo "  Note: COMPUTE_WH is Snowflake's default account warehouse."
echo "  It was not created by this lab and is not destroyed by"
echo "  teardown. It will auto-suspend when idle (no credits lost)."
echo "============================================================"
