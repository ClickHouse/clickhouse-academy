#!/usr/bin/env bash
# ============================================================
# setup.sh — Provision ClickHouse Cloud service for Part 3
#
# This script does ONE thing: provision the ClickHouse Cloud cluster
# via Terraform and write the connection details to .clickhouse_state.
#
# Everything else (dbt, migration script, verification, Superset, benchmark)
# is done manually following the steps in README.md — because those
# are the migration decisions you should make deliberately, not skip.
#
# Prerequisites:
#   - terraform >= 1.6
#   - Completed Part 2 (02-plan-and-design) with migration-plan.md
#
# Usage:
#   cp .env.example .env && vim .env   # fill in your CH Cloud credentials
#   source .env && ./setup.sh
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Auto-source .env if present
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/.env"
  set +a
fi

# Auto-source .clickhouse_state if present (written after Terraform apply)
if [[ -f "${SCRIPT_DIR}/.clickhouse_state" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/.clickhouse_state"
  set +a
fi

TERRAFORM_DIR="${SCRIPT_DIR}/terraform"

# ── Helpers ───────────────────────────────────────────────────
BOLD="\033[1m"; RESET="\033[0m"
BLUE="\033[1;34m"; GREEN="\033[1;32m"; YELLOW="\033[1;33m"; RED="\033[1;31m"

log()  { echo -e "\n${BLUE}${BOLD}▶ $*${RESET}"; _STEP_START=$(date +%s); _CURRENT_STEP="$*"; }
ok()   { local s=$(( $(date +%s) - ${_STEP_START:-$(date +%s)} )); echo -e "${GREEN}${BOLD}✓ $*${RESET} (${s}s)"; }
info() { echo -e "  ${BOLD}·${RESET} $*"; }
warn() { echo -e "${YELLOW}  ⚠ $*${RESET}"; }
die()  { echo -e "\n${RED}${BOLD}✗ ERROR: $*${RESET}\n"; exit 1; }

_CURRENT_STEP="initializing"
_STEP_START=$(date +%s)

on_error() {
  echo -e "\n${RED}${BOLD}Setup FAILED at: ${_CURRENT_STEP}${RESET}"
  echo "  Troubleshooting:"
  echo "  • Verify CLICKHOUSE_ORG_ID, CLICKHOUSE_TOKEN_KEY, CLICKHOUSE_TOKEN_SECRET are set"
  echo "  • Check API key permissions at https://console.clickhouse.cloud → Settings → API Keys"
  echo "  • Run manually: cd terraform && terraform plan"
}
trap 'on_error' ERR

# ── Header ─────────────────────────────────────────────────────
echo -e "\n${BLUE}${BOLD}NYC Taxi Migration Lab — Part 3: ClickHouse Cloud${RESET}"
echo    "────────────────────────────────────────────────────"

# ── Part 2 prerequisite check (soft gate) ─────────────────────
PART2_PLAN="${SCRIPT_DIR}/../02-plan-and-design/migration-plan.md"
if [[ ! -f "${PART2_PLAN}" ]]; then
  warn "Part 2 migration-plan.md not found at ${PART2_PLAN}"
  warn "Strongly recommended: complete Part 2 (Plan & Design) before continuing."
  warn "Proceeding anyway — but you may encounter design decisions without context."
else
  COMPLETED=$(grep -c '\- \[x\]' "${PART2_PLAN}" 2>/dev/null || echo 0)
  TOTAL=$(grep -c '\- \[[ x]\]' "${PART2_PLAN}" 2>/dev/null || echo 4)
  if [[ "${COMPLETED}" -ge "${TOTAL}" ]]; then
    info "Part 2 migration plan: ${COMPLETED}/${TOTAL} sections complete ✓"
  else
    warn "Part 2 migration plan: ${COMPLETED}/${TOTAL} sections complete — consider finishing worksheets first."
  fi
fi

# ── Prerequisites check ───────────────────────────────────────
info "Checking prerequisites..."
command -v terraform >/dev/null 2>&1 || die "terraform not found → https://developer.hashicorp.com/terraform/downloads"

MISSING_VARS=()
[[ -z "${CLICKHOUSE_ORG_ID:-}"       ]] && MISSING_VARS+=("CLICKHOUSE_ORG_ID")
[[ -z "${CLICKHOUSE_TOKEN_KEY:-}"    ]] && MISSING_VARS+=("CLICKHOUSE_TOKEN_KEY")
[[ -z "${CLICKHOUSE_TOKEN_SECRET:-}" ]] && MISSING_VARS+=("CLICKHOUSE_TOKEN_SECRET")
[[ -z "${CLICKHOUSE_PASSWORD:-}"     ]] && MISSING_VARS+=("CLICKHOUSE_PASSWORD")

if [[ ${#MISSING_VARS[@]} -gt 0 ]]; then
  echo -e "${RED}${BOLD}Missing required environment variables:${RESET}"
  for v in "${MISSING_VARS[@]}"; do echo "    $v"; done
  echo ""
  echo "  Copy .env.example → .env and fill in your ClickHouse Cloud credentials."
  exit 1
fi

info "Prerequisites OK."

# ── Terraform: Provision ClickHouse Cloud service ─────────────
log "Provisioning ClickHouse Cloud service via Terraform"

info "Writing terraform.tfvars..."
cat > "${TERRAFORM_DIR}/terraform.tfvars" <<EOF
clickhouse_org_id       = "${CLICKHOUSE_ORG_ID}"
clickhouse_token_key    = "${CLICKHOUSE_TOKEN_KEY}"
clickhouse_token_secret = "${CLICKHOUSE_TOKEN_SECRET}"
clickhouse_password     = "${CLICKHOUSE_PASSWORD}"
cohort                  = "${LAB_COHORT:-fy27-q1}"
EOF

cd "${TERRAFORM_DIR}"

info "Running terraform init..."
terraform init -upgrade

info "Running terraform validate..."
terraform validate

info "Running terraform plan..."
terraform plan -out=tfplan

info "Running terraform apply..."
terraform apply tfplan

# Extract outputs and write state file
CLICKHOUSE_HOST="$(terraform output -raw clickhouse_host)"
CLICKHOUSE_PORT="$(terraform output -raw clickhouse_port 2>/dev/null || echo '8443')"
SERVICE_ID="$(terraform output -raw service_id)"

info "Writing .clickhouse_state..."
cat > "${SCRIPT_DIR}/.clickhouse_state" <<EOF
export CLICKHOUSE_HOST="${CLICKHOUSE_HOST}"
export CLICKHOUSE_PORT="${CLICKHOUSE_PORT}"
export SERVICE_ID="${SERVICE_ID}"
EOF

export CLICKHOUSE_HOST
export CLICKHOUSE_PORT
export SERVICE_ID

cd "${SCRIPT_DIR}"

ok "ClickHouse service provisioned: ${CLICKHOUSE_HOST}"

# ── Done — hand off to runbook ────────────────────────────────
SETUP_ELAPSED=$(( $(date +%s) - _STEP_START ))
echo ""
echo -e "${GREEN}${BOLD}════════════════════════════════════════════════════════════${RESET}"
echo -e "${GREEN}${BOLD}  ClickHouse Cloud service is ready.${RESET}"
echo ""
echo "  Host:     ${CLICKHOUSE_HOST}"
echo "  Port:     ${CLICKHOUSE_PORT}"
echo "  User:     ${CLICKHOUSE_USER:-default}"
echo "  Password: (from .env)"
echo ""
echo "  Connection details saved to: .clickhouse_state"
echo "  Source it in any terminal:   source .clickhouse_state"
echo ""
echo -e "${BOLD}  ─── Next steps ───────────────────────────────────────────${RESET}"
echo "  Follow README.md (Section 7) to complete the remaining steps:"
echo ""
echo "    Step 7.1 ✓  Provision cluster (done)"
echo "    Step 7.2    Migrate data  →  scripts/02_migrate_trips.py"
echo "    Step 7.3    Populate analytics layer  →  dbt run"
echo "    Step 7.4    Create dictionary  →  scripts/04_create_dictionary.sql"
echo "    Step 7.5    Set up Superset dashboards  →  superset/add_clickhouse_connection.sh"
echo "    Step 7.6    Run benchmark  →  scripts/run_benchmark.sh"
echo "    Step 7.7    Cutover (optional)  →  scripts/03_cutover.sh"
echo "    Step 7.8    Verify data parity  →  scripts/01_verify_migration.sh"
echo ""
echo -e "${GREEN}${BOLD}════════════════════════════════════════════════════════════${RESET}"
