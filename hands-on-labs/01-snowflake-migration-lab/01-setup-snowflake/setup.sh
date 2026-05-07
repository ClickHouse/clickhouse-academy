#!/usr/bin/env bash
# ============================================================
# setup.sh — Full automated spin-up for NYC Taxi Snowflake lab
#
# Prerequisites:
#   - terraform >= 1.6
#   - snowsql CLI installed and configured
#   - dbt-snowflake installed (pip install dbt-snowflake)
#   - docker + docker-compose
#
# Usage:
#   cp .env.example .env && vim .env   # fill in your credentials
#   source .env && ./setup.sh
#
# Flags:
#   --skip-seed       Skip S3 data loading (~10 min saved, tables stay empty)
#   --skip-dbt        Skip dbt pipeline run
#   --skip-superset   Skip Superset + trip producer startup
#   --full-refresh    Force dbt --full-refresh even if FACT_TRIPS already exists
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Auto-source .env if present — so you can run ./setup.sh directly without
# manually running `source .env` first. `set -a` auto-exports every variable.
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/.env"
  set +a
fi

TERRAFORM_DIR="${SCRIPT_DIR}/terraform"
SCRIPTS_DIR="${SCRIPT_DIR}/scripts"
DBT_DIR="${SCRIPT_DIR}/dbt/nyc_taxi_dbt"
SUPERSET_DIR="${SCRIPT_DIR}/superset"

# ── Flags ─────────────────────────────────────────────────────
SKIP_SEED=false
SKIP_DBT=false
SKIP_SUPERSET=false
FORCE_FULL_REFRESH=false
ONLY_SUPERSET=false

for arg in "$@"; do
  case $arg in
    --skip-seed)       SKIP_SEED=true ;;
    --skip-dbt)        SKIP_DBT=true ;;
    --skip-superset)   SKIP_SUPERSET=true ;;
    --only-superset)   ONLY_SUPERSET=true; SKIP_SEED=true; SKIP_DBT=true ;;
    --full-refresh)    FORCE_FULL_REFRESH=true ;;
    --help)
      echo "Usage: ./setup.sh [--skip-seed] [--skip-dbt] [--skip-superset] [--only-superset] [--full-refresh]"
      echo ""
      echo "Flags:"
      echo "  --skip-seed       Skip data seeding (~12 min saved, tables stay empty)"
      echo "  --skip-dbt        Skip dbt pipeline run"
      echo "  --skip-superset   Skip Superset + trip producer startup"
      echo "  --only-superset   Start ONLY Superset (skip Terraform, seed, dbt)"
      echo "  --full-refresh    Force dbt full-refresh even if FACT_TRIPS exists"
      exit 0 ;;
    *) echo "Unknown flag: $arg  (use --help)"; exit 1 ;;
  esac
done

# Initialize TRIPS_COUNT so it's never unbound
TRIPS_COUNT=0

# ── Step counter (adjusts total based on active steps) ────────
STEP=0
TOTAL_STEPS=5
[[ "${ONLY_SUPERSET}" == "true" ]]   && TOTAL_STEPS=2   # Only Superset + Validate
[[ "${SKIP_SEED}" == "true" ]]       && TOTAL_STEPS=$((TOTAL_STEPS - 1))
[[ "${SKIP_DBT}" == "true" ]]        && TOTAL_STEPS=$((TOTAL_STEPS - 1))
[[ "${SKIP_SUPERSET}" == "true" ]]   && TOTAL_STEPS=$((TOTAL_STEPS - 1))

# ── Helpers ───────────────────────────────────────────────────
BOLD="\033[1m"; RESET="\033[0m"
BLUE="\033[1;34m"; GREEN="\033[1;32m"; YELLOW="\033[1;33m"; RED="\033[1;31m"

log()  { STEP=$((STEP + 1)); echo -e "\n${BLUE}┌─ Step ${STEP}/${TOTAL_STEPS}: $*${RESET}"; _STEP_START=$(date +%s); _CURRENT_STEP="$*"; }
ok()   { local s=$(( $(date +%s) - ${_STEP_START:-$(date +%s)} )); echo -e "${GREEN}└─ ✓ $* ${RESET}(${s}s)"; }
info() { echo -e "   ${BOLD}·${RESET} $*"; }
warn() { echo -e "${YELLOW}   ⚠ $*${RESET}"; }
die()  { echo -e "\n${RED}${BOLD}✗ ERROR: $*${RESET}\n"; exit 1; }

_CURRENT_STEP="initializing"
_STEP_START=$(date +%s)
SETUP_START=$(date +%s)

# ── Error trap ────────────────────────────────────────────────
on_error() {
  local line="$1"
  echo -e "\n${RED}${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
  echo -e "${RED}${BOLD}║  Setup FAILED                                                ║${RESET}"
  echo -e "${RED}${BOLD}║  Step: ${_CURRENT_STEP}${RESET}"
  echo -e "${RED}${BOLD}║  Line: ${line}                                                     ║${RESET}"
  echo -e "${RED}${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
  echo ""

  case "${_CURRENT_STEP}" in
    *"Terraform"*)
      echo "  Troubleshooting:"
      echo "  • Verify credentials: SNOWFLAKE_ORG, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD"
      echo "  • Test connection: snowsql -a "${SNOWFLAKE_ORG:-ORG}-${SNOWFLAKE_ACCOUNT:-ACCOUNT}" -u "${SNOWFLAKE_USER:-USER}""
      echo "  • Run manually: cd terraform && terraform plan"
      ;;
    *"tables"*|*"seed"*|*"stream"*|*"view"*)
      echo "  Troubleshooting:"
      echo "  • Test snowsql: snowsql -a ${SNOWFLAKE_ORG:-ORG}-${SNOWFLAKE_ACCOUNT:-ACCOUNT} -u ${SNOWFLAKE_USER:-USER}"
      echo "  • Ensure SYSADMIN role is granted to your user"
      echo "  • Check the failing SQL file in scripts/ for syntax issues"
      ;;
    *"dbt"*)
      echo "  Troubleshooting:"
      echo "  • Run dbt debug: cd dbt/nyc_taxi_dbt && dbt debug"
      echo "  • Check profiles.yml: cat ~/.dbt/profiles.yml"
      echo "  • Re-run skipping seed: source .env && ./setup.sh --skip-seed"
      ;;
    *"Superset"*)
      echo "  Troubleshooting:"
      echo "  • Check Docker is running: docker info"
      echo "  • Check container logs: docker logs nyc_taxi_superset"
      echo "  • Re-run skipping Superset: source .env && ./setup.sh --skip-seed --skip-superset"
      ;;
    *)
      echo "  Re-run: source .env && ./setup.sh --skip-seed"
      ;;
  esac
  echo ""
}
trap 'on_error $LINENO' ERR

# ── Snowflake helper ──────────────────────────────────────────
snowsql_exec() {
  local label="$1" file="$2" role="${3:-SYSADMIN}"
  info "Running ${label}..."
  SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
    --accountname "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
    --username    "${SNOWFLAKE_USER}" \
    --rolename    "${role}" \
    -f            "${file}" \
    --option output_format=plain \
    --option friendly=false
}

# ── 0. Prerequisites ──────────────────────────────────────────
echo -e "\n${BLUE}${BOLD}NYC Taxi Snowflake Migration Lab — Setup${RESET}"
echo    "──────────────────────────────────────────"

info "Checking prerequisites..."

# snowsql on macOS is often installed as a shell alias pointing to:
#   /Applications/SnowSQL.app/Contents/MacOS/snowsql
# command -v doesn't resolve aliases, so we check the known path as a fallback.
SNOWSQL_CMD=""
if   command -v snowsql >/dev/null 2>&1;                         then SNOWSQL_CMD="snowsql"
elif [[ -x "/Applications/SnowSQL.app/Contents/MacOS/snowsql" ]]; then SNOWSQL_CMD="/Applications/SnowSQL.app/Contents/MacOS/snowsql"
fi

# Prefer the project-local venv dbt (Python 3.13) over any system dbt.
# The system dbt may be installed under Python 3.14 which has mashumaro incompatibilities.
DBT_CMD=""
if   [[ -x "${SCRIPT_DIR}/.venv/bin/dbt" ]]; then DBT_CMD="${SCRIPT_DIR}/.venv/bin/dbt"
elif command -v dbt >/dev/null 2>&1;          then DBT_CMD="dbt"
fi

MISSING_TOOLS=()
command -v terraform >/dev/null 2>&1 || MISSING_TOOLS+=("terraform  →  https://developer.hashicorp.com/terraform/downloads")
[[ -z "${SNOWSQL_CMD}" ]]            && MISSING_TOOLS+=("snowsql    →  https://docs.snowflake.com/en/user-guide/snowsql-install-config")
[[ -z "${DBT_CMD}" ]]                && MISSING_TOOLS+=("dbt        →  pip install dbt-snowflake")
if [[ ${#MISSING_TOOLS[@]} -gt 0 ]]; then
  echo -e "${RED}${BOLD}✗ Missing required tools:${RESET}"
  for t in "${MISSING_TOOLS[@]}"; do echo "    $t"; done
  exit 1
fi
if ! command -v docker >/dev/null 2>&1; then
  warn "docker not found — Superset and trip producer will be skipped"
  SKIP_SUPERSET=true
elif ! docker info >/dev/null 2>&1; then
  warn "Docker daemon is not running — Superset and trip producer will be skipped"
  warn "Start Docker Desktop / OrbStack, then re-run: ./setup.sh --skip-seed --skip-dbt"
  SKIP_SUPERSET=true
fi
info "Tools: terraform $(terraform version -json 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("terraform_version","?"))' 2>/dev/null || terraform version | head -1 | grep -o '[0-9]*\.[0-9]*\.[0-9]*' | head -1) · snowsql (${SNOWSQL_CMD}) · dbt $("${DBT_CMD}" --version 2>&1 | grep 'installed' | grep -o '[0-9]*\.[0-9]*\.[0-9]*' | head -1) (${DBT_CMD})"

info "Checking environment variables..."
MISSING_VARS=()
[[ -z "${SNOWFLAKE_ORG:-}"      ]] && MISSING_VARS+=("SNOWFLAKE_ORG")
[[ -z "${SNOWFLAKE_ACCOUNT:-}"  ]] && MISSING_VARS+=("SNOWFLAKE_ACCOUNT")
[[ -z "${SNOWFLAKE_USER:-}"     ]] && MISSING_VARS+=("SNOWFLAKE_USER")
[[ -z "${SNOWFLAKE_PASSWORD:-}" ]] && MISSING_VARS+=("SNOWFLAKE_PASSWORD")
if [[ ${#MISSING_VARS[@]} -gt 0 ]]; then
  die "Missing environment variables: ${MISSING_VARS[*]}\n  Copy .env.example → .env, fill in values, then: source .env && ./setup.sh"
fi
info "Account: ${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}  ·  User: ${SNOWFLAKE_USER}"

echo ""
info "Active steps: Terraform | SQL scripts$([ "${SKIP_SEED}" = "true" ] && echo ' (--skip-seed)') | $([ "${SKIP_DBT}" = "true" ] && echo "dbt skipped" || echo "dbt") | $([ "${SKIP_SUPERSET}" = "true" ] && echo "Superset skipped" || echo "Superset") | Validate"
echo ""

# ── Step 1: Terraform ─────────────────────────────────────────
_CURRENT_STEP="Terraform — provisioning Snowflake infrastructure"
log "Terraform — provisioning Snowflake infrastructure"

cd "${TERRAFORM_DIR}"
cat > terraform.tfvars <<-EOF
snowflake_org      = "${SNOWFLAKE_ORG}"
snowflake_account  = "${SNOWFLAKE_ACCOUNT}"
snowflake_user     = "${SNOWFLAKE_USER}"
snowflake_password = "${SNOWFLAKE_PASSWORD}"
environment        = "${LAB_ENVIRONMENT:-lab}"
lab_cohort         = "${LAB_COHORT:-fy27-q1}"
EOF

info "terraform init..."
terraform init -upgrade -input=false -no-color 2>&1 | grep -E "^(Terraform|Initializing|Upgrading|  -)" || true

info "terraform validate..."
terraform validate -no-color

info "terraform plan..."
terraform plan -out=tfplan -input=false -no-color 2>&1 | tail -5

info "terraform apply..."
terraform apply -input=false -no-color tfplan 2>&1 | grep -E "^(Apply|  \+|  ~|  -|Plan:|Changes to|No changes)" || true

ok "Snowflake infrastructure provisioned"
cd "${SCRIPT_DIR}"

# ── Step 2: SQL scripts ───────────────────────────────────────
_CURRENT_STEP="SQL scripts — creating tables and loading data"
log "SQL scripts — creating tables and loading data"

snowsql_exec "creating tables (01_create_tables.sql)" "${SCRIPTS_DIR}/01_create_tables.sql"
info "Tables created."

# TRIPS_RAW now exists — apply the Terraform resources that depend on it
# (CDC stream, 5-min CDC task, hourly agg task).
cd "${TERRAFORM_DIR}"
info "Terraform — creating CDC stream and scheduled tasks..."
terraform apply \
  -target=snowflake_execute.trips_cdc_stream \
  -target=snowflake_execute.cdc_consume_task \
  -target=snowflake_execute.hourly_agg_task \
  -input=false -auto-approve -no-color 2>&1 \
  | grep -E "^(Apply|  \+|  ~|  -|Plan:|Changes to|No changes)" || true
info "CDC stream and tasks created (both SUSPENDED — resuming CDC task now)."
info "Resuming CDC task (CDC_CONSUME_TASK)..."
SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
  --accountname "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
  --username "${SNOWFLAKE_USER}" \
  --rolename SYSADMIN \
  -q "ALTER TASK NYC_TAXI_DB.RAW.CDC_CONSUME_TASK RESUME;" \
  --option output_format=plain --option friendly=false >/dev/null 2>&1 || warn "Failed to resume CDC task"
cd "${SCRIPT_DIR}"

if [[ "${SKIP_SEED}" == "true" ]]; then
  warn "Seed skipped (--skip-seed flag). TRIPS_RAW will be empty until you run scripts/02_seed_data.sql."
else
  # Auto-detect: skip seeding if TRIPS_RAW already has data (idempotent re-runs)
  TRIPS_COUNT=$(SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
    --accountname "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
    --username "${SNOWFLAKE_USER}" \
    --rolename SYSADMIN \
    -q "SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.TRIPS_RAW" \
    --option output_format=plain --option friendly=false 2>/dev/null \
    | grep -E '^ *[0-9]+ *$' | head -1 | tr -d ' ' || echo "0")
  TRIPS_COUNT="${TRIPS_COUNT//[^0-9]/}"

  if [[ "${TRIPS_COUNT:-0}" -gt 0 ]]; then
    info "TRIPS_RAW already has ${TRIPS_COUNT} rows — skipping seed."
  else
    info "Generating 50M synthetic NYC Taxi trips — this takes 8–12 minutes..."
    info "Dataset: Synthetic TABLE(GENERATOR) with realistic TLC distributions (2019-2022)"
    info "Zones: Real 265 NYC TLC zones | Payment types: realistic distribution"
    info "You can monitor progress in the Snowflake UI: Admin → Query History"

    # Run seed; if it fails, check Snowflake error
    if ! snowsql_exec "seeding data (02_seed_data.sql)" "${SCRIPTS_DIR}/02_seed_data.sql"; then
      die "Seed failed. Check the SQL script for errors:\n
  • Review ${SCRIPTS_DIR}/02_seed_data.sql\n
  • Verify warehouse TRANSFORM_WH is running: snowsql -a \${SNOWFLAKE_ORG}-\${SNOWFLAKE_ACCOUNT} -u \${SNOWFLAKE_USER} -q 'SHOW WAREHOUSES'\n
  • Check Snowflake query history for detailed error messages"
    fi

    # Validate that seed actually loaded data
    TRIPS_AFTER=$(SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
      --accountname "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
      --username "${SNOWFLAKE_USER}" \
      --rolename SYSADMIN \
      -q "SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.TRIPS_RAW" \
      --option output_format=plain --option friendly=false 2>/dev/null \
      | grep -E '^ *[0-9]+ *$' | head -1 | tr -d ' ' || echo "0")
    TRIPS_AFTER="${TRIPS_AFTER//[^0-9]/}"

    if [[ "${TRIPS_AFTER:-0}" -eq 0 ]]; then
      die "Seed completed but no rows loaded into TRIPS_RAW. Check Snowflake logs and network connectivity."
    fi

    info "Data loaded (${TRIPS_AFTER} rows). TRIP_METADATA VARIANT populated synthetically (lab telemetry field)."
    TRIPS_COUNT="${TRIPS_AFTER}"
  fi
fi

ok "Tables and seed data ready (${TRIPS_COUNT} rows in TRIPS_RAW)"

# ── Step 3: dbt ───────────────────────────────────────────────
if [[ "${SKIP_DBT}" == "false" ]]; then
  _CURRENT_STEP="dbt — building transformation pipeline"
  log "dbt — building transformation pipeline"
  cd "${DBT_DIR}"

  export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-${DBT_DIR}}"

  if [[ ! -f "${DBT_PROFILES_DIR}/profiles.yml" ]]; then
    if [[ -f "${DBT_DIR}/profiles.yml.example" ]]; then
      cp "${DBT_DIR}/profiles.yml.example" "${DBT_PROFILES_DIR}/profiles.yml"
      warn "profiles.yml not found — copied example to ${DBT_PROFILES_DIR}/profiles.yml"
      warn "Edit it with your Snowflake credentials, then re-run: ./setup.sh --skip-seed"
      warn "Skipping dbt for now."
      cd "${SCRIPT_DIR}"
    else
      die "No profiles.yml found.\n  Copy dbt/nyc_taxi_dbt/profiles.yml.example to ~/.dbt/profiles.yml and fill in your credentials."
    fi
  else
    info "Installing dbt packages (dbt deps)..."
    "${DBT_CMD}" deps

    info "Running dbt seeds (reference CSVs)..."
    if ! "${DBT_CMD}" seed --full-refresh; then
      warn "dbt seed had non-fatal errors (no CSV seeds present is expected)."
    fi

    # First run: full-refresh to build all models from scratch.
    # Re-runs: incremental to preserve producer-inserted trips.
    FACT_EXISTS=$(SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
      --accountname "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
      --username "${SNOWFLAKE_USER}" \
      --rolename ANALYST_ROLE \
      -q "SELECT COUNT(*) FROM NYC_TAXI_DB.ANALYTICS.FACT_TRIPS" \
      --option output_format=plain --option friendly=false 2>/dev/null \
      | grep -E '^ *[0-9]+ *$' | head -1 | tr -d ' ' || echo "0")
    FACT_EXISTS="${FACT_EXISTS//[^0-9]/}"  # strip any non-numeric chars

    if [[ "${FORCE_FULL_REFRESH}" == "true" ]] || [[ "${FACT_EXISTS:-0}" -eq 0 ]]; then
      info "First run — building all models with --full-refresh..."
      "${DBT_CMD}" run --full-refresh
    else
      info "FACT_TRIPS has ${FACT_EXISTS} rows — running incremental (producer data preserved)..."
      "${DBT_CMD}" run
    fi

    info "Running dbt tests..."
    if ! "${DBT_CMD}" test; then
      warn "Some dbt tests failed — check output above. The environment is still usable."
      warn "Tests often fail on an empty dataset (before seeding). Re-run after seeding."
    fi

    info "Creating secure view over ANALYTICS schema (04_create_secure_view.sql)..."
    snowsql_exec "creating secure view (04_create_secure_view.sql)" "${SCRIPTS_DIR}/04_create_secure_view.sql" SYSADMIN

    info "Resuming hourly aggregation task (HOURLY_AGG_TASK)..."
    SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
      --accountname "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
      --username "${SNOWFLAKE_USER}" \
      --rolename SYSADMIN \
      -q "ALTER TASK NYC_TAXI_DB.STAGING.HOURLY_AGG_TASK RESUME;" \
      --option output_format=plain --option friendly=false >/dev/null 2>&1 || warn "Failed to resume hourly task"

    ok "dbt pipeline complete"
    cd "${SCRIPT_DIR}"
  fi
fi

# ── Step 4: Superset + producer ───────────────────────────────
DOCKER_AVAILABLE=true
SUPERSET_STARTED=false

if [[ "${SKIP_SUPERSET}" == "false" ]]; then
  _CURRENT_STEP="Superset + trip producer — starting Docker services"

  # Check if Docker is available
  if ! command -v docker >/dev/null 2>&1; then
    warn "Docker not found in PATH (--skip-superset flag would avoid this step)."
    DOCKER_AVAILABLE=false
  elif ! docker info >/dev/null 2>&1; then
    warn "Docker daemon is not running (docker info failed)."
    warn "Start Docker and re-run: ./setup.sh --skip-seed (to skip data load)"
    DOCKER_AVAILABLE=false
  fi

  if [[ "${DOCKER_AVAILABLE}" == "true" ]]; then
    log "Superset + trip producer — starting Docker services"
    cd "${SUPERSET_DIR}"

    info "Starting containers (docker-compose up -d)..."
    if ! docker-compose --env-file ../.env up -d 2>&1 | tee /tmp/docker-compose.log; then
      warn "docker-compose failed — check the error above."
      warn "Superset will not be available. You can start it manually later:"
      warn "  cd superset && docker-compose --env-file ../.env up -d"
      DOCKER_AVAILABLE=false
    else
      SUPERSET_STARTED=true
      info "Waiting for Superset to be healthy (up to 2 minutes)..."
      SUPERSET_READY=false
      for elapsed in $(seq 5 5 120); do
        if curl -sf http://localhost:8088/health >/dev/null 2>&1; then
          SUPERSET_READY=true
          info "Superset healthy after ${elapsed}s."
          break
        fi
        printf "\r   waiting... %3ds / 120s" "${elapsed}"
        sleep 5
      done
      echo ""  # clear the \r line

      if [[ "${SUPERSET_READY}" == "true" ]]; then
        info "Registering database connections and importing dashboards..."
        bash "${SUPERSET_DIR}/init_superset.sh" \
          || warn "Superset init completed with warnings — check http://localhost:8088"
      else
        warn "Superset did not become healthy within 2 minutes."
        warn "Check container logs: docker logs nyc_taxi_superset"
        warn "You can retry Superset init manually: bash superset/init_superset.sh"
      fi

      PRODUCER_RUNNING=$(docker ps --filter "name=nyc_taxi_producer" --filter "status=running" -q)
      if [[ -n "${PRODUCER_RUNNING}" ]]; then
        info "Trip producer is running ($(docker inspect --format='{{.Config.Env}}' nyc_taxi_producer | grep -o 'TRIPS_PER_MINUTE=[^ ]*' || echo '~60 trips/min'))."
        info "Monitor: docker logs -f nyc_taxi_producer"
      else
        warn "Trip producer container is not running. Check: docker logs nyc_taxi_producer"
      fi

      ok "Docker services started"
      cd "${SCRIPT_DIR}"
    fi
  fi

  if [[ "${DOCKER_AVAILABLE}" == "false" ]]; then
    warn "Skipping Superset (Docker not available or --skip-superset flag set)."
    warn "Run manually later: cd superset && docker-compose up -d"
  fi
else
  warn "Superset startup skipped (--skip-superset flag)."
fi

# ── Step 5: Validate ──────────────────────────────────────────
_CURRENT_STEP="validation — checking environment health"
log "Validation — checking environment health"

VALIDATION_OUTPUT=$(SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}" "${SNOWSQL_CMD}" \
  --accountname "${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}" \
  --username    "${SNOWFLAKE_USER}" \
  --rolename    SYSADMIN \
  -f "${SCRIPTS_DIR}/validate_environment.sql" \
  --option output_format=plain \
  --option friendly=false 2>&1)

echo "${VALIDATION_OUTPUT}"

FAIL_COUNT=$(echo "${VALIDATION_OUTPUT}" | grep -cE "\sFAIL(\s|$)" || true)
if [[ "${FAIL_COUNT}" -gt 0 ]]; then
  warn "${FAIL_COUNT} validation check(s) failed — see FAIL rows above."
  warn "Common causes: dbt not yet run, seed still in progress, or empty tables with --skip-seed."
  ok "Validation complete (with warnings)"
else
  ok "All validation checks passed"
fi

# ── Summary ───────────────────────────────────────────────────
TOTAL_ELAPSED=$(( $(date +%s) - SETUP_START ))
TOTAL_MIN=$(( TOTAL_ELAPSED / 60 ))
TOTAL_SEC=$(( TOTAL_ELAPSED % 60 ))

echo ""
echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${GREEN}${BOLD}║  NYC Taxi Snowflake Lab — Setup Complete! ✓                  ║${RESET}"
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""
echo "  Snowflake:  ${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT} · NYC_TAXI_DB"
echo "  Warehouses: TRANSFORM_WH (SMALL) · ANALYTICS_WH (MEDIUM)"
echo "  Schemas:    RAW · STAGING · ANALYTICS"
echo ""
echo "  Completed in: ${TOTAL_MIN}m ${TOTAL_SEC}s"
echo ""
echo "  What was set up:"
echo "    ✓ Terraform:  warehouses, database, schemas, roles, resource monitor, CDC stream + tasks (SUSPENDED)"
echo "    ✓ SQL:        TRIPS_RAW + dimension tables"
if [[ "${SKIP_SEED}" == "false" ]]; then
  if [[ "${TRIPS_COUNT:-0}" -gt 1000000 ]]; then
    echo "    ✓ Data:       ~50M synthetic trips (realistic TLC distributions) + TRIP_METADATA VARIANT"
  else
    echo "    ⚠ Data:       TRIPS_RAW not seeded — re-run setup to load data (~12 min)"
  fi
fi
[[ "${SKIP_DBT}"      == "false" ]] && echo "    ✓ dbt:        FACT_TRIPS + DIM_* + AGG_HOURLY_ZONE_TRIPS"

# Show Docker/Superset status
if [[ "${SKIP_SUPERSET}" == "true" ]]; then
  echo "    ⊘ Superset:   skipped (--skip-superset flag)"
elif [[ "${DOCKER_AVAILABLE}" == "false" ]]; then
  echo "    ⚠ Superset:   NOT STARTED — Docker daemon is not running"
  echo "                  Start Docker, then: cd superset && docker-compose up -d"
elif [[ "${SUPERSET_STARTED}" == "true" ]]; then
  echo "    ✓ Superset:   http://localhost:8088  (admin / admin)"
  echo "    ✓ Producer:   ~${TRIPS_PER_MINUTE:-60} fake trips/min → TRIPS_RAW (docker logs -f nyc_taxi_producer)"
else
  echo "    ⚠ Superset:   startup skipped (check logs above)"
fi

echo ""
echo "  Next steps:"
echo "    1. Open the Snowflake UI and explore NYC_TAXI_DB"
echo "    2. Run the 7 queries in queries/ — understand each migration challenge"
echo "    3. Review dbt models in dbt/nyc_taxi_dbt/models/"
echo "    4. Complete the worksheets in 02-plan-and-design/worksheets/"
echo "    5. Get SA sign-off, then proceed to 02-migrate-to-clickhouse/"
echo ""
echo "  Cost:       ~\$47/partner/day  (auto-suspend = \$0 when idle)"
echo "  Tear down:  ./teardown.sh"
echo ""
