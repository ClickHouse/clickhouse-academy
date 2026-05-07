#!/usr/bin/env bash
# ============================================================
# init_superset.sh
# Registers Snowflake and ClickHouse database connections
# and imports pre-built dashboard definitions.
# Run once after docker-compose up.
# ============================================================
# -u: error on unset variables  -o pipefail: catch pipe failures
# -e intentionally omitted: non-critical step failures print a warning and continue
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUPERSET_URL="${SUPERSET_URL:-http://localhost:8088}"
ADMIN_USER="${SUPERSET_ADMIN_USER:-admin}"
ADMIN_PASS="${SUPERSET_ADMIN_PASSWORD:-admin}"

# ── helpers ──────────────────────────────────────────────────────────────────
_update_db() {
  local name="$1" uri="$2"
  echo ">>> Updating connection URI: ${name}"
  local db_id
  db_id=$(curl -s \
    -H "${AUTH_HEADER}" \
    "${SUPERSET_URL}/api/v1/database/?q=(page_size:100)" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
match = next((r['id'] for r in d.get('result', []) if r['database_name'] == sys.argv[1]), '')
print(match)
" "${name}" 2>/dev/null || echo "")

  if [ -z "${db_id}" ]; then
    echo "    Connection not found — skipping update."
    return
  fi

  local response http_code
  response=$(curl -s -w "\n%{http_code}" \
    -X PUT "${SUPERSET_URL}/api/v1/database/${db_id}" \
    -b /tmp/superset_cookies.txt \
    -H "${AUTH_HEADER}" \
    -H "${CSRF_HEADER}" \
    -H "Content-Type: application/json" \
    -d "{\"sqlalchemy_uri\": \"${uri}\"}")
  http_code=$(echo "${response}" | tail -1)
  if [ "${http_code}" = "200" ]; then
    echo "    Updated successfully."
  else
    echo "    ERROR (HTTP ${http_code}): $(echo "${response}" | sed '$d')"
  fi
}

_register_db() {
  local name="$1" uri="$2"
  echo ">>> Registering: ${name}"

  # Check if connection already exists
  local existing
  existing=$(curl -s \
    -H "${AUTH_HEADER}" \
    "${SUPERSET_URL}/api/v1/database/?q=(filters:!((col:database_name,opr:DatabaseFilter,val:'${name}')))" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null || echo "0")

  if [ "${existing}" -gt 0 ] 2>/dev/null; then
    echo "    Already registered — skipping."
    return
  fi

  local response
  response=$(curl -s -w "\n%{http_code}" \
    -X POST "${SUPERSET_URL}/api/v1/database/" \
    -b /tmp/superset_cookies.txt \
    -H "${AUTH_HEADER}" \
    -H "${CSRF_HEADER}" \
    -H "Content-Type: application/json" \
    -d "{
      \"database_name\": \"${name}\",
      \"sqlalchemy_uri\": \"${uri}\",
      \"expose_in_sqllab\": true,
      \"allow_run_async\": false
    }")

  local http_code body
  http_code=$(echo "${response}" | tail -1)
  body=$(echo "${response}" | sed '$d')

  if [ "${http_code}" = "201" ]; then
    echo "    Registered successfully."
  else
    echo "    ERROR (HTTP ${http_code}): ${body}"
  fi
}

# ── wait for Superset ─────────────────────────────────────────────────────────
echo ">>> Waiting for Superset to be ready..."
until curl -sf "${SUPERSET_URL}/health" > /dev/null; do
  sleep 3
done
echo ">>> Superset is up."

# ── authenticate ──────────────────────────────────────────────────────────────
LOGIN_RESPONSE=$(curl -s -c /tmp/superset_cookies.txt \
  -X POST "${SUPERSET_URL}/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"${ADMIN_USER}\", \"password\": \"${ADMIN_PASS}\", \"provider\": \"db\", \"refresh\": true}")

ACCESS_TOKEN=$(echo "${LOGIN_RESPONSE}" | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")
echo ">>> Authenticated."

AUTH_HEADER="Authorization: Bearer ${ACCESS_TOKEN}"

# Fetch CSRF token (required for all POST requests).
# Must use the same session cookie from login — otherwise Superset sees a mismatched session.
CSRF_TOKEN=$(curl -s \
  -b /tmp/superset_cookies.txt \
  -c /tmp/superset_cookies.txt \
  -H "${AUTH_HEADER}" \
  "${SUPERSET_URL}/api/v1/security/csrf_token/" \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['result'])")
CSRF_HEADER="X-CSRFToken: ${CSRF_TOKEN}"
echo ">>> CSRF token obtained."

# ── Snowflake connection ───────────────────────────────────────────────────────
# Build the account identifier: use ORG-ACCOUNT format if both vars are set,
# otherwise fall back to SNOWFLAKE_ACCOUNT as-is (may already contain the full identifier)
if [ -n "${SNOWFLAKE_ORG:-}" ]; then
  SF_ACCOUNT="${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}"
else
  SF_ACCOUNT="${SNOWFLAKE_ACCOUNT}"
fi

# URL-encode the password so special characters (#, !, *, @, etc.) don't break the URI
SF_PASSWORD_ENC=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "${SNOWFLAKE_PASSWORD}")
SF_USER_ENC=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "${SNOWFLAKE_USER}")

SNOWFLAKE_URI="snowflake://${SF_USER_ENC}:${SF_PASSWORD_ENC}@${SF_ACCOUNT}/NYC_TAXI_DB/ANALYTICS?warehouse=ANALYTICS_WH&role=ANALYST_ROLE"
_register_db "NYC Taxi — Snowflake (Source)" "${SNOWFLAKE_URI}"

# ── ClickHouse connection (pre-configured for Act 2) ─────────────────────────
# Only register if CLICKHOUSE_HOST is explicitly set — it won't be available until Act 2
if [ -n "${CLICKHOUSE_HOST:-}" ]; then
  CH_PASSWORD_ENC=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "${CLICKHOUSE_PASSWORD:-}")
  CLICKHOUSE_URI="clickhousedb://${CLICKHOUSE_USER:-default}:${CH_PASSWORD_ENC}@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT:-8443}/nyc_taxi?protocol=https"
  _register_db "NYC Taxi — ClickHouse Cloud (Target)" "${CLICKHOUSE_URI}"
else
  echo ">>> Skipping ClickHouse connection (CLICKHOUSE_HOST not set — configure in Act 2)."
fi

# ── Import dashboards ─────────────────────────────────────────────────────────
# Superset expects dashboard exports as .zip files.
# The stub .json files in dashboards/ are metadata placeholders — not importable.
# After building dashboards manually in the UI, export them as .zip and place
# them in the dashboards/ directory, then re-run this script to import them.
echo ">>> Importing dashboards..."
shopt -s nullglob
dashboard_files=("${SCRIPT_DIR}/dashboards"/*.zip)
if [ ${#dashboard_files[@]} -eq 0 ]; then
  echo "    No dashboard exports found in dashboards/ (expected .zip files)."
  echo "    Build dashboards in the UI, export them, then re-run this script."
else
  for dashboard_file in "${dashboard_files[@]}"; do
    dashboard_name=$(basename "${dashboard_file}" .zip)
    echo "    Importing: ${dashboard_name}"

    # The exported zip has database passwords redacted as XXXXXXXXXX.
    # Build a passwords JSON mapping each databases/*.yaml path in the zip
    # to the real Snowflake password so Superset can validate and import.
    # Superset strips the zip root directory before matching passwords.
    # Keys must be root-stripped paths: "databases/Foo.yaml" not "export_dir/databases/Foo.yaml"
    PASSWORDS=$(python3 -c "
import zipfile, json, sys
zip_path, sf_pass = sys.argv[1], sys.argv[2]
pw = {}
with zipfile.ZipFile(zip_path) as z:
    for name in z.namelist():
        if '/databases/' in name and name.endswith('.yaml'):
            stripped = '/'.join(name.split('/')[1:])
            pw[stripped] = sf_pass
print(json.dumps(pw))
" "${dashboard_file}" "${SNOWFLAKE_PASSWORD}")

    response=$(curl -s -w "\n%{http_code}" \
      -X POST "${SUPERSET_URL}/api/v1/dashboard/import/" \
      -b /tmp/superset_cookies.txt \
      -H "${AUTH_HEADER}" \
      -H "${CSRF_HEADER}" \
      -F "formData=@${dashboard_file};type=application/zip" \
      -F "overwrite=true" \
      -F "passwords=${PASSWORDS}")
    http_code=$(echo "${response}" | tail -1)
    if [ "${http_code}" = "200" ]; then
      echo "    Imported successfully."
    else
      echo "    WARNING (HTTP ${http_code}): $(echo "${response}" | sed '$d')"
    fi
  done
fi

# ── Re-apply correct connection URIs (dashboard import may overwrite them) ────
# Dashboard ZIPs embed the exporter's credentials. Re-stamp with env-var values
# so the connection always reflects the current environment, regardless of import order.
_update_db "NYC Taxi — Snowflake (Source)" "${SNOWFLAKE_URI}"
if [ -n "${CLICKHOUSE_HOST:-}" ]; then
  _update_db "NYC Taxi — ClickHouse Cloud (Target)" "${CLICKHOUSE_URI}"
fi

echo ""
echo "============================================================"
echo "  Superset initialized."
echo "  URL:      ${SUPERSET_URL}"
echo "  Username: ${ADMIN_USER}"
echo "  Password: ${ADMIN_PASS}"
echo "============================================================"
