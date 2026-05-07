#!/usr/bin/env bash
# add_clickhouse_connection.sh
# Imports the 4 ClickHouse dashboards into Superset by patching the exported
# ZIP with the current ClickHouse credentials and calling the import endpoint.
#
# Run after: setup.sh Steps 1-5 (Terraform + dbt + migration complete)
# Called by: setup.sh Step 7
# Also safe to re-run: import uses overwrite=true
# ─────────────────────────────────────────────────────────────────
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUPERSET_URL="${SUPERSET_URL:-http://localhost:8088}"
ADMIN_USER="${SUPERSET_ADMIN_USER:-admin}"
ADMIN_PASS="${SUPERSET_ADMIN_PASSWORD:-admin}"

EXPORT_ZIP="${SCRIPT_DIR}/dashboards/dashboard_export_20260401T084808.zip"

# ── Validate required vars ────────────────────────────────────────────────────
for var in CLICKHOUSE_HOST CLICKHOUSE_USER CLICKHOUSE_PASSWORD; do
  if [ -z "${!var:-}" ]; then
    echo "ERROR: ${var} is not set. Source .env and .clickhouse_state first."
    exit 1
  fi
done

if [ ! -f "${EXPORT_ZIP}" ]; then
  echo "ERROR: Dashboard export not found at ${EXPORT_ZIP}"
  exit 1
fi

CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8443}"

# URL-encode credentials so special characters don't break the URI
CH_PASSWORD_ENC=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "${CLICKHOUSE_PASSWORD}")
CH_USER_ENC=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "${CLICKHOUSE_USER}")
CLICKHOUSE_URI="clickhousedb://${CH_USER_ENC}:${CH_PASSWORD_ENC}@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/analytics?secure=true"

# ── Wait for Superset ─────────────────────────────────────────────────────────
echo ">>> Waiting for Superset to be ready..."
until curl -sf "${SUPERSET_URL}/health" > /dev/null; do
  sleep 3
done
echo ">>> Superset is up."

# ── Authenticate ──────────────────────────────────────────────────────────────
LOGIN_RESPONSE=$(curl -s -c /tmp/superset_cookies.txt \
  -X POST "${SUPERSET_URL}/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"${ADMIN_USER}\", \"password\": \"${ADMIN_PASS}\", \"provider\": \"db\", \"refresh\": true}")

ACCESS_TOKEN=$(echo "${LOGIN_RESPONSE}" | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null || true)
if [ -z "${ACCESS_TOKEN}" ]; then
  echo "ERROR: Superset login failed. Check SUPERSET_ADMIN_USER / SUPERSET_ADMIN_PASSWORD."
  echo "       Response: ${LOGIN_RESPONSE}"
  exit 1
fi
echo ">>> Authenticated."

AUTH_HEADER="Authorization: Bearer ${ACCESS_TOKEN}"

CSRF_TOKEN=$(curl -s \
  -b /tmp/superset_cookies.txt \
  -c /tmp/superset_cookies.txt \
  -H "${AUTH_HEADER}" \
  "${SUPERSET_URL}/api/v1/security/csrf_token/" \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['result'])")
CSRF_HEADER="X-CSRFToken: ${CSRF_TOKEN}"
echo ">>> CSRF token obtained."

# ── Patch ZIP: replace database URI with current credentials ──────────────────
# The exported database YAML has the original host and XXXXXXXXXX as the
# password placeholder. Rewrite sqlalchemy_uri before importing.
echo ">>> Patching dashboard export with current ClickHouse credentials..."
PATCHED_ZIP=$(mktemp /tmp/superset_import_XXXXXX.zip)

python3 - "${EXPORT_ZIP}" "${CLICKHOUSE_URI}" "${PATCHED_ZIP}" <<'PYEOF'
import sys, zipfile, re, yaml, json

src_zip, new_uri, dst_zip = sys.argv[1], sys.argv[2], sys.argv[3]

def clean_position(pos):
    """
    The exported ZIP has two sets of chart entries per dashboard:
      1. Ghost rows (ROW-1, ROW-2, …): proper 2-per-row layout, width=6 each,
         but chart entries have no uuid so they break on import.
      2. Duplicate row (ROW-N-XXXXXX): all charts crammed into one row at
         width=4, but these entries DO have uuid + sliceName.

    Fix: patch the ghost entries with the uuid/sliceName from the duplicate row,
    then drop the duplicate row so only the properly-laid-out rows remain.
    """
    # 1. Build chartId → {uuid, sliceName} from UUID-bearing entries
    uuid_map = {}
    for v in pos.values():
        if isinstance(v, dict) and v.get('type') == 'CHART':
            meta = v.get('meta', {})
            if meta.get('uuid') and meta.get('chartId') is not None:
                uuid_map[meta['chartId']] = {
                    'uuid': meta['uuid'],
                    'sliceName': meta.get('sliceName', ''),
                }

    # 2. Identify ghost chart keys (CHART-<digits>, no uuid)
    ghost_keys = {
        k for k, v in pos.items()
        if isinstance(v, dict)
        and v.get('type') == 'CHART'
        and re.fullmatch(r'CHART-\d+', k)
        and not v.get('meta', {}).get('uuid')
    }

    # 3. Patch ghost entries with uuid + sliceName; keep their layout (width=6)
    for k in ghost_keys:
        chart_id = pos[k]['meta']['chartId']
        if chart_id in uuid_map:
            pos[k]['meta']['uuid'] = uuid_map[chart_id]['uuid']
            pos[k]['meta']['sliceName'] = uuid_map[chart_id]['sliceName']

    # 4. Identify UUID-bearing chart keys (the duplicates)
    uuid_chart_keys = {
        k for k, v in pos.items()
        if isinstance(v, dict)
        and v.get('type') == 'CHART'
        and not re.fullmatch(r'CHART-\d+', k)
        and v.get('meta', {}).get('uuid')
    }

    # 5. Remove uuid-bearing chart keys from every parent's children list
    for v in pos.values():
        if isinstance(v, dict) and 'children' in v:
            v['children'] = [c for c in v['children'] if c not in uuid_chart_keys]

    # 6. Remove uuid-bearing chart entries from the position dict
    for k in uuid_chart_keys:
        del pos[k]

    # 7. Drop any ROW that is now empty (the duplicate all-in-one row)
    empty_rows = {
        k for k, v in pos.items()
        if isinstance(v, dict) and v.get('type') == 'ROW' and not v.get('children')
    }
    for v in pos.values():
        if isinstance(v, dict) and 'children' in v:
            v['children'] = [c for c in v['children'] if c not in empty_rows]
    for k in empty_rows:
        del pos[k]

    return pos

with zipfile.ZipFile(src_zip, 'r') as zin, \
     zipfile.ZipFile(dst_zip, 'w', zipfile.ZIP_DEFLATED) as zout:
    for item in zin.infolist():
        data = zin.read(item.filename)
        if 'databases/' in item.filename and item.filename.endswith('.yaml'):
            text = data.decode('utf-8')
            text = re.sub(
                r'^sqlalchemy_uri:.*$',
                f'sqlalchemy_uri: {new_uri}',
                text,
                flags=re.MULTILINE
            )
            data = text.encode('utf-8')
        elif 'dashboards/' in item.filename and item.filename.endswith('.yaml'):
            doc = yaml.safe_load(data)
            if 'position' in doc and isinstance(doc['position'], dict):
                doc['position'] = clean_position(doc['position'])
            data = yaml.dump(doc, allow_unicode=True, sort_keys=False).encode('utf-8')
        zout.writestr(item, data)
PYEOF

echo "    Done."

# ── Import ────────────────────────────────────────────────────────────────────
echo ">>> Importing dashboards (overwrite=true)..."
response=$(curl -s -w "\n%{http_code}" \
  -X POST "${SUPERSET_URL}/api/v1/dashboard/import/" \
  -b /tmp/superset_cookies.txt \
  -H "${AUTH_HEADER}" \
  -H "${CSRF_HEADER}" \
  -F "formData=@${PATCHED_ZIP};type=application/zip" \
  -F "overwrite=true")
http_code=$(echo "${response}" | tail -1)
body=$(echo "${response}" | sed '$d')

rm -f "${PATCHED_ZIP}"

if [ "${http_code}" = "200" ]; then
  echo ""
  echo "============================================================"
  echo "  ClickHouse Superset Setup Complete"
  echo "  Superset URL: ${SUPERSET_URL}"
  echo ""
  echo "  Dashboards imported:"
  echo "    1. CH — Operations Command Center"
  echo "    2. CH — Executive Weekly Report"
  echo "    3. CH — Driver Quality Analytics"
  echo "    4. CH — Capabilities Showcase"
  echo ""
  echo "  Login: ${SUPERSET_URL} / ${ADMIN_USER} / ${ADMIN_PASS}"
  echo "============================================================"
else
  echo "ERROR: Import failed (HTTP ${http_code}):"
  echo "${body}"
  exit 1
fi
