#!/usr/bin/env bash
# ============================================================
# render_diagram.sh
# Renders docs/architecture.mmd → docs/architecture.png
#
# Uses Docker (preferred — no Node.js required) with automatic
# fallback to npx @mermaid-js/mermaid-cli.
#
# Usage:
#   ./scripts/render_diagram.sh
#   ./scripts/render_diagram.sh --format svg   # render as SVG instead
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

INPUT="${LAB_DIR}/docs/architecture.mmd"
FORMAT="${1:---format}"
if [[ "${FORMAT}" == "--format" ]]; then
  FORMAT="${2:-png}"
fi
FORMAT="${FORMAT#--format}"
FORMAT="${FORMAT:-png}"
OUTPUT="${LAB_DIR}/docs/architecture.${FORMAT}"

MERMAID_IMAGE="minlag/mermaid-cli:latest"
MMDC_ARGS=(
  -i "/data/docs/architecture.mmd"
  -o "/data/docs/architecture.${FORMAT}"
  --backgroundColor white
  --width 2400
  --height 1800
)

log()  { echo -e "\033[1;34m>>> $*\033[0m"; }
ok()   { echo -e "\033[1;32m    ✓ $*\033[0m"; }
die()  { echo -e "\033[1;31m    ✗ $*\033[0m"; exit 1; }

[[ -f "${INPUT}" ]] || die "Source file not found: ${INPUT}"

render_docker() {
  log "Rendering with Docker (${MERMAID_IMAGE})..."
  docker run --rm \
    -v "${LAB_DIR}:/data" \
    "${MERMAID_IMAGE}" \
    "${MMDC_ARGS[@]}"
}

render_npx() {
  log "Rendering with npx @mermaid-js/mermaid-cli..."
  cd "${LAB_DIR}"
  npx --yes @mermaid-js/mermaid-cli \
    -i "docs/architecture.mmd" \
    -o "docs/architecture.${FORMAT}" \
    --backgroundColor white \
    --width 2400 \
    --height 1800
}

if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
  render_docker || {
    log "Docker render failed — falling back to npx..."
    command -v npx &>/dev/null || die "Docker failed and npx not found. Install Node.js >= 18 as a fallback."
    render_npx
  }
elif command -v npx &>/dev/null; then
  render_npx
else
  die "Neither docker nor npx found. Install Docker (preferred) or Node.js >= 18."
fi

ok "Rendered: ${OUTPUT}"
echo ""
echo "  Source:  docs/architecture.mmd"
echo "  Output:  docs/architecture.${FORMAT}"
echo "  To update: edit docs/architecture.mmd then re-run this script."
