#!/usr/bin/env bash
# ============================================================
# render_diagram.sh — Render a Mermaid diagram to PNG or SVG
#
# Shared script for all lab parts. Run it from the lab root or from a
# specific part directory (e.g. 01-setup-snowflake/).
#
# Uses Docker (preferred — no Node.js required) with automatic
# fallback to npx @mermaid-js/mermaid-cli.
#
# Usage (from lab root — renders all diagrams across all parts):
#   common/scripts/render_diagram.sh --all
#
# Usage (from a part directory — renders that part's diagrams only):
#   ../common/scripts/render_diagram.sh --all
#   ../common/scripts/render_diagram.sh                              # docs/architecture.mmd → PNG
#   ../common/scripts/render_diagram.sh docs/architecture_detail.mmd # specific file → PNG
#   ../common/scripts/render_diagram.sh --format svg                 # SVG output
#   ../common/scripts/render_diagram.sh docs/arch.mmd --format svg  # specific file as SVG
# ============================================================
set -euo pipefail

MERMAID_IMAGE="minlag/mermaid-cli:latest"

log()  { echo -e "\033[1;34m>>> $*\033[0m"; }
ok()   { echo -e "\033[1;32m    ✓ $*\033[0m"; }
die()  { echo -e "\033[1;31m    ✗ $*\033[0m"; exit 1; }

# ── Parse arguments ───────────────────────────────────────────
RENDER_ALL=false
FORMAT="png"
INPUT_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)    RENDER_ALL=true; shift ;;
    --format) FORMAT="${2:-png}"; shift 2 ;;
    --format=*) FORMAT="${1#--format=}"; shift ;;
    -*)       die "Unknown option: $1" ;;
    *)        INPUT_FILE="$1"; shift ;;
  esac
done

# ── Resolve paths ─────────────────────────────────────────────
# CALL_DIR: where the user ran the script from (used for relative input paths).
# LAB_ROOT: always the lab root — two levels up from this script's location
#           (common/scripts/ → common/ → lab root). Used as Docker mount root
#           when rendering across multiple parts.
CALL_DIR="${PWD}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# LAB_DIR: the root used for Docker mount and relative path calculations.
# Defaults to CALL_DIR; switched to LAB_ROOT when rendering across parts.
LAB_DIR="${CALL_DIR}"

render_file() {
  local mmd_file="${1}"

  # Make path absolute before deriving the output path
  [[ "${mmd_file}" = /* ]] || mmd_file="${CALL_DIR}/${mmd_file}"
  local out_file="${mmd_file%.mmd}.${FORMAT}"

  [[ -f "${mmd_file}" ]] || die "Source file not found: ${mmd_file}"

  # Paths relative to LAB_DIR for Docker mount
  local rel_input="${mmd_file#${LAB_DIR}/}"
  local rel_output="${out_file#${LAB_DIR}/}"

  log "Rendering ${rel_input} → ${rel_output}"

  local mmdc_args=(
    -i "/data/${rel_input}"
    -o "/data/${rel_output}"
    --backgroundColor white
    --width 2400
    --height 1800
  )

  if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
    docker run --rm \
      -v "${LAB_DIR}:/data" \
      "${MERMAID_IMAGE}" \
      "${mmdc_args[@]}" || {
      log "Docker render failed — falling back to npx..."
      _render_npx "${mmd_file}" "${out_file}"
    }
  elif command -v npx &>/dev/null; then
    _render_npx "${mmd_file}" "${out_file}"
  else
    die "Neither docker nor npx found. Install Docker (preferred) or Node.js >= 18."
  fi

  ok "Rendered: ${out_file#${LAB_DIR}/}"
}

_render_npx() {
  local mmd_file="${1}"; local out_file="${2}"
  command -v npx &>/dev/null || die "npx not found. Install Node.js >= 18."
  npx --yes @mermaid-js/mermaid-cli \
    -i "${mmd_file}" \
    -o "${out_file}" \
    --backgroundColor white \
    --width 2400 \
    --height 1800
}

# ── Render ────────────────────────────────────────────────────
if [[ "${RENDER_ALL}" == true ]]; then
  shopt -s nullglob
  mmds=( "${CALL_DIR}"/docs/*.mmd )
  if [[ ${#mmds[@]} -gt 0 ]]; then
    # Single part: files are in $CALL_DIR/docs/ — mount $CALL_DIR
    LAB_DIR="${CALL_DIR}"
  else
    # No local docs/*.mmd — search all parts under the lab root.
    # Switch the Docker mount to LAB_ROOT so all part paths are accessible.
    LAB_DIR="${LAB_ROOT}"
    mmds=( "${LAB_ROOT}"/*/docs/*.mmd )
    [[ ${#mmds[@]} -gt 0 ]] || die "No .mmd files found in ${CALL_DIR}/docs/ or ${LAB_ROOT}/*/docs/"
  fi
  for f in "${mmds[@]}"; do
    render_file "${f}"
  done
else
  INPUT_FILE="${INPUT_FILE:-docs/architecture.mmd}"
  render_file "${INPUT_FILE}"
fi

echo ""
echo "  To regenerate all diagrams (from lab root):"
echo "    common/scripts/render_diagram.sh --all"
echo ""
echo "  To regenerate diagrams for one part (from that part's directory):"
echo "    ../common/scripts/render_diagram.sh --all"
