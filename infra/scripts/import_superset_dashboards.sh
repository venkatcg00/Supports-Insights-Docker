#!/usr/bin/env bash
#
# import_superset_dashboards.sh — Import Superset dashboards from ZIP exports.
#
# Behavior:
# - Loads ../.env
# - Detects Superset container (prefers name "superset", then image fallback)
# - Waits for /health to be ready
# - Imports dashboards from fixed ZIP path(s) inside the container
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Paths & Config ============================== #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"  # keep as-is

LOG_PREFIX="[SUPERSET_IMPORT]"
QUIET=${QUIET:-false}
NO_COLOR=${NO_COLOR:-false}

# Tunables (override via env if desired)
HEALTH_TIMEOUT=${HEALTH_TIMEOUT:-180}   # seconds
HEALTH_INTERVAL=${HEALTH_INTERVAL:-5}   # seconds

# Fixed ZIP locations inside the container (do not change paths/behavior)
ZIP_PATHS=(
  "/app/exports/dashboard_export.zip"
)

# =============================== Colors ================================= #
if [[ -t 1 && "$NO_COLOR" != "true" ]]; then
  RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; BLUE='\033[1;34m'; DIM='\033[2m'; NC='\033[0m'
else
  RED=''; YELLOW=''; GREEN=''; BLUE=''; DIM=''; NC=''
fi

# =============================== Logging ================================= #
log()    { $QUIET && return 0; local lvl="$1"; shift; local msg="$*"; case "$lvl" in
            INFO)  echo -e "${BLUE}${LOG_PREFIX} [INFO]${NC}  ${msg}";;
            WARN)  echo -e "${YELLOW}${LOG_PREFIX} [WARN]${NC}  ${msg}";;
            OK)    echo -e "${GREEN}${LOG_PREFIX} [OK]${NC}    ${msg}";;
            ERR)   echo -e "${RED}${LOG_PREFIX} [ERR]${NC}   ${msg}";;
            STEP)  echo -e "${DIM}${LOG_PREFIX} [STEP]${NC}  ${msg}";;
            *)     echo -e "${LOG_PREFIX} ${lvl} ${msg}";;
          esac; }
die()    { log ERR "$*"; exit 1; }

# ============================= Error Trap ================================ #
_last_cmd=""
trap 'rc=$?; [[ $rc -ne 0 ]] && log ERR "Command failed: ${_last_cmd} (exit $rc)"; exit $rc' EXIT
run() { _last_cmd="$*"; "$@"; }

# ============================== Helpers ================================= #
need_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing dependency: $1"; }

load_env() {
  [[ -f "$ENV_FILE" ]] || die ".env file not found at $ENV_FILE"
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
  log INFO "Loaded environment from $ENV_FILE"
}

validate_env() {
  local required=( PROJECT_USER )
  local missing=()
  for v in "${required[@]}"; do
    [[ -n "${!v:-}" ]] || missing+=("$v")
  done
  (( ${#missing[@]} == 0 )) || die "Missing required env vars: ${missing[*]}"
}

find_superset_container() {
  local name
  if docker ps --format '{{.Names}}' | grep -qw '^superset$'; then
    echo "superset"; return 0
  fi
  name="$(docker ps --filter "ancestor=apache/superset" --format '{{.Names}}' | head -n1 || true)"
  [[ -n "$name" ]] || name="$(docker ps --filter "ancestor=bitnami/superset" --format '{{.Names}}' | head -n1 || true)"
  [[ -n "$name" ]] || die "Superset container not found (by name or image)."
  echo "$name"
}

wait_for_superset_health() {
  local container="$1"
  local waited=0
  log STEP "Waiting for Superset health (timeout=${HEALTH_TIMEOUT}s, interval=${HEALTH_INTERVAL}s)…"
  while (( waited <= HEALTH_TIMEOUT )); do
    if docker exec "$container" curl -fsS http://localhost:8088/health >/dev/null 2>&1; then
      log OK "Superset is healthy."
      return 0
    fi
    if (( waited >= HEALTH_TIMEOUT )); then
      die "Timed out waiting for Superset health endpoint."
    fi
    log INFO "Superset not healthy yet… elapsed=${waited}s"
    sleep "$HEALTH_INTERVAL"
    waited=$(( waited + HEALTH_INTERVAL ))
  done
}

import_zip() {
  local container="$1" zip_path="$2"
  log INFO "Checking for ZIP: ${zip_path}"
  if docker exec "$container" bash -lc "[ -f \"$zip_path\" ]"; then
    log INFO "Importing dashboards from ${zip_path}"
    # --username is preserved from your original script
    if ! docker exec "$container" superset import-dashboards --path "$zip_path" --username "$PROJECT_USER" >/dev/null 2>&1; then
      log ERR "Failed to import $zip_path (check Superset version / CLI flags)."
      log INFO "Tip: 'docker exec $container superset import-dashboards --help'"
      return 1
    fi
    log OK "Imported ${zip_path}"
  else
    log WARN "ZIP not found: ${zip_path} (skipping)"
  fi
}

# ================================= Main ================================= #
main() {
  need_cmd docker
  need_cmd curl

  load_env
  validate_env

  local superset
  superset="$(find_superset_container)"
  log INFO "Using Superset container: ${superset}"

  wait_for_superset_health "$superset"

  log STEP "Importing dashboard ZIP exports into Superset…"
  local z
  for z in "${ZIP_PATHS[@]}"; do
    import_zip "$superset" "$z"
  done

  log OK "Dashboard import completed."
}

main "$@"
