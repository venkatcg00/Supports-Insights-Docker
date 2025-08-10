#!/usr/bin/env bash
#
# create_airflow_variables.sh — Add Airflow variables from .env entries.
#
# Behavior:
# - Reads *_BUCKET= and *_TOPIC= from ../.env
# - Detects Airflow container (prefers name "airflow", then image fallback)
# - Sets Airflow variables via `airflow variables set` (idempotent upsert)
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Paths & Config ============================== #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"   # keep path unchanged

LOG_PREFIX="[AIRFLOW_VARS]"
QUIET=${QUIET:-false}
NO_COLOR=${NO_COLOR:-false}

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
  # Load to make values available if referenced elsewhere; not strictly required
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
  log INFO "Loaded environment from $ENV_FILE"
}

find_airflow_container() {
  local name
  if docker ps --format '{{.Names}}' | grep -qw '^airflow$'; then
    echo "airflow"; return 0
  fi
  name="$(docker ps --filter "ancestor=apache/airflow" --format '{{.Names}}' | head -n1 || true)"
  [[ -n "$name" ]] || name="$(docker ps --filter "ancestor=bitnami/airflow" --format '{{.Names}}' | head -n1 || true)"
  [[ -n "$name" ]] || die "Airflow container not found (by name or image)."
  echo "$name"
}

af_exec() {
  local container="$1"; shift
  docker exec "$container" "$@"
}

# Parse *_BUCKET= and *_TOPIC= lines from .env
# - ignore comments/blank lines
# - keep only keys/values we want
# - strip surrounding quotes from values
# - deduplicate by key
collect_env_vars() {
  declare -Ag VARS=()
  local line key val

  while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" =~ ^[[:space:]]*$ ]] && continue
    [[ "$line" =~ (_BUCKET=|_TOPIC=) ]] || continue

    key="${line%%=*}"
    val="${line#*=}"

    # Strip optional surrounding quotes
    val="${val%\"}"; val="${val#\"}"
    val="${val%\'}"; val="${val#\'}"
    # Trim whitespace
    val="$(echo -e "$val" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"

    [[ -n "$key" && -n "$val" ]] && VARS["$key"]="$val"
  done < "$ENV_FILE"

  KEYS=()
  local k
  for k in "${!VARS[@]}"; do
    KEYS+=("$k")
  done
}

set_airflow_var() {
  local container="$1" key="$2" value="$3"
  # Use bash -lc to ensure the Airflow CLI is on PATH as configured in the container
  log INFO "Setting Airflow var: ${key}=${value}"
  run af_exec "$container" bash -lc "airflow variables set \"$key\" \"$value\""
}

# ================================= Main ================================= #
main() {
  need_cmd docker
  load_env

  local afc
  afc="$(find_airflow_container)"
  log STEP "Adding Airflow variables inside ${afc}…"

  collect_env_vars
  if (( ${#KEYS[@]} == 0 )); then
    log INFO "No *_BUCKET or *_TOPIC entries found in .env — nothing to do."
    exit 0
  fi

  local key
  for key in "${KEYS[@]}"; do
    set_airflow_var "$afc" "$key" "${VARS[$key]}"
  done

  log OK "Airflow variables created/updated."
}

main "$@"
