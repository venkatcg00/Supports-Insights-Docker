#!/usr/bin/env bash
#
# wait_for_airflow_components.sh — Ensure Airflow components are running.
#
# Behavior (unchanged):
# - Detects the Airflow container (name "airflow" first, then image fallback)
# - Starts scheduler, triggerer, and dag-processor if missing
# - Waits until all are running, or times out and optionally offers a reset
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Constants & Paths ============================ #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"            # points to ./infra
ENV_FILE="$PROJECT_ROOT/.env"                      # keep path unchanged
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"    # explicit compose file

LOG_PREFIX="[AIRFLOW_WAIT]"
QUIET=${QUIET:-false}
NO_COLOR=${NO_COLOR:-false}
FORCE=${FORCE:-false}

# Tunables (can be overridden via env)
RETRY_INTERVAL=${RETRY_INTERVAL:-5}
MAX_RETRIES=${MAX_RETRIES:-60}

# =============================== Colors ================================== #
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
exec 3>&1  # for interactive prompts even if stdout is redirected

# ============================== Helpers ================================== #
need_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing dependency: $1"; }

load_env() {
  [[ -f "$ENV_FILE" ]] || die ".env file not found at $ENV_FILE"
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
  log INFO "Loaded environment from $ENV_FILE"
}

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

confirm() {
  $FORCE && return 0
  local prompt="$1"
  read -r -p "$(printf "%b%s%b " "$YELLOW" "$prompt [y/N]?" "$NC")" reply <&3 || true
  [[ "${reply,,}" == "y" || "${reply,,}" == "yes" ]]
}

find_airflow_container() {
  local name
  # Prefer explicit container name
  if docker ps --format '{{.Names}}' | grep -qw '^airflow$'; then
    echo "airflow"; return 0
  fi
  # Fallback by image (support common bases)
  name="$(docker ps --filter "ancestor=apache/airflow" --format '{{.Names}}' | head -n1 || true)"
  [[ -n "$name" ]] || name="$(docker ps --filter "ancestor=bitnami/airflow" --format '{{.Names}}' | head -n1 || true)"
  [[ -n "$name" ]] || die "Airflow container not found (by name or image)."
  echo "$name"
}

airflow_exec() {
  local container="$1"; shift
  docker exec "$container" "$@"
}

is_running() {
  # Grep the process list inside the container; avoid matching the grep itself
  local container="$1" pattern="$2"
  airflow_exec "$container" bash -lc "ps aux | grep -i \"$pattern\" | grep -v grep" >/dev/null 2>&1
}

start_component() {
  local container="$1" name="$2" cmd="$3"
  if is_running "$container" "$name"; then
    log INFO "$name already running."
  else
    log INFO "Starting $name…"
    # -d sends it to the background on the host; the process runs in the container
    run docker exec -d "$container" bash -lc "$cmd"
  fi
}

print_ps() {
  local container="$1"
  log INFO "Current Airflow processes:"
  airflow_exec "$container" ps aux || true
}

# ================================= Main ================================= #
main() {
  need_cmd docker
  load_env

  [[ -f "$COMPOSE_FILE" ]] || die "Compose file not found: $COMPOSE_FILE"

  local af
  af="$(find_airflow_container)"
  log INFO "Using Airflow container: $af"

  log STEP "Checking which Airflow components are running…"
  start_component "$af" "airflow scheduler"     "airflow scheduler"
  start_component "$af" "airflow triggerer"     "airflow triggerer"
  start_component "$af" "airflow dag-processor" "airflow dag-processor"

  log STEP "Waiting for Airflow components (interval=${RETRY_INTERVAL}s, max=${MAX_RETRIES} tries)…"
  local tries=0
  while (( tries <= MAX_RETRIES )); do
    local ok_sched=""; local ok_trig=""; local ok_proc=""
    is_running "$af" "airflow scheduler"     && ok_sched=ok
    is_running "$af" "airflow triggerer"     && ok_trig=ok
    is_running "$af" "airflow dag-processor" && ok_proc=ok

    if [[ "$ok_sched" == "ok" && "$ok_trig" == "ok" && "$ok_proc" == "ok" ]]; then
      log OK "All Airflow components are running."
      return 0
    fi

    if (( tries >= MAX_RETRIES )); then
      log ERR "Timeout while waiting for Airflow components."
      print_ps "$af"
      if confirm "Run 'docker compose down -v' to reset everything?"; then
        run compose down -v --remove-orphans
      fi
      exit 1
    fi

    log INFO "Still waiting… elapsed=$((tries * RETRY_INTERVAL))s"
    sleep "$RETRY_INTERVAL"
    ((tries++))
  done
}

main "$@"
