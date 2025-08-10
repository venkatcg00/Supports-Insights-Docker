#!/usr/bin/env bash
#
# wait_for_containers_healthy.sh — Wait until all compose services are ready.
#
# Behavior:
# - For services with a healthcheck: waits for "healthy".
# - For services without a healthcheck: waits for the container to be running.
# - Times out after MAX_WAIT_TIME seconds (default 300), polling every SLEEP_INTERVAL (default 5).
# - On timeout, optionally runs `docker compose down -v` (asks unless FORCE=true).
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Constants & Paths ============================ #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"

# .env (prefer infra/.env; fallback to repo root .env)
ENV_FILE="$PROJECT_ROOT/.env"
[[ -f "$ENV_FILE" ]] || ENV_FILE="$(cd "$PROJECT_ROOT/.." && pwd)/.env"

LOG_PREFIX="[WAIT_HEALTH]"
QUIET=${QUIET:-false}
NO_COLOR=${NO_COLOR:-false}
FORCE=${FORCE:-false}

# Config (override by exporting before running)
MAX_WAIT_TIME=${MAX_WAIT_TIME:-300}
SLEEP_INTERVAL=${SLEEP_INTERVAL:-5}

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
exec 3>&1  # for prompts even if stdout is redirected

# ============================== Helpers ================================== #
load_env_if_present() {
  if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck source=/dev/null
    . "$ENV_FILE"
    set +a
    log INFO "Loaded environment from $ENV_FILE"
  else
    log WARN ".env not found at $ENV_FILE (continuing without it)"
  fi
}

need_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing dependency: $1"; }

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

container_exists() {
  local name="$1"
  docker ps -a --format '{{.Names}}' | grep -Fxq "$name"
}

container_health() {
  local name="$1"
  # prints: healthy | starting | unhealthy | none | missing
  if ! container_exists "$name"; then
    echo "missing"; return 0
  fi
  local health
  health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$name" 2>/dev/null || echo "none")"
  echo "$health"
}

container_running() {
  local name="$1"
  docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null | grep -qi true
}

confirm() {
  $FORCE && return 0
  local prompt="$1"
  read -r -p "$(printf "%b%s%b " "$YELLOW" "$prompt [y/N]?" "$NC")" reply <&3 || true
  [[ "${reply,,}" == "y" || "${reply,,}" == "yes" ]]
}

summarize_pending() {
  local -n _arr="$1"
  ((${#_arr[@]}==0)) && return 0
  log WARN "Still waiting on: ${_arr[*]}"
}

# ================================ Main =================================== #
main() {
  need_cmd docker
  load_env_if_present
  [[ -f "$COMPOSE_FILE" ]] || die "Compose file not found: $COMPOSE_FILE"

  # Service names (keys in docker-compose.yml). In your compose you also set container_name
  # equal to the service name, so we can address containers by service.
  mapfile -t services < <(compose ps --services)
  (( ${#services[@]} > 0 )) || die "No services found in compose file."

  log STEP "Waiting for containers to report ready (timeout=${MAX_WAIT_TIME}s, interval=${SLEEP_INTERVAL}s)..."

  local waited=0
  while (( waited <= MAX_WAIT_TIME )); do
    local pending=()
    for svc in "${services[@]}"; do
      # container name is service name in your compose (container_name: <svc>)
      local cname="$svc"
      local h; h="$(container_health "$cname")"

      case "$h" in
        healthy)
          # good
          ;;
        none)
          # no healthcheck — require that it is running
          if ! container_running "$cname"; then
            pending+=("$svc(no-healthcheck:starting)")
          fi
          ;;
        starting)
          pending+=("$svc(starting)")
          ;;
        unhealthy)
          pending+=("$svc(unhealthy)")
          ;;
        missing)
          pending+=("$svc(missing)")
          ;;
        *)
          pending+=("$svc($h)")
          ;;
      esac
    done

    if (( ${#pending[@]} == 0 )); then
      log OK "All containers are ready."
      return 0
    fi

    if (( waited >= MAX_WAIT_TIME )); then
      log ERR "Timeout while waiting for containers."
      summarize_pending pending

      log INFO "Unhealthy containers:"
      docker ps --filter "health=unhealthy" || true
      echo

      if confirm "Run 'docker compose down -v' to reset everything?"; then
        run compose down -v --remove-orphans
      fi
      exit 1
    fi

    summarize_pending pending
    log INFO "Elapsed: ${waited}s / ${MAX_WAIT_TIME}s — checking again in ${SLEEP_INTERVAL}s"
    sleep "$SLEEP_INTERVAL"
    waited=$(( waited + SLEEP_INTERVAL ))
  done
}

main "$@"
