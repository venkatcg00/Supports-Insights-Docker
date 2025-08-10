#!/usr/bin/env bash
#
# platform_setup.sh — Manage the Insights & Telemetry Platform lifecycle.
#
# Usage:
#   ./platform_setup.sh <command> [options]
#
# Commands:
#   setup      Build & start all services, then run one-time init steps
#   start      Start (or build+start) services only
#   stop       Stop services (keeps volumes)
#   delete     Stop services and delete volumes (DANGEROUS)
#   reset      delete -> setup (full rebuild)
#   status     Show service status (add --watch to stream)
#
# Options (global):
#   --no-color       Disable colored output
#   --quiet          Minimal logs
#   --force          Skip confirmation prompts (DANGEROUS with delete/reset)
#   --profile NAME   Set COMPOSE_PROFILES (comma-separated if multiple)
#   --logs SERVICE   After setup/start, tail logs of SERVICE
#
# Notes:
# - Looks for env at: ./infra/.env (preferred) or project-root ./.env
# - Exits non-zero on any failure; prints last error context.
# - Designed to be idempotent; safe to re-run `setup`.
#

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Constants & Paths ============================ #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

# Prefer infra/.env; fallback to repo root .env
ENV_FILE="$SCRIPT_DIR/.env"
[[ -f "$ENV_FILE" ]] || ENV_FILE="$REPO_ROOT/.env"

LOG_PREFIX="[PLATFORM]"
QUIET=false
NO_COLOR=false
FORCE=false
WATCH_STATUS=false
PROFILE_VALUE=""
TAIL_LOGS_SERVICE=""

# =============================== Colors ================================== #
if [[ -t 1 ]]; then
  RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; BLUE='\033[1;34m'; DIM='\033[2m'; NC='\033[0m'
else
  RED=''; YELLOW=''; GREEN=''; BLUE=''; DIM=''; NC=''
fi

# =============================== Logging ================================= #
log()    { $QUIET && return 0; local lvl="$1"; shift; local msg="$*"; $NO_COLOR && echo "${LOG_PREFIX} ${lvl} ${msg}" && return; case "$lvl" in
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
trap 'log ERR "Interrupted"; exit 130' INT
trap 'log ERR "Terminated"; exit 143' TERM
exec 3>&1  # keep FD 3 for prompts even if stdout is redirected

# Wrap any command to capture for trap
run() { _last_cmd="$*"; "$@"; }

# ============================== Helpers ================================== #
usage() {
  cat <<EOF
Usage: $(basename "$0") <command> [options]

Commands:
  setup      Build & start all services, then run init steps
  start      Start (or build+start) services only
  stop       Stop services (keeps volumes)
  delete     Stop services and remove volumes (DANGEROUS)
  reset      Full rebuild: delete -> setup
  status     Show service status (add --watch to stream)

Options:
  --no-color        Disable colors
  --quiet           Minimal logs
  --force           Skip confirmations
  --profile NAME    Set COMPOSE_PROFILES (comma-separated ok)
  --logs SERVICE    Tail logs of SERVICE after start/setup
  --watch           (for status) stream updates

Examples:
  $(basename "$0") setup --profile airflow,superset
  $(basename "$0") delete --force
  $(basename "$0") status --watch
EOF
}

confirm() {
  $FORCE && return 0
  local prompt="$1"
  read -r -p "$(printf "%b%s%b " "$YELLOW" "$prompt [y/N]?" "$NC")" reply <&3 || true
  [[ "${reply,,}" == "y" || "${reply,,}" == "yes" ]]
}

need_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing dependency: $1"; }

load_env() {
  [[ -f "$ENV_FILE" ]] || die ".env not found at $ENV_FILE"

  # Allow blank lines and comments; let bash parse quoting/escapes correctly.
  # Temporarily enable auto-export so variables defined in the file are exported.
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a

  log INFO "Loaded environment from $ENV_FILE"
}

compose() {
  local env=()
  [[ -n "$PROFILE_VALUE" ]] && env+=( "COMPOSE_PROFILES=$PROFILE_VALUE" )
  # shellcheck disable=SC2086
  env ${env[@]+"${env[@]}"} docker compose -f "$COMPOSE_FILE" "$@"
}

retry() {
  local max="$1"; shift
  local delay="$1"; shift
  local n=1
  until "$@"; do
    if (( n >= max )); then return 1; fi
    log WARN "Attempt $n failed. Retrying in ${delay}s…"
    sleep "$delay"; ((n++))
  done
}

time_step() {
  local label="$1"; shift
  log STEP "$label"
  local start end
  start=$(date +%s)
  "$@"
  end=$(date +%s)
  log OK "$label (took $((end - start))s)"
}

tail_logs_if_requested() {
  [[ -z "$TAIL_LOGS_SERVICE" ]] && return 0
  log INFO "Tailing logs for service: $TAIL_LOGS_SERVICE (Ctrl+C to stop)"
  compose logs -f "$TAIL_LOGS_SERVICE"
}

# ========================== Preflight Checks ============================= #
preflight() {
  need_cmd docker
  need_cmd bash
  need_cmd curl
  load_env
  [[ -f "$COMPOSE_FILE" ]] || die "Compose file not found: $COMPOSE_FILE"
  run docker version >/dev/null || die "Docker daemon not available"
  run docker compose version >/dev/null || die "Docker Compose plugin not available"
}

# ============================ Workflows ================================= #
start_platform() {
  preflight

  if compose ps --status running | grep -q 'Up'; then
    log OK "All containers are already running. Skipping recreation."
  else
    time_step "Starting containers (no rebuild)" compose up -d --no-recreate --no-build
    time_step "Waiting for containers health" bash "$SCRIPT_DIR/scripts/wait_for_containers_health.sh"
    log OK "Platform started."
  fi

  tail_logs_if_requested
}

setup_platform() {
  preflight

  # Only build/recreate if containers are not already up
  if compose ps --status running | grep -q 'Up'; then
    log OK "Containers already running. Skipping rebuild."
  else
    time_step "Generating PostgreSQL user SQL" bash "$SCRIPT_DIR/scripts/create_postgres_user.sh"
    time_step "Starting containers" compose up -d
    time_step "Waiting for containers health" bash "$SCRIPT_DIR/scripts/wait_for_containers_health.sh"
  fi

  time_step "Creating MinIO buckets" bash "$SCRIPT_DIR/scripts/create_minio_buckets.sh"
  time_step "Creating Kafka topics" bash "$SCRIPT_DIR/scripts/create_kafka_topics.sh"
  time_step "Verifying Airflow components" bash "$SCRIPT_DIR/scripts/wait_for_airflow_components.sh"
  time_step "Creating Airflow connections" bash "$SCRIPT_DIR/scripts/create_airflow_connections.sh"
  time_step "Creating Airflow variables" bash "$SCRIPT_DIR/scripts/create_airflow_variables.sh"
  time_step "Creating Superset DB connections" bash "$SCRIPT_DIR/scripts/create_superset_connections.sh"
  time_step "Importing Superset dashboards (idempotent)" bash "$SCRIPT_DIR/scripts/import_superset_dashboards.sh"
  time_step "Printing endpoints" bash "$SCRIPT_DIR/scripts/print_endpoints.sh"

  log OK "Setup complete. All services initialized."
  tail_logs_if_requested
}

stop_platform() {
  preflight
  time_step "Stopping containers" compose stop
  log OK "Platform stopped."
}

delete_platform() {
  preflight
  confirm "This will STOP all services and REMOVE volumes. Continue?" \
    || { log WARN "Aborted by user."; return 1; }
  time_step "Deleting platform (down -v)" compose down -v --remove-orphans
  log OK "Platform deleted."
}

reset_platform() {
  preflight
  confirm "Full reset: delete volumes and re-run setup. Continue?" \
    || { log WARN "Aborted by user."; return 1; }
  delete_platform
  setup_platform
}

platform_status() {
  preflight
  if $WATCH_STATUS; then
    log INFO "Watching status (refresh every 3s). Ctrl+C to stop."
    while true; do
      compose ps
      sleep 3
      echo
    done
  else
    compose ps
  fi
}

# ============================== Arg Parse ================================ #
cmd=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    setup|start|stop|delete|reset|status) cmd="$1"; shift;;
    --no-color) NO_COLOR=true; RED=''; YELLOW=''; GREEN=''; BLUE=''; DIM=''; NC=''; shift;;
    --quiet) QUIET=true; shift;;
    --force) FORCE=true; shift;;
    --watch) WATCH_STATUS=true; shift;;
    --profile) PROFILE_VALUE="${2:-}"; [[ -z "$PROFILE_VALUE" ]] && die "--profile requires a value"; shift 2;;
    --logs) TAIL_LOGS_SERVICE="${2:-}"; [[ -z "$TAIL_LOGS_SERVICE" ]] && die "--logs requires SERVICE name"; shift 2;;
    -h|--help) usage; exit 0;;
    *) log WARN "Unknown argument: $1"; usage; exit 2;;
  esac
done

[[ -z "$cmd" ]] && { usage; exit 2; }

# ================================ Main =================================== #
case "$cmd" in
  setup)  setup_platform ;;
  start)  start_platform ;;
  stop)   stop_platform ;;
  delete) delete_platform ;;
  reset)  reset_platform ;;
  status) platform_status ;;
esac
