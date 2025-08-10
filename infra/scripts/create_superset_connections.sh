#!/usr/bin/env bash
#
# add_superset_connections.sh — Idempotently add Superset DB connections
# (Postgres) using YAML inside the Superset container.
#
# Behavior:
# - Loads ../.env
# - Detects Superset container (prefers name "superset", then image fallback)
# - Waits for /health to be ready
# - Writes YAML to $HOME/tmp/protected_superset_dbs.yaml, copies into container,
#   imports via `superset import-datasources`, then cleans up.
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Paths & Config ============================== #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"  # keep as-is

LOG_PREFIX="[SUPERSET_CONN]"
QUIET=${QUIET:-false}
NO_COLOR=${NO_COLOR:-false}

# Tunables (override via env if desired)
HEALTH_TIMEOUT=${HEALTH_TIMEOUT:-180}   # seconds
HEALTH_INTERVAL=${HEALTH_INTERVAL:-5}   # seconds
SUPERSET_PORT_DEFAULT=8088              # only for messages; request uses container localhost

# Temporary file locations (keep your path convention)
HOST_TMP_DIR="${HOME}/tmp"
HOST_TEMP_YAML="${HOST_TMP_DIR}/protected_superset_dbs.yaml"
CONTAINER_TEMP_YAML="/tmp/protected_superset_dbs.yaml"

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
cleanup() {
  # best-effort cleanup; do not fail the script on cleanup errors
  rm -f "$HOST_TEMP_YAML" 2>/dev/null || true
  if [[ -n "${SUPERSET_CONTAINER:-}" ]]; then
    docker exec -u root "$SUPERSET_CONTAINER" rm -f "$CONTAINER_TEMP_YAML" >/dev/null 2>&1 || true
  fi
}
trap 'rc=$?; [[ $rc -ne 0 ]] && log ERR "Command failed: ${_last_cmd} (exit $rc)"; cleanup; exit $rc' EXIT
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
  local required=(
    POSTGRES_DEFAULT_USER PROJECT_PASSWORD
    POSTGRES_HOST POSTGRES_PORT POSTGRES_DATABASE_NAME
  )
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
  # Common images
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

write_yaml() {
  run mkdir -p "$HOST_TMP_DIR"
  log STEP "Writing datasource YAML to $HOST_TEMP_YAML"
  cat > "$HOST_TEMP_YAML" <<EOF
databases:
  - database_name: Postgres_Project_DB
    sqlalchemy_uri: postgresql://${POSTGRES_DEFAULT_USER}:${PROJECT_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE_NAME}
    extra: "{}"
    allow_dml: true
EOF
}

copy_and_import() {
  local container="$1"
  log INFO "Copying YAML into container: $CONTAINER_TEMP_YAML"
  run docker cp "$HOST_TEMP_YAML" "${container}:${CONTAINER_TEMP_YAML}"

  log STEP "Importing database connections (superset import-datasources)…"
  if ! docker exec "$container" superset import-datasources --path "$CONTAINER_TEMP_YAML" >/dev/null 2>&1; then
    log ERR "Failed to import datasources. Check Superset version and supported flags."
    log INFO "Tip: run 'docker exec $container superset import-datasources --help' to see available options."
    return 1
  fi

  log OK "Superset DB connections created or updated."
  return 0
}

# ================================= Main ================================= #
main() {
  need_cmd docker
  need_cmd curl

  load_env
  validate_env

  SUPERSET_CONTAINER="$(find_superset_container)"
  log INFO "Using Superset container: ${SUPERSET_CONTAINER}"

  wait_for_superset_health "$SUPERSET_CONTAINER"
  write_yaml
  copy_and_import "$SUPERSET_CONTAINER"
}

main "$@"
