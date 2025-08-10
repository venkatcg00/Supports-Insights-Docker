#!/usr/bin/env bash
#
# create_airflow_connections.sh — Create/update Airflow connections via CLI.
#
# Behavior:
# - Uses .env at ../.env for connection details
# - Detects Airflow container (prefers name "airflow", then image fallback)
# - For each connection: delete if exists, then add (idempotent outcome)
# - Handles MinIO with --conn-extra JSON
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Paths & Config ============================== #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"  # keep path unchanged

LOG_PREFIX="[AIRFLOW_CONN]"
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
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
  log INFO "Loaded environment from $ENV_FILE"
}

validate_env() {
  local required=(
    PROJECT_USER PROJECT_PASSWORD
    POSTGRES_HOST POSTGRES_PORT POSTGRES_DATABASE_NAME
    MONGO_HOST MONGO_PORT MONGO_DATABASE_NAME
    KAFKA_BROKER_HOST KAFKA_BROKER_PORT
    MINIO_HOST MINIO_PORT
  )
  local missing=()
  for v in "${required[@]}"; do
    [[ -n "${!v:-}" ]] || missing+=("$v")
  done
  (( ${#missing[@]} == 0 )) || die "Missing required env vars: ${missing[*]}"
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

conn_exists() {
  local container="$1" id="$2"
  af_exec "$container" airflow connections get "$id" >/dev/null 2>&1
}

conn_delete_if_exists() {
  local container="$1" id="$2"
  if conn_exists "$container" "$id"; then
    log INFO "$id exists → deleting"
    run af_exec "$container" airflow connections delete "$id"
  else
    log INFO "$id not found"
  fi
}

add_connection_uri() {
  local container="$1" id="$2" uri="$3"
  log INFO "Creating $id"
  run af_exec "$container" airflow connections add "$id" --conn-uri "$uri"
}

add_connection_minio_extra() {
  local container="$1" id="$2"
  local host="http://${MINIO_HOST}:${MINIO_PORT}"
  # Create the JSON inside the container to avoid complex host quoting
  run af_exec "$container" bash -lc "cat > /tmp/${id}.json <<'JSON'
{
  \"host\": \"${host}\",
  \"aws_access_key_id\": \"${PROJECT_USER}\",
  \"aws_secret_access_key\": \"${PROJECT_PASSWORD}\",
  \"endpoint_url\": \"${host}\",
  \"region_name\": \"us-east-1\",
  \"verify\": false
}
JSON
airflow connections add '${id}' --conn-type aws --conn-extra \"\$(cat /tmp/${id}.json)\" && rm -f /tmp/${id}.json"
}

# ================================= Main ================================= #
main() {
  need_cmd docker
  load_env
  validate_env

  local afc
  afc="$(find_airflow_container)"
  log STEP "Creating Airflow connections inside ${afc}…"

  # Postgres
  conn_delete_if_exists "$afc" "postgres_project_connection"
  add_connection_uri "$afc" "postgres_project_connection" \
    "postgresql://${PROJECT_USER}:${PROJECT_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE_NAME}"

  # Mongo
  conn_delete_if_exists "$afc" "mongo_project_connection"
  add_connection_uri "$afc" "mongo_project_connection" \
    "mongo://${PROJECT_USER}:${PROJECT_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${MONGO_DATABASE_NAME}?authSource=admin"

  # Kafka
  conn_delete_if_exists "$afc" "kafka_project_connection"
  add_connection_uri "$afc" "kafka_project_connection" \
    "kafka://${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"

  # MinIO (aws conn with extra JSON)
  conn_delete_if_exists "$afc" "minio_project_connection"
  log INFO "Creating minio_project_connection"
  add_connection_minio_extra "$afc" "minio_project_connection"

  log OK "All Airflow project connections created or updated."
}

main "$@"
