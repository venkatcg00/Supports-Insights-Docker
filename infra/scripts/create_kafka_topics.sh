#!/usr/bin/env bash
#
# create_kafka_topics.sh — Ensure Kafka topics (from .env *_TOPIC= entries) exist.
#
# Behavior:
# - Reads *_TOPIC= values from ../.env
# - Runs kafka-topics.sh inside the Bitnami Kafka container
# - Creates topics if missing with partitions=1, replication-factor=1
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Paths & Config ============================== #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"  # keep path unchanged

LOG_PREFIX="[KAFKA_TOPICS]"
QUIET=${QUIET:-false}
NO_COLOR=${NO_COLOR:-false}

# Optional tuning
BROKER_RETRIES=${BROKER_RETRIES:-10}
BROKER_RETRY_DELAY=${BROKER_RETRY_DELAY:-3}

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
  local required=( KAFKA_BROKER_HOST KAFKA_BROKER_PORT )
  local missing=()
  for v in "${required[@]}"; do
    [[ -n "${!v:-}" ]] || missing+=("$v")
  done
  (( ${#missing[@]} == 0 )) || die "Missing required env vars: ${missing[*]}"
}

collect_topics() {
  # Parse *_TOPIC= lines; strip quotes; ignore blank/comment; dedup
  declare -Ag TOPIC_SET=()
  local line v
  while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" =~ ^[[:space:]]*$ ]] && continue
    [[ "$line" =~ ^[A-Z0-9_]+_TOPIC= ]] || continue

    v="${line#*=}"
    # strip optional surrounding quotes
    v="${v%\"}"; v="${v#\"}"
    v="${v%\'}"; v="${v#\'}"
    # trim whitespace
    v="$(echo -e "$v" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    [[ -n "$v" ]] && TOPIC_SET["$v"]=1
  done < "$ENV_FILE"

  TOPICS=()
  local t
  for t in "${!TOPIC_SET[@]}"; do
    TOPICS+=("$t")
  done
}

find_kafka_container() {
  local name
  if docker ps --format '{{.Names}}' | grep -qw '^kafka$'; then
    echo "kafka"; return 0
  fi
  name="$(docker ps --filter "ancestor=bitnami/kafka" --format '{{.Names}}' | head -n1 || true)"
  [[ -n "$name" ]] || die "Kafka container not found (by name or image)."
  echo "$name"
}

kafka_exec() {
  local container="$1"; shift
  docker exec "$container" kafka-topics.sh "$@"
}

wait_for_broker() {
  local container="$1"
  local i=1
  local bs="${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"

  while (( i <= BROKER_RETRIES )); do
    if kafka_exec "$container" --bootstrap-server "$bs" --list >/dev/null 2>&1; then
      log OK "Kafka broker reachable at $bs"
      return 0
    fi
    log WARN "Broker not ready at $bs (attempt $i/$BROKER_RETRIES). Retrying in ${BROKER_RETRY_DELAY}s…"
    sleep "$BROKER_RETRY_DELAY"
    ((i++))
  done
  die "Kafka broker not reachable at $bs after ${BROKER_RETRIES} attempts."
}

ensure_topic() {
  local container="$1" topic="$2"
  local bs="${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"

  if kafka_exec "$container" --bootstrap-server "$bs" --topic "$topic" --describe >/dev/null 2>&1; then
    log INFO "Topic '${topic}' exists."
  else
    log INFO "Creating topic '${topic}'…"
    run kafka_exec "$container" \
      --bootstrap-server "$bs" \
      --create --if-not-exists \
      --topic "$topic" \
      --partitions 1 \
      --replication-factor 1
    log OK "Created topic '${topic}'."
  fi
}

# ================================= Main ================================= #
main() {
  need_cmd docker
  load_env
  validate_env
  collect_topics

  if (( ${#TOPICS[@]} == 0 )); then
    log INFO "No *_TOPIC entries found in .env — nothing to do."
    exit 0
  fi

  log STEP "Topics to ensure: ${TOPICS[*]}"

  local container
  container="$(find_kafka_container)"
  log INFO "Using Kafka container: $container"

  wait_for_broker "$container"

  local t
  for t in "${TOPICS[@]}"; do
    ensure_topic "$container" "$t"
  done

  log OK "Kafka topic setup complete."
}

main "$@"
