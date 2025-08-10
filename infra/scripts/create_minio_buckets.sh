#!/usr/bin/env bash
#
# create_minio_buckets.sh — Ensure MinIO buckets defined in .env exist.
#
# Behavior:
# - Reads bucket names from *_BUCKET= lines in ../.env
# - Uses `mc` inside the running MinIO container to create buckets if missing
# - Resolves the MinIO container by explicit name `minio`, then by image
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Paths & Config ============================== #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"  # keep path unchanged

LOG_PREFIX="[MINIO_BUCKETS]"
QUIET=${QUIET:-false}
NO_COLOR=${NO_COLOR:-false}
FORCE=${FORCE:-false}

# Defaults (can be overridden via env before calling)
ALIAS_NAME=${ALIAS_NAME:-localminio}
ALIAS_RETRIES=${ALIAS_RETRIES:-3}
ALIAS_RETRY_DELAY=${ALIAS_RETRY_DELAY:-2}

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
  # Use shell to parse quoting correctly but don't pollute the environment
  # with unwanted names: we only need MINIO_* and PROJECT_* here.
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
  log INFO "Loaded environment from $ENV_FILE"
}

validate_env() {
  local required=( MINIO_HOST MINIO_PORT PROJECT_USER PROJECT_PASSWORD )
  local missing=()
  for v in "${required[@]}"; do
    [[ -n "${!v:-}" ]] || missing+=("$v")
  done
  (( ${#missing[@]} == 0 )) || die "Missing required env vars: ${missing[*]}"
}

# Extract bucket names from *_BUCKET= lines; strip quotes; ignore blanks/comments; dedup.
collect_buckets() {
  local line k v bucket
  declare -Ag BUCKET_SET=()
  while IFS= read -r line; do
    # skip comments/blank
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" =~ ^[[:space:]]*$ ]] && continue
    # only *_BUCKET=
    [[ "$line" =~ ^[A-Z0-9_]+_BUCKET= ]] || continue

    k="${line%%=*}"
    v="${line#*=}"

    # strip optional surrounding quotes
    v="${v%\"}"; v="${v#\"}"
    v="${v%\'}"; v="${v#\'}"

    # trim whitespace
    v="$(echo -e "$v" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"

    [[ -n "$v" ]] && BUCKET_SET["$v"]=1
  done < "$ENV_FILE"

  BUCKETS=()
  for bucket in "${!BUCKET_SET[@]}"; do
    BUCKETS+=("$bucket")
  done
}

find_minio_container() {
  local name
  if docker ps --format '{{.Names}}' | grep -qw '^minio$'; then
    echo "minio"; return 0
  fi
  name="$(docker ps --filter "ancestor=minio/minio" --format '{{.Names}}' | head -n1 || true)"
  [[ -n "$name" ]] || die "Could not find a running MinIO container (by name or image)."
  echo "$name"
}

mc_exec() {
  local container="$1"; shift
  docker exec "$container" mc "$@"
}

ensure_alias() {
  local container="$1"
  local attempt=1
  local target_url="http://${MINIO_HOST}:${MINIO_PORT}"

  while (( attempt <= ALIAS_RETRIES )); do
    if mc_exec "$container" alias set "$ALIAS_NAME" "$target_url" "$PROJECT_USER" "$PROJECT_PASSWORD" >/dev/null 2>&1; then
      log OK "Configured mc alias '$ALIAS_NAME' → $target_url"
      return 0
    fi
    log WARN "Failed to set mc alias (attempt $attempt/${ALIAS_RETRIES}). Retrying in ${ALIAS_RETRY_DELAY}s…"
    sleep "$ALIAS_RETRY_DELAY"
    ((attempt++))
  done

  # try a no-op to see if already configured
  if mc_exec "$container" alias list | grep -q " $ALIAS_NAME "; then
    log OK "mc alias '$ALIAS_NAME' already present."
    return 0
  fi

  die "Unable to configure mc alias '$ALIAS_NAME'."
}

ensure_bucket() {
  local container="$1"
  local bucket="$2"

  if mc_exec "$container" ls "${ALIAS_NAME}/${bucket}" >/dev/null 2>&1; then
    log INFO "Bucket '${bucket}' exists."
  else
    log INFO "Creating bucket '${bucket}'…"
    run mc_exec "$container" mb "${ALIAS_NAME}/${bucket}"
    log OK "Created bucket '${bucket}'."
  fi
}

# ================================= Main ================================= #
main() {
  need_cmd docker
  load_env
  validate_env
  collect_buckets

  if (( ${#BUCKETS[@]} == 0 )); then
    log INFO "No *_BUCKET entries found in .env — nothing to do."
    exit 0
  fi
  log STEP "Buckets to ensure exist: ${BUCKETS[*]}"

  local container
  container="$(find_minio_container)"
  log INFO "Using MinIO container: $container"

  ensure_alias "$container"

  local b
  for b in "${BUCKETS[@]}"; do
    ensure_bucket "$container" "$b"
  done

  log OK "All MinIO buckets are present."
}

main "$@"
