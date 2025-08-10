#!/usr/bin/env bash
#
# create_postgres_user.sh — Generate infra/sql/16_user_creation.sql
# to create the project role and grant privileges.
#
# Usage: invoked by platform_setup.sh

set -Eeuo pipefail
IFS=$'\n\t'

# =========================== Constants & Paths ============================ #
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SQL_OUTPUT="$PROJECT_ROOT/sql/16_user_creation.sql"
ENV_FILE="$PROJECT_ROOT/.env"   # Do not change: matches current working setup

LOG_PREFIX="[CREATE_PG_USER]"
QUIET=${QUIET:-false}
NO_COLOR=${NO_COLOR:-false}

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

# ============================== Helpers ================================== #
load_env_if_needed() {
  if [[ -z "${PROJECT_USER:-}" ]]; then
    [[ -f "$ENV_FILE" ]] || die ".env file not found at $ENV_FILE"
    # Allow blanks/comments, preserve quoting; export variables automatically.
    set -a
    # shellcheck source=/dev/null
    . "$ENV_FILE"
    set +a
    log INFO "Loaded environment from $ENV_FILE"
  else
    log INFO "Environment already present; skipping .env load"
  fi
}

validate_env() {
  local required_vars=(
    PROJECT_USER
    PROJECT_PASSWORD
    POSTGRES_DATABASE_NAME
    AIRFLOW_METADATA_DATABASE
    SUPERSET_METADATA_DATABASE
  )
  local missing=()
  for var in "${required_vars[@]}"; do
    if [[ -z "${!var:-}" ]]; then
      missing+=("$var")
    fi
  done
  (( ${#missing[@]} == 0 )) || die "Missing required env vars: ${missing[*]}"
}

prepare_output() {
  local out_dir
  out_dir="$(dirname "$SQL_OUTPUT")"
  [[ -d "$out_dir" ]] || run mkdir -p "$out_dir"
  # quick write test
  : > "$SQL_OUTPUT" 2>/dev/null || die "Cannot write to $SQL_OUTPUT"
}

generate_sql() {
  log STEP "Generating SQL script at $SQL_OUTPUT"
  cat > "$SQL_OUTPUT" <<'EOF'
-- Ensure we're inside the correct DB
\c support_insights;

-- Create user if not exists
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '__PROJECT_USER__') THEN
      CREATE ROLE "__PROJECT_USER__" WITH LOGIN PASSWORD '__PROJECT_PASSWORD__' SUPERUSER;
   END IF;
END $$;

-- Ensure full access (even if role already existed)
ALTER ROLE "__PROJECT_USER__" WITH SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN;

-- Grant access to all important databases
GRANT ALL PRIVILEGES ON DATABASE __POSTGRES_DATABASE_NAME__     TO "__PROJECT_USER__";
GRANT ALL PRIVILEGES ON DATABASE __AIRFLOW_METADATA_DATABASE__  TO "__PROJECT_USER__";
GRANT ALL PRIVILEGES ON DATABASE __SUPERSET_METADATA_DATABASE__ TO "__PROJECT_USER__";

-- Set search_path
ALTER ROLE "__PROJECT_USER__" SET search_path TO ds, info, aud, lnd, cdc, prs, pre_dm, dm, dwh, vw, public;
EOF

  # Inject environment values (simple token replacement; keeps behavior intact)
  # Note: not changing hard-coded \c target per your request.
  run sed -i \
    -e "s/__PROJECT_USER__/${PROJECT_USER//\//\\/}/g" \
    -e "s/__PROJECT_PASSWORD__/${PROJECT_PASSWORD//\//\\/}/g" \
    -e "s/__POSTGRES_DATABASE_NAME__/${POSTGRES_DATABASE_NAME//\//\\/}/g" \
    -e "s/__AIRFLOW_METADATA_DATABASE__/${AIRFLOW_METADATA_DATABASE//\//\\/}/g" \
    -e "s/__SUPERSET_METADATA_DATABASE__/${SUPERSET_METADATA_DATABASE//\//\\/}/g" \
    "$SQL_OUTPUT"

  log OK "User creation SQL generated."
}

# ================================= Main ================================= #
main() {
  load_env_if_needed
  validate_env
  prepare_output
  generate_sql
}

main "$@"
