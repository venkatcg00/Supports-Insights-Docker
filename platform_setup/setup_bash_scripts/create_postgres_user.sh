#!/bin/bash

# Script: create_postgres_user.sh
# Description: Generates 16_user_creation.sql in correct order to create and grant privileges to the project user.
# Requirements: Must be sourced after .env is loaded and used after schema creation SQLs.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SQL_OUTPUT="$PROJECT_ROOT/database_setup_sql_scripts/16_user_creation.sql"

# Source .env if not already sourced
if [ -z "$PROJECT_USER" ]; then
  ENV_FILE="$PROJECT_ROOT/.env"
  if [ ! -f "$ENV_FILE" ]; then
    echo "[ERROR] .env file not found at $ENV_FILE"
    exit 1
  fi
  source "$ENV_FILE"
fi

# Ensure required variables are present
REQUIRED_VARS=(
  PROJECT_USER PROJECT_PASSWORD
  POSTGRES_DATABASE_NAME
  AIRFLOW_METADATA_DATABASE
  SUPERSET_METADATA_DATABASE
  HIVE_METASTORE_DATABASE
)

for var in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!var}" ]; then
    echo "[ERROR] Environment variable $var is not set"
    exit 1
  fi
done

echo "[INFO] Generating SQL script at $SQL_OUTPUT"

cat > "$SQL_OUTPUT" <<EOF
-- Ensure we're inside the correct DB
\\c support_insights;

-- Create user if not exists
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${PROJECT_USER}') THEN
      CREATE ROLE "${PROJECT_USER}" WITH LOGIN PASSWORD '${PROJECT_PASSWORD}' SUPERUSER;
   END IF;
END \$\$;

-- Ensure full access (even if role already existed)
ALTER ROLE "${PROJECT_USER}" WITH SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN;

-- Grant access to all important databases
GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DATABASE_NAME} TO "${PROJECT_USER}";
GRANT ALL PRIVILEGES ON DATABASE ${AIRFLOW_METADATA_DATABASE} TO "${PROJECT_USER}";
GRANT ALL PRIVILEGES ON DATABASE ${SUPERSET_METADATA_DATABASE} TO "${PROJECT_USER}";
GRANT ALL PRIVILEGES ON DATABASE ${HIVE_METASTORE_DATABASE} TO "${PROJECT_USER}";

-- Set search_path
ALTER ROLE "${PROJECT_USER}" SET search_path TO ds, info, aud, lnd, cdc, prs, pre_dm, dm, dwh, public;
EOF

echo "[SUCCESS] User creation SQL generated at $SQL_OUTPUT"
