#!/bin/bash

# Script: platform_setup.sh
# Description: Orchestrates the entire setup of the Support Insights Platform.
# Usage: ./platform_setup.sh [--reset]

set -e

# ========== Config ==========
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
LOG_PREFIX="[PLATFORM SETUP]"

# ========== Logging ==========
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() {
  local message="$1"
  local color="${2:-$NC}"
  echo -e "${color}${LOG_PREFIX} $message${NC}"
}

# ========== Reset Flag ==========
if [[ "$1" == "--reset" ]]; then
  log "Reset flag detected. Stopping containers and removing volumes..." "$YELLOW"
  docker compose -f "$SCRIPT_DIR/docker-compose.yml" down -v
fi

# ========== Load .env ==========
if [ ! -f "$ENV_FILE" ]; then
  log ".env file not found at $ENV_FILE" "$RED"
  exit 1
fi
source "$ENV_FILE"

# ========== Step 1: Generate PostgreSQL User SQL ==========
log "Generating user creation SQL for PostgreSQL..."
bash "$SCRIPT_DIR/setup_bash_scripts/create_postgres_user.sh"

# ========== Step 2: Docker Compose Up ==========
log "Starting Docker containers..." "$YELLOW"
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

# ========== Step 3: Wait for Containers ==========
bash "$SCRIPT_DIR/setup_bash_scripts/wait_for_containers_health.sh"

# ========== Step 4: Create MinIO Buckets ==========
log "Creating MinIO buckets from .env..."
bash "$SCRIPT_DIR/setup_bash_scripts/create_minio_buckets.sh"

# ========== Step 5: Create Kafka Topics ==========
log "Creating Kafka topics from .env..."
bash "$SCRIPT_DIR/setup_bash_scripts/create_kafka_topics.sh"

# ========== Step 6: Start and Wait for Airflow Components ==========
log "Starting and verifying Airflow components..." "$YELLOW"
bash "$SCRIPT_DIR/setup_bash_scripts/wait_for_airflow_components.sh"

# ========== Step 7: Create Airflow Connections ==========
log "Creating Airflow connections..."
bash "$SCRIPT_DIR/setup_bash_scripts/create_airflow_connections.sh"

# ========== Step 8: Create Airflow Variables ==========
log "Creating Airflow variables from .env..."
bash "$SCRIPT_DIR/setup_bash_scripts/create_airflow_variables.sh"

# ========== Step 9: Create Superset Connections ==========
log "Creating Superset database connections..."
bash "$SCRIPT_DIR/setup_bash_scripts/create_superset_connections.sh"

# ========== Step 10: Import Superset Dashboards ==========
log "Importing Superset dashboards if missing..."
bash "$SCRIPT_DIR/setup_bash_scripts/import_superset_dashboards.sh"

# ========== Step 11: Show We UI ports of docker apps ==========
log "Importing Superset dashboards if missing..."
bash "$SCRIPT_DIR/setup_bash_scripts/print_endpoints.sh"

# ========== Done ==========
log "Platform setup complete. All services are up and initialized." "$GREEN"
