#!/bin/bash

# Script: platform_setup.sh
# Description: Manages the Insights & Telemetry Platform lifecycle.
# Usage: ./platform_setup.sh [setup|start|stop|delete|reset|status]

set -e

# ========== Config ==========
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
LOG_PREFIX="[PLATFORM SETUP]"
DOCKER_COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

# ========== Logging ==========
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[1;34m'
NC='\033[0m'

log() {
  local message="$1"
  local color="${2:-$NC}"
  echo -e "${color}${LOG_PREFIX} $message${NC}"
}

# ========== Helper Functions ==========
check_env_file() {
  if [ ! -f "$ENV_FILE" ]; then
    log ".env file not found at $ENV_FILE" "$RED"
    exit 1
  fi
  source "$ENV_FILE"
}

# ========== Commands ==========

setup_platform() {
  check_env_file
  
  log "Generating PostgreSQL user SQL..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/create_postgres_user.sh"

  log "Starting Docker containers..." "$YELLOW"
  docker compose -f "$DOCKER_COMPOSE_FILE" up -d

  log "Waiting for containers to become healthy..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/wait_for_containers_health.sh"

  log "Creating MinIO buckets..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/create_minio_buckets.sh"

  log "Creating Kafka topics..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/create_kafka_topics.sh"

  log "Starting and verifying Airflow components..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/wait_for_airflow_components.sh"

  log "Creating Airflow connections..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/create_airflow_connections.sh"

  log "Creating Airflow variables..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/create_airflow_variables.sh"

  log "Creating Superset database connections..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/create_superset_connections.sh"

  log "Importing Superset dashboards if missing..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/import_superset_dashboards.sh"

  log "Displaying platform endpoints..." "$BLUE"
  bash "$SCRIPT_DIR/setup_bash_scripts/print_endpoints.sh"

  log "Setup complete. All services are up and initialized." "$GREEN"
}

start_platform() {
  log "Starting Docker containers..." "$YELLOW"
  docker compose -f "$DOCKER_COMPOSE_FILE" up -d
  bash "$SCRIPT_DIR/setup_bash_scripts/wait_for_containers_health.sh"
  log "Platform started." "$GREEN"
}

stop_platform() {
  log "Stopping Docker containers..." "$YELLOW"
  docker compose -f "$DOCKER_COMPOSE_FILE" stop
  log "Platform stopped." "$GREEN"
}

delete_platform() {
  log "Deleting the platform: stopping containers and removing volumes..." "$RED"
  docker compose -f "$DOCKER_COMPOSE_FILE" down -v
  log "Platform deleted." "$GREEN"
}

reset_platform() {
  delete_platform
  setup_platform
}

platform_status() {
  log "Displaying Docker container status:" "$YELLOW"
  docker compose -f "$DOCKER_COMPOSE_FILE" ps
}

# ========== Main ==========

case "$1" in
  setup)
    setup_platform
    ;;
  start)
    start_platform
    ;;
  stop)
    stop_platform
    ;;
  delete)
    delete_platform
    ;;
  reset)
    reset_platform
    ;;
  status)
    platform_status
    ;;
  *)
    echo -e "${YELLOW}Usage: $0 {setup|start|stop|delete|reset|status}${NC}"
    exit 1
    ;;
esac
