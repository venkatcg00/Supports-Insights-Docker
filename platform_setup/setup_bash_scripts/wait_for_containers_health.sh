#!/bin/bash

# Script: wait_for_containers_healthy.sh
# Description: Waits for all running containers to become healthy.
# Usage: Called from platform_setup.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

# Load .env
if [ -f "$ENV_FILE" ]; then
  source "$ENV_FILE"
else
  echo "[ERROR] .env not found at $ENV_FILE"
  exit 1
fi

# Config
MAX_WAIT_TIME=300   # total 5 minutes
SLEEP_INTERVAL=5
TIME_WAITED=0

echo "[INFO] Waiting for all containers to report healthy..."

while true; do
  UNHEALTHY_CONTAINERS=$(docker ps --filter "health=unhealthy" --format '{{.Names}}')
  STARTING_CONTAINERS=$(docker ps --filter "health=starting" --format '{{.Names}}')
  HEALTHY_COUNT=$(docker ps --filter "health=healthy" --format '{{.Names}}' | wc -l)
  TOTAL_EXPECTED=$(docker compose ps --services | wc -l)

  if [ -z "$UNHEALTHY_CONTAINERS" ] && [ -z "$STARTING_CONTAINERS" ] && [ "$HEALTHY_COUNT" -eq "$TOTAL_EXPECTED" ]; then
    echo "[SUCCESS] All containers are healthy."
    break
  fi

  if [ "$TIME_WAITED" -ge "$MAX_WAIT_TIME" ]; then
    echo "[ERROR] Timeout reached while waiting for containers to become healthy."
    echo "Unhealthy containers:"
    docker ps --filter "health=unhealthy"
    echo ""
    read -rp "Do you want to run 'docker compose down -v'? (y/n): " choice
    if [[ "$choice" =~ ^[Yy]$ ]]; then
      docker compose down -v
    fi
    exit 1
  fi

  echo "[INFO] Still waiting... Elapsed: ${TIME_WAITED}s / ${MAX_WAIT_TIME}s"
  sleep "$SLEEP_INTERVAL"
  TIME_WAITED=$((TIME_WAITED + SLEEP_INTERVAL))
done
