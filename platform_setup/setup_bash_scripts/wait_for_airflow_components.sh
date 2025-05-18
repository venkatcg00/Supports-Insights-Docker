#!/bin/bash

# Script: wait_for_airflow_components.sh
# Description: Starts missing Airflow components (scheduler, triggerer, dag-processor) and waits for them to run

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

if [ -f "$ENV_FILE" ]; then
  source "$ENV_FILE"
else
  echo "[ERROR] .env file not found at $ENV_FILE"
  exit 1
fi

# Detect Airflow container by name first, then fallback to image
if docker ps --format '{{.Names}}' | grep -qw airflow; then
  AIRFLOW_CONTAINER=airflow
elif docker ps --filter "ancestor=bitnami/airflow" --format '{{.Names}}' | grep -q .; then
  AIRFLOW_CONTAINER=$(docker ps --filter "ancestor=bitnami/airflow" --format '{{.Names}}' | head -n1)
else
  echo "[ERROR] Airflow container not found (by name or image)."
  exit 1
fi

echo "[INFO] Checking which Airflow components are already running..."

# Helper to check running process
is_running() {
  docker exec "$AIRFLOW_CONTAINER" ps aux | grep -i "$1" | grep -v grep > /dev/null
}

start_component() {
  local name="$1"
  local cmd="$2"
  if is_running "$name"; then
    echo "[SKIP] $name already running"
  else
    echo "[START] Starting $name..."
    docker exec -d "$AIRFLOW_CONTAINER" bash -c "$cmd"
  fi
}

# Start each component manually (if not running)
start_component "airflow scheduler" "airflow scheduler"
start_component "airflow triggerer" "airflow triggerer"
start_component "airflow dag-processor" "airflow dag-processor"

# Wait for them to appear in process list
echo "[INFO] Waiting for all Airflow components to be running..."

RETRY_INTERVAL=5
MAX_RETRIES=60
RETRIES=0

while true; do
  SCHEDULER_OK=$(is_running "airflow scheduler" && echo ok)
  TRIGGERER_OK=$(is_running "airflow triggerer" && echo ok)
  DAG_PROCESSOR_OK=$(is_running "airflow dag-processor" && echo ok)

  if [[ "$SCHEDULER_OK" == "ok" && "$TRIGGERER_OK" == "ok" && "$DAG_PROCESSOR_OK" == "ok" ]]; then
    echo "[SUCCESS] All Airflow components are now running."
    break
  fi

  if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
    echo "[ERROR] Timeout while waiting for Airflow components."
    docker exec "$AIRFLOW_CONTAINER" ps aux
    read -rp "Do you want to run 'docker compose down -v'? (y/n): " choice
    if [[ "$choice" =~ ^[Yy]$ ]]; then
      docker compose down -v
    fi
    exit 1
  fi

  echo "[INFO] Waiting... $((RETRIES * RETRY_INTERVAL))s elapsed"
  sleep "$RETRY_INTERVAL"
  RETRIES=$((RETRIES + 1))
done
