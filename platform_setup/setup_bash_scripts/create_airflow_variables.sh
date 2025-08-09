#!/bin/bash

# Script: create_airflow_variables.sh
# Description: Adds *_BUCKET and *_TOPIC variables from .env as Airflow variables inside Airflow container.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

# Load .env
if [ ! -f "$ENV_FILE" ]; then
  echo "[ERROR] .env file not found at $ENV_FILE"
  exit 1
fi

source "$ENV_FILE"

# Detect Airflow container by name first, then fallback to image
if docker ps --format '{{.Names}}' | grep -qw airflow; then
  AIRFLOW_CONTAINER=airflow
elif docker ps --filter "ancestor=bitnami/airflow" --format '{{.Names}}' | grep -q .; then
  AIRFLOW_CONTAINER=$(docker ps --filter "ancestor=bitnami/airflow" --format '{{.Names}}' | head -n1)
else
  echo "[ERROR] Airflow container not found (by name or image)."
  exit 1
fi

echo "[INFO] Adding Airflow variables from .env..."

# Process *_BUCKET and *_TOPIC
grep -E '(_BUCKET=|_TOPIC=)' "$ENV_FILE" | while IFS='=' read -r key value; do
  if [ -n "$key" ] && [ -n "$value" ]; then
    echo "[SET] Variable $key = $value"
    docker exec "$AIRFLOW_CONTAINER" airflow variables set "$key" "$value"
  fi
done

echo "[SUCCESS] Airflow variables created."
