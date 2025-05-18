#!/bin/bash

# Script: create_airflow_connections.sh
# Description: Creates Airflow connections using CLI inside Bitnami Airflow container (Airflow 3, idempotent).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

# Load env vars
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

echo "[INFO] Creating Airflow connections inside $AIRFLOW_CONTAINER..."

# Helper function
add_connection() {
  local conn_id="$1"
  local conn_uri="$2"

  echo -n "[INFO] Checking $conn_id... "
  if docker exec "$AIRFLOW_CONTAINER" airflow connections get "$conn_id" >/dev/null 2>&1; then
    echo "exists → deleting"
    docker exec "$AIRFLOW_CONTAINER" airflow connections delete "$conn_id"
  else
    echo "not found"
  fi

  echo "[ADD] Creating $conn_id"
  docker exec "$AIRFLOW_CONTAINER" airflow connections add "$conn_id" --conn-uri "$conn_uri"
}

add_connection "postgres_project_connection" \
  "postgresql://${PROJECT_USER}:${PROJECT_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE_NAME}"

add_connection "mongo_project_connection" \
  "mongo://${PROJECT_USER}:${PROJECT_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${MONGO_DATABASE_NAME}"

add_connection "kafka_project_connection" \
  "kafka://${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}"

add_connection "spark_project_connection" \
  "spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"

add_connection "s3_project_connection" \
  "s3://${PROJECT_USER}:${PROJECT_PASSWORD}@${MINIO_HOST}:${MINIO_PORT}"

add_connection "trino_project_connection" \
  "trino://${PROJECT_USER}:${PROJECT_PASSWORD}@${TRINO_HOST}:${TRINO_HTTP_PORT}/iceberg"

echo "[SUCCESS] All Airflow project connections created or updated."
