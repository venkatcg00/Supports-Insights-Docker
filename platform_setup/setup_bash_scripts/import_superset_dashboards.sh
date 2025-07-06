#!/bin/bash

# Script: import_superset_dashboards.sh
# Description: Imports dashboards into Superset from ZIP file(s) placed in bind path (idempotent).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "[ERROR] .env file not found at $ENV_FILE"
  exit 1
fi

source "$ENV_FILE"

# Detect Superset container
if docker ps --format '{{.Names}}' | grep -qw superset; then
  SUPERSET_CONTAINER=superset
elif docker ps --filter "ancestor=bitnami/superset" --format '{{.Names}}' | grep -q .; then
  SUPERSET_CONTAINER=$(docker ps --filter "ancestor=bitnami/superset" --format '{{.Names}}' | head -n1)
else
  echo "[ERROR] Superset container not found"
  exit 1
fi

# Wait for Superset to be healthy
echo "[INFO] Waiting for Superset to be healthy..."
until docker exec "$SUPERSET_CONTAINER" curl -f http://localhost:8088/health > /dev/null 2>&1; do
  echo "Superset is not healthy yet. Retrying in 5 seconds..."
  sleep 5
done
echo "[INFO] Superset is healthy."

echo "[INFO] Importing dashboard ZIP exports into Superset..."

ZIP_PATHS=(
  "/app/bitnami/superset_home/clickstream_telemetry/dashboard_export.zip"
  "/app/bitnami/superset_home/support_insights/dashboard_export.zip"
)

for ZIP in "${ZIP_PATHS[@]}"; do
  echo "[IMPORT] Checking for ZIP: $ZIP"
  docker exec "$SUPERSET_CONTAINER" sh -c "
    if [ -f \"$ZIP\" ]; then
      echo \"[IMPORTING ZIP] $ZIP\"
      superset import-dashboards --path \"$ZIP\" --username \"$PROJECT_USER\"
    else
      echo \"[SKIP] ZIP file not found: $ZIP\"
    fi
  "
done

echo "[SUCCESS] Dashboard import from ZIP files completed."
