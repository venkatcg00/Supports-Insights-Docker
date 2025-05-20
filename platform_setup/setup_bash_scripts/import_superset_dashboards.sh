#!/bin/bash

# Script: import_superset_dashboards.sh
# Description: Imports dashboards into Superset from clickstream and support folders (idempotent).

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

echo "[INFO] Importing dashboards into Superset..."

DASHBOARD_DIRS=(
  "/app/bitnami/superset_home/clickstream_telemetry/dashboards"
  "/app/bitnami/superset_home/support_insights/dashboards"
)

for DIR in "${DASHBOARD_DIRS[@]}"; do
  echo "[IMPORT] Scanning directory: $DIR"
  docker exec "$SUPERSET_CONTAINER" sh -c "
    if [ -d \"$DIR\" ]; then
      FOUND=0
      for f in \"$DIR\"/*.json; do
        if [ -f \"\$f\" ]; then
          FOUND=1
          echo \"[IMPORTING] \$f\"
          /opt/bitnami/superset/venv/bin/superset import-dashboards --path \"\$f\"
        fi
      done
      if [ \"\$FOUND\" -eq 0 ]; then
        echo \"[INFO] No dashboards found in $DIR\"
      fi
    else
      echo \"[SKIP] Directory does not exist: $DIR\"
    fi
  "
done

echo "[SUCCESS] Dashboard import process completed — either imported new dashboards or none were present."