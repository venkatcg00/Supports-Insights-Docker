#!/bin/bash

# Idempotently add Superset DB connections (Postgres + Trino) using YAML within Superset container

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
until docker exec $SUPERSET_CONTAINER curl -f http://localhost:8088/health > /dev/null 2>&1; do
  echo "Superset is not healthy yet. Retrying in 5 seconds..."
  sleep 5
done
echo "[INFO] Superset is healthy."

echo "[INFO] Adding Superset DB connections"

# Create temporary YAML file in user-writable directory
mkdir -p "$HOME/tmp"
TEMP_YAML="$HOME/tmp/protected_superset_dbs.yaml"
cat <<EOF > "$TEMP_YAML"
databases:
  - database_name: Postgres_Project_DB
    sqlalchemy_uri: postgresql://${POSTGRES_DEFAULT_USER}:${PROJECT_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE_NAME}
    extra: "{}"
    allow_dml: true
EOF

# Copy YAML file to container
docker cp "$TEMP_YAML" "$SUPERSET_CONTAINER:/tmp/protected_superset_dbs.yaml"

# Import database connections
echo "[INFO] Importing database connections..."
if ! docker exec "$SUPERSET_CONTAINER" superset import-datasources --path /tmp/protected_superset_dbs.yaml 2>&1; then
  echo "[ERROR] Failed to import datasources. Check Superset version and supported flags."
  echo "[INFO] Run 'docker exec $SUPERSET_CONTAINER superset import-datasources --help' for available options."
  rm -f "$TEMP_YAML"
  docker exec -u root "$SUPERSET_CONTAINER" rm -f /tmp/protected_superset_dbs.yaml || echo "[WARNING] Failed to remove /tmp/protected_superset_dbs.yaml in container"
  exit 1
fi

# Clean up
rm -f "$TEMP_YAML"
docker exec -u root "$SUPERSET_CONTAINER" rm -f /tmp/protected_superset_dbs.yaml || echo "[WARNING] Failed to remove /tmp/protected_superset_dbs.yaml in container"

echo "[SUCCESS] Superset DB connections created or updated."