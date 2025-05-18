#!/bin/bash

# Script: create_minio_buckets.sh
# Description: Creates MinIO buckets defined in .env using mc inside the MinIO container.

set -e

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

# Load .env
if [ ! -f "$ENV_FILE" ]; then
  echo "[ERROR] .env file not found at $ENV_FILE"
  exit 1
fi
source "$ENV_FILE"

# Validate essential vars
for var in MINIO_HOST MINIO_PORT PROJECT_USER PROJECT_PASSWORD; do
  if [ -z "${!var}" ]; then
    echo "[ERROR] Environment variable $var is not set"
    exit 1
  fi
done

# Collect bucket names
BUCKETS=$(grep -E '^[A-Z0-9_]+_BUCKET=' "$ENV_FILE" \
  | cut -d '=' -f 2 \
  | tr '\n' ' ')
if [ -z "$BUCKETS" ]; then
  echo "[INFO] No *_BUCKET entries found in .env – nothing to do."
  exit 0
fi

echo "[INFO] Buckets to ensure exist: $BUCKETS"

# Detect MinIO container by name first, then by image
if docker ps --format '{{.Names}}' | grep -qw minio; then
  MINIO_CONTAINER=minio
elif docker ps --filter "ancestor=minio/minio" --format '{{.Names}}' | grep -q .; then
  MINIO_CONTAINER=$(docker ps --filter "ancestor=minio/minio" --format '{{.Names}}' | head -n1)
else
  echo "[ERROR] Could not find a running MinIO container (by name or image)."
  exit 1
fi

echo "[INFO] Using MinIO container: $MINIO_CONTAINER"

# Ensure mc alias is registered (idempotent)
docker exec "$MINIO_CONTAINER" mc alias set localminio \
  "http://${MINIO_HOST}:${MINIO_PORT}" \
  "$PROJECT_USER" "$PROJECT_PASSWORD" \
  >/dev/null 2>&1 || true

# Create each bucket if missing
for bucket in $BUCKETS; do
  echo -n "[INFO] Checking bucket '$bucket'… "
  if docker exec "$MINIO_CONTAINER" mc ls "localminio/$bucket" >/dev/null 2>&1; then
    echo "exists."
  else
    echo "creating…"
    docker exec "$MINIO_CONTAINER" mc mb "localminio/$bucket"
  fi
done

echo "[SUCCESS] All MinIO buckets are now present."
