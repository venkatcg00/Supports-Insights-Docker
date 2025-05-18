#!/bin/bash

# Script: create_kafka_topics.sh
# Description: Creates Kafka topics defined in .env using kafka-topics.sh inside Bitnami Kafka container.

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

# Validate required env
for var in KAFKA_BROKER_HOST KAFKA_BROKER_PORT; do
  if [ -z "${!var}" ]; then
    echo "[ERROR] Environment variable $var is not set"
    exit 1
  fi
done

# Collect all *_TOPIC values
TOPICS=$(grep -E '^[A-Z0-9_]+_TOPIC=' "$ENV_FILE" | cut -d '=' -f2 | tr '\n' ' ')

if [ -z "$TOPICS" ]; then
  echo "[INFO] No *_TOPIC entries found in .env"
  exit 0
fi

# Detect Kafka container
if docker ps --format '{{.Names}}' | grep -qw kafka; then
  KAFKA_CONTAINER=kafka
elif docker ps --filter "ancestor=bitnami/kafka" --format '{{.Names}}' | grep -q .; then
  KAFKA_CONTAINER=$(docker ps --filter "ancestor=bitnami/kafka" --format '{{.Names}}' | head -n1)
else
  echo "[ERROR] Kafka container not found (by name or image)."
  exit 1
fi

echo "[INFO] Using Kafka container: $KAFKA_CONTAINER"
echo "[INFO] Topics to ensure: $TOPICS"

# Create topics
for topic in $TOPICS; do
  echo -n "[INFO] Checking topic '$topic'… "
  if docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server "$KAFKA_BROKER_HOST:$KAFKA_BROKER_PORT" --topic "$topic" --describe >/dev/null 2>&1; then
    echo "exists."
  else
    echo "creating…"
    docker exec "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server "$KAFKA_BROKER_HOST:$KAFKA_BROKER_PORT" --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
  fi
done

echo "[SUCCESS] Kafka topic setup complete."
