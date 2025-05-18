#!/bin/bash

# Idempotently add Superset DB connections (Postgres + Trino) using Python within Superset container

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

echo "[INFO] Adding Superset DBs using Python script inside $SUPERSET_CONTAINER"

docker exec -i "$SUPERSET_CONTAINER" python3 <<EOF
from superset.app import create_app

app = create_app()

with app.app_context():
    from superset import db, security_manager
    from superset.models.core import Database

    def add_or_update_connection(name, uri):
        existing = db.session.query(Database).filter_by(database_name=name).first()
        if existing:
            print(f"[UPDATE] Connection '{name}' already exists, updating URI.")
            existing.sqlalchemy_uri = uri
            existing.extra = '{}'
        else:
            print(f"[ADD] Creating new connection '{name}'.")
            new_db = Database(
                database_name=name,
                sqlalchemy_uri=uri,
                extra='{}',
                created_by_fk=security_manager.find_user('admin').id if security_manager.find_user('admin') else None
            )
            db.session.add(new_db)
        db.session.commit()

    add_or_update_connection("Postgres_Project_DB", "postgresql://${PROJECT_USER}:${PROJECT_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE_NAME}")
    add_or_update_connection("Trino_Project_DB", "trino://${PROJECT_USER}:${PROJECT_PASSWORD}@${TRINO_HOST}:${TRINO_HTTP_PORT}/iceberg")
EOF

echo "[SUCCESS] Superset DB connections created or updated."
