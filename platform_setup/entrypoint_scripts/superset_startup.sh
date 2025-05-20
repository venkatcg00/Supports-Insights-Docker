#!/bin/bash

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h "${SUPERSET_DATABASE_HOST}" -p "${SUPERSET_DATABASE_PORT_NUMBER}" -U postgres -d "${SUPERSET_DATABASE_NAME}"; do
  echo "PostgreSQL is not ready yet. Retrying in 5 seconds..."
  sleep 5
done
echo "PostgreSQL is ready!"

export PGPASSWORD=${PROJECT_PASSWORD}

# Test database connectivity
echo "Testing database connectivity..."
if ! psql -h "${SUPERSET_DATABASE_HOST}" -p "${SUPERSET_DATABASE_PORT_NUMBER}" -U postgres -d "${SUPERSET_DATABASE_NAME}" -c "SELECT 1;" > /dev/null 2>&1; then
  echo "Error: Failed to connect to PostgreSQL database."
  exit 1
fi
echo "Database connectivity test passed."

# Set environment variables for Superset
export SUPERSET__SQLALCHEMY_URI="postgresql://postgres:${PROJECT_PASSWORD}@${SUPERSET_DATABASE_HOST}:${SUPERSET_DATABASE_PORT_NUMBER}/${SUPERSET_DATABASE_NAME}"
export SUPERSET_SECRET_KEY="${PROJECT_PASSWORD}"

# Initialize the Superset database
echo "Running Superset database upgrade..."
/opt/bitnami/superset/venv/bin/superset db upgrade

# Check if admin user exists
echo "Checking for existing admin user..."
if /opt/bitnami/superset/venv/bin/superset fab list-users | grep -q "${PROJECT_USER}"; then
  echo "Admin user ${PROJECT_USER} already exists. Skipping creation."
else
  echo "Creating Superset admin user..."
  /opt/bitnami/superset/venv/bin/superset fab create-admin \
    --username "${PROJECT_USER}" \
    --firstname "${FIRST_NAME}" \
    --lastname "${LAST_NAME}" \
    --email "${EMAIL_ADDRESS}" \
    --password "${PROJECT_PASSWORD}"
fi

# Initialize Superset (run only if not initialized)
if [ ! -f "/app/superset_home/.init-done" ]; then
  echo "Initializing Superset..."
  /opt/bitnami/superset/venv/bin/superset init
  touch /app/superset_home/.init-done
else
  echo "Superset already initialized. Skipping init."
fi

# Start Superset
echo "Starting Superset server..."
exec /opt/bitnami/superset/venv/bin/superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger