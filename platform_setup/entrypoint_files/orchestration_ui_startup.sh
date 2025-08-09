#!/bin/bash
# Entrypoint script for the Orchestration_UI container in the Data Generator Suite.
# Performs checks, sets up SQLite database, creates folders, and runs the orchestrator.
# Mount at /app/orchestration_ui_startup.sh via `../entrypoint_scripts/orchestration_ui_startup.sh:/app/orchestration_ui_startup.sh`.
# Ensure the host path is a file, not a directory, and has executable permissions (`chmod +x`).
# Uses sqlite3 CLI for database setup.

set -e

# Temporary log function until /app/logs is created
early_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ENTRYPOINT - $1" >&2
}

early_log "INFO - Starting entrypoint setup"

# Create /app/logs first to ensure logging works
LOG_DIR="/app/logs"
if [ ! -d "$LOG_DIR" ]; then
    early_log "INFO - Creating log directory: $LOG_DIR"
    if ! mkdir -p "$LOG_DIR" || ! chmod 755 "$LOG_DIR"; then
        early_log "ERROR - Failed to create log directory: $LOG_DIR"
        exit 1
    else
        early_log "INFO - Created log directory: $LOG_DIR"
    fi
fi
if ! test -w "$LOG_DIR"; then
    early_log "ERROR - Log directory not writable: $LOG_DIR"
    exit 1
fi

# Initialize logging to file and stdout
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/python_startup_${TIMESTAMP}.log"
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ENTRYPOINT - $1" | tee -a "$LOG_FILE"
}

log "INFO - Log file initialized: $LOG_FILE"

# Initialize error flag
ERROR_FOUND=0

# Check environment variables
log "INFO - Checking environment variables"
EXPECTED_ENV_VARS=(
    MONGO_HOST
    MONGO_PORT
    MONGO_USER
    MONGO_PASSWORD
    MONGO_DB
    MINIO_HOST
    MINIO_PORT
    MINIO_USER
    MINIO_PASSWORD
    MINIO_CLIENT_GAMMA_STORAGE_BUCKET
    MINIO_CLICKSTREAM_TELEMETRY_BUCKET
    KAFKA_BOOTSTRAP_SERVERS
    KAFKA_TELEMETRY_VEHICLE_STATS_TOPIC
    KAFKA_ANALYTICS_SESSION_DURATION_TOPIC
    KAFKA_CLIENT_BETA_STORAGE_TOPIC
    CHUNK_SIZE
    SQLITE_DB_FILE
)
for var in "${EXPECTED_ENV_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        log "ERROR - Missing environment variable: $var"
        ERROR_FOUND=1
    fi
done

# Check orchestrator script
ORCHESTRATOR_SCRIPT="/app/scripts/python_scripts_orchestrator/data_generator_orchestrator.py"
log "INFO - Checking orchestrator script: $ORCHESTRATOR_SCRIPT"
if [ ! -f "$ORCHESTRATOR_SCRIPT" ]; then
    log "ERROR - Orchestrator script not found: $ORCHESTRATOR_SCRIPT"
    ERROR_FOUND=1
else
    log "INFO - Orchestrator script found"
fi

# Check and create remaining folders
log "INFO - Checking and creating folders"
FOLDERS=(
    /app/storage
    /app/scripts/support_insights
    /app/scripts/clickstream_telemetry
    /app/scripts/python_scripts_orchestrator
    /app/tests/support_insights_tests
    /app/tests/clickstream_telemetry
)
for dir in "${FOLDERS[@]}"; do
    if [ ! -d "$dir" ]; then
        log "INFO - Creating directory: $dir"
        if ! mkdir -p "$dir" || ! chmod 755 "$dir"; then
            log "ERROR - Failed to create directory: $dir"
            ERROR_FOUND=1
        else
            log "INFO - Created directory: $dir"
        fi
    fi
    if [ -d "$dir" ] && ! test -w "$dir"; then
        log "ERROR - Directory not writable: $dir"
        ERROR_FOUND=1
    else
        log "INFO - Directory accessible: $dir"
    fi
done

# Log permissions and user for debugging
log "INFO - Current user: $(whoami)"
log "INFO - Permissions of /app/storage: $(ls -ld /app/storage)"

# Setup SQLite database using sqlite3 CLI
SQL_SCRIPT="/app/sqlite3_db_setup.sql"
log "INFO - Setting up SQLite database: $SQLITE_DB_FILE"

# Rename existing database if present
if [ -f "$SQLITE_DB_FILE" ]; then
    log "INFO - Existing database found, renaming to $SQLITE_DB_FILE.bak"
    mv "$SQLITE_DB_FILE" "$SQLITE_DB_FILE.bak" 2>>"$LOG_FILE" || log "WARNING - Failed to rename existing database"
fi

# Test SQLite file creation
log "INFO - Testing SQLite file creation"
if ! sqlite3 /app/storage/test.db "CREATE TABLE test (id INTEGER);" 2>>"$LOG_FILE"; then
    log "ERROR - Failed to create test SQLite database"
    ERROR_FOUND=1
else
    log "INFO - Test SQLite database created successfully"
    rm -f /app/storage/test.db 2>>"$LOG_FILE" || log "WARNING - Failed to remove test database"
fi

# Execute SQL script
if [ ! -f "$SQL_SCRIPT" ]; then
    log "ERROR - SQL script not found: $SQL_SCRIPT"
    ERROR_FOUND=1
else
    # Log SQL script content
    log "INFO - SQL script content: $(cat $SQL_SCRIPT)"
    if ! sqlite3 "$SQLITE_DB_FILE" < "$SQL_SCRIPT" 2>>"$LOG_FILE"; then
        log "ERROR - Failed to set up SQLite database: $DB_FILE"
        ERROR_FOUND=1
    else
        log "INFO - SQLite database setup complete"
        chmod 664 "$SQLITE_DB_FILE" 2>/dev/null || log "WARNING - Failed to set permissions on $SQLITE_DB_FILE"
    fi
fi

# Check for errors and exit if any
if [ "$ERROR_FOUND" -eq 1 ]; then
    log "ERROR - One or more setup steps failed; exiting"
    exit 1
fi

# Run orchestrator
log "INFO - Starting orchestrator: $ORCHESTRATOR_SCRIPT"
exec python "$ORCHESTRATOR_SCRIPT"