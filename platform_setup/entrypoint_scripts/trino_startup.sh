#!/bin/bash

# Create data directory
mkdir -p $TRINO_DATA_DIR/iceberg-data
chmod -R 777 $TRINO_DATA_DIR/iceberg-data

# Copy PostgreSQL JDBC driver to Trino's lib directory
cp /usr/lib/trino/plugin/iceberg/postgresql-jdbc.jar /usr/lib/trino/lib/

# Debug: List files in key directories
ls -l /etc/trino/
ls -l /etc/trino/catalog/
ls -l /usr/lib/trino/lib/

# Wait for dependencies
sleep 10

# Start Trino
exec /usr/lib/trino/bin/launcher run