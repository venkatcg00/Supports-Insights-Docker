#!/bin/bash

TRINO_CATALOG_DIR=/etc/trino/catalog

cat > "$TRINO_CATALOG_DIR/iceberg.properties" <<EOF
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.file-format=${ICEBERG_FILE_FORMAT}

iceberg.jdbc-catalog.catalog-name=iceberg
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${HIVE_METASTORE_DATABASE}
iceberg.jdbc-catalog.connection-user=${POSTGRES_DEFAULT_USER}
iceberg.jdbc-catalog.connection-password=${PROJECT_PASSWORD}
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.default-warehouse-dir=s3://${MINIO_CLICKSTREAM_TELEMETRY_BUCKET}

fs.native-s3.enabled=true
s3.endpoint=http://${MINIO_HOST}:${MINIO_PORT}
s3.aws-access-key=${PROJECT_USER}
s3.aws-secret-key=${PROJECT_PASSWORD}
s3.path-style-access=${S3_PATH_STYLE_ACCESS}
s3.region=us-east-1
EOF


# Wait for dependencies
sleep 10

# Start Trino
/usr/lib/trino/bin/run-trino