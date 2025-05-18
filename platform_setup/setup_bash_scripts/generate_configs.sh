#!/bin/bash

# generate_configs.sh
# Creates all Trino configs and JDBC jar using .env variables.
# Supports standalone or platform_setup.sh usage.

set -e

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

# Validate and load .env
if [ -z "$PROJECT_USER" ]; then
    if [ ! -f "$ENV_FILE" ]; then
        echo "[ERROR] .env file not found at $ENV_FILE"
        exit 1
    fi
    echo "[INFO] Sourcing environment from $ENV_FILE"
    source "$ENV_FILE"
fi

# Defaults
TRINO_HOST=${TRINO_HOST:-trino}

# Required vars
REQUIRED_VARS=(
  PROJECT_USER PROJECT_PASSWORD POSTGRES_HOST POSTGRES_PORT
  POSTGRES_DEFAULT_USER HIVE_METASTORE_DATABASE
  MINIO_HOST MINIO_PORT MINIO_CLIENT_GAMMA_STORAGE_BUCKET
  ICEBERG_FILE_FORMAT S3_PATH_STYLE_ACCESS
  TRINO_ENVIRONMENT TRINO_DATA_DIR
)

for var in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!var}" ]; then
    echo "[ERROR] Environment variable $var is not set."
    exit 1
  fi
done

# Paths
TRINO_CONFIG_DIR="$PROJECT_ROOT/trino/etc"
TRINO_CATALOG_DIR="$TRINO_CONFIG_DIR/catalog"
JARS_DIR="$PROJECT_ROOT/jars"
JDBC_JAR="$JARS_DIR/postgresql-jdbc.jar"

mkdir -p "$TRINO_CATALOG_DIR" "$JARS_DIR"
chmod u+w "$JARS_DIR"

# --- JDBC JAR Download ---
PRIMARY_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
FALLBACK_URL="https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"

download_jar() {
  local url=$1
  local out=$2
  if command -v wget &>/dev/null; then
    wget --inet4-only -O "$out" "$url"
  elif command -v curl &>/dev/null; then
    curl -L -o "$out" "$url"
  else
    echo "[ERROR] Neither wget nor curl available to download JDBC JAR."
    exit 1
  fi
}

if [ -d "$JDBC_JAR" ]; then
  echo "[WARN] Removing directory named postgresql-jdbc.jar"
  rm -rf "$JDBC_JAR"
fi

if [ ! -f "$JDBC_JAR" ]; then
  echo "[INFO] Downloading JDBC jar..."
  download_jar "$PRIMARY_URL" "$JDBC_JAR" || {
    echo "[WARN] Primary download failed, retrying fallback..."
    rm -f "$JDBC_JAR"
    download_jar "$FALLBACK_URL" "$JDBC_JAR" || {
      echo "[ERROR] Failed to download JDBC jar from both URLs."
      exit 1
    }
  }

  if ! unzip -l "$JDBC_JAR" | grep -q "META-INF/MANIFEST.MF"; then
    echo "[ERROR] Downloaded JDBC jar is invalid."
    head -n 10 "$JDBC_JAR"
    rm -f "$JDBC_JAR"
    exit 1
  fi

  echo "[INFO] JDBC jar downloaded successfully."
else
  echo "[INFO] JDBC jar already exists."
fi

# --- Create iceberg.properties ---
cat > "$TRINO_CATALOG_DIR/iceberg.properties" <<EOF
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.file-format=${ICEBERG_FILE_FORMAT}

iceberg.jdbc-catalog.catalog-name=iceberg
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${HIVE_METASTORE_DATABASE}
iceberg.jdbc-catalog.connection-user=${POSTGRES_DEFAULT_USER}
iceberg.jdbc-catalog.connection-password=${PROJECT_PASSWORD}
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.default-warehouse-dir=s3://${MINIO_CLIENT_GAMMA_STORAGE_BUCKET}/warehouse/

fs.native-s3.enabled=true
s3.endpoint=http://${MINIO_HOST}:${MINIO_PORT}
s3.aws-access-key=${PROJECT_USER}
s3.aws-secret-key=${PROJECT_PASSWORD}
s3.path-style-access=${S3_PATH_STYLE_ACCESS}
s3.region=us-east-1
EOF

# --- config.properties ---
cat > "$TRINO_CONFIG_DIR/config.properties" <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://${TRINO_HOST}:8080
http-server.process-forwarded=true
EOF

# --- node.properties ---
cat > "$TRINO_CONFIG_DIR/node.properties" <<EOF
node.environment=${TRINO_ENVIRONMENT}
node.data-dir=${TRINO_DATA_DIR}
plugin.dir=/usr/lib/trino/plugin
EOF

# --- jvm.config ---
cat > "$TRINO_CONFIG_DIR/jvm.config" <<EOF
-server
-Xmx2G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+UseGCOverheadLimit
-XX:+ExitOnOutOfMemoryError
-XX:ReservedCodeCacheSize=256M
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
EOF

# --- log.properties ---
cat > "$TRINO_CONFIG_DIR/log.properties" <<EOF
io.trino=INFO
org.apache.iceberg=INFO
EOF

# --- Summary ---
echo -e "\\Configuration complete:"
echo "- iceberg.properties"
echo "- config.properties"
echo "- node.properties"
echo "- jvm.config"
echo "- log.properties"
echo "- JDBC jar: $JDBC_JAR"
