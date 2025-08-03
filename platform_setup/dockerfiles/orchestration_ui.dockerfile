FROM python:3.11-slim

# Install system dependencies in a single layer and clean up
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    netcat-openbsd \
    curl \
    python3-lxml \
    sqlite3 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/*

# Pre-install Python packages with version locking
RUN pip install --no-cache-dir \
    pymongo \
    aiohttp \
    boto3 \
    confluent-kafka \
    faker \
    tenacity \
    avro-python3 && \
    # Clean up pip cache and temporary files
    rm -rf /root/.cache/pip/* /tmp/*

# Create directories
RUN mkdir -p /app && \
    mkdir -p /app/scripts && \
    mkdir -p /app/logs && \
    mkdir -p /app/tests && \
    mkdir -p /app/scripts/support_insights && \
    mkdir -p /app/tests/support_insights_tests && \
    mkdir -p /app/scripts/clickstream_telemetry && \
    mkdir -p /app/tests/clickstream_telemetry && \
    mkdir -p /app/storage

# Set permissions and ownership
RUN chmod 755 /app && \
    chmod 755 /app/scripts && \
    chmod 755 /app/logs && \
    chmod 755 /app/tests && \
    chmod 755 /app/scripts/support_insights && \
    chmod 755 /app/tests/support_insights_tests && \
    chmod 755 /app/scripts/clickstream_telemetry && \
    chmod 755 /app/tests/clickstream_telemetry && \
    chmod 755 /app/storage && \
    chown -R $(whoami) /app/storage

WORKDIR /app