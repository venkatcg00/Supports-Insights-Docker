FROM bitnami/superset:4.1.2

USER root

# Install essential dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client \
    nano \
    curl \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/*

# Install psycopg2-binary and Pillow
RUN pip install --no-cache-dir psycopg2-binary==2.9.10 Pillow==10.4.0 && \
    rm -rf /root/.cache/pip/* /tmp/*

# Create dashboard directories and set permissions
RUN mkdir -p /app/superset_home/clickstream_telemetry/dashboards \
    /app/superset_home/support_insights/dashboards && \
    chown -R 1001:1001 /app/superset_home && \
    chmod -R 755 /app/superset_home

USER 1001

# Use custom entrypoint
ENTRYPOINT ["/app/superset_startup.sh"]