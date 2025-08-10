FROM apache/superset:b3f436a-py311

USER root

# Install essential system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    python3-dev \
    libpq-dev \
    postgresql-client \
    nano \
    curl \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/*

# Bootstrap pip inside the Superset venv (if missing)
RUN /app/.venv/bin/python3 -m ensurepip --upgrade

# Install Python packages inside Superset's venv
RUN /app/.venv/bin/python3 -m pip install --no-cache-dir \
    psycopg2-binary \
    Pillow && \
    rm -rf /root/.cache/pip/* /tmp/*

# Setup dashboard directories and permissions
RUN mkdir -p /app/superset_home/support_insights/dashboards && \
    chown -R superset:superset /app/superset_home && \
    chmod -R 755 /app/superset_home

USER superset

ENTRYPOINT ["/app/superset_startup.sh"]
