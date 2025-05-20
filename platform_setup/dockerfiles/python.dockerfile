FROM python:3.12.10-slim

# Install system dependencies in a single layer and clean up
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    netcat-openbsd \
    curl \
    python3-lxml && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/*

# Pre-install Python packages with version locking
RUN pip install --no-cache-dir \
    sqlalchemy==2.0.35 \
    psycopg2-binary==2.9.10 \
    pymongo==4.10.1 \
    aiohttp==3.10.10 \
    boto3==1.35.39 \
    confluent-kafka==2.5.3 \
    avro-python3==1.10.2 && \
    # Clean up pip cache and temporary files
    rm -rf /root/.cache/pip/* /tmp/*

WORKDIR /app