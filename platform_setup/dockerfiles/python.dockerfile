FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk netcat-openbsd curl python3-lxml && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Pre-install Python packages
RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    pymongo \
    aiohttp \
    polars \
    minio \
    psutil \
    requests \
    confluent-kafka \
    avro-python3 \
    pyarrow \
    pyiceberg \
    trino

WORKDIR /app