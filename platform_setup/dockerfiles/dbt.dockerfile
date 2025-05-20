FROM python:3.12.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install dbt-core and dbt-trino
RUN pip install --no-cache-dir dbt-core==1.8.6 dbt-trino==1.8.1

# Copy dbt profiles.yml
COPY profiles.yml /root/.dbt/profiles.yml

# Default command to keep container running
CMD ["bash"]