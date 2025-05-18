# Use the official Bitnami Superset image as the base
FROM bitnami/superset:4.1.2

# Switch to root user to install dependencies and modify permissions
USER root

# Install PostgreSQL client to test database connectivity
RUN apt-get update && apt-get install -y nano curl postgresql-client g++ && apt-get clean

# Ensure pip and setuptools are modern
RUN pip install --upgrade pip setuptools wheel pillow psycopg2-binary==2.9.10

# Install Superset Python package for scripting access
RUN pip install \
  apache-superset==2.1.0 \
  werkzeug==2.1.2 \
  Flask==2.1.3 \
  flask-wtf==1.0.1 \
  sqlparse==0.4.3 \
  marshmallow==3.19.0 \
  marshmallow_enum==1.5.1

# Create a PostgreSQL user for Superset and adjust permissions
RUN mkdir -p /app/bitnami/superset_home/clickstream_telemetry/dashboards \
    && mkdir -p /app/bitnami/superset_home/support_insights/dashboards \
    && chown -R 1001:1001 /app/bitnami/superset_home \
    && chmod -R 755 /app/bitnami/superset_home

# Switch back to the default non-root user (UID 1001)
USER 1001

# Set the entrypoint to the custom startup script
ENTRYPOINT ["/superset_startup.sh"]