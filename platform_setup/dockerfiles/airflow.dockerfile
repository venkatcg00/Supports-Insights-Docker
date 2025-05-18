FROM bitnami/airflow:3.0.0

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk nano curl gcc postgresql-client && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    mkdir -p /opt/bitnami/airflow && \
    chown -R 1001:1001 /opt/bitnami/airflow && \
    chmod -R 755 /opt/bitnami/airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    polars \
    apache-airflow-providers-postgres \
    apache-airflow-providers-mongo \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-amazon \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-fab \
    avro-python3 \
    pyarrow \
    pyiceberg \
    trino

USER 1001