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
    psycopg2-binary==2.9.10 \
    pandas==2.1.4 \
    polars==0.20.2 \
    pyspark==3.4.1 \
    apache-airflow-providers-postgres==5.13.0 --no-deps \
    apache-airflow-providers-mongo==4.1.0 --no-deps \
    apache-airflow-providers-apache-kafka==1.4.0 --no-deps \
    apache-airflow-providers-amazon==8.25.0 --no-deps \
    apache-airflow-providers-fab==1.3.0 --no-deps \
    apache-airflow-providers-trino==5.8.0 --no-deps \
    avro-python3==1.10.2 \
    pyarrow==14.0.2 && \
    # Clean up pip cache and temporary files
    rm -rf /root/.cache/pip/* /tmp/*

USER 1001