FROM apache/airflow:3.0.2rc2-python3.11

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk nano curl gcc postgresql-client && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow[postgres,mongo,apache.kafka,amazon,trino,webserver]==3.0.2rc2" \
    pandas \
    polars \
    pyspark==3.5.6 \
    pymongo \
    dnspython \
    asyncpg \
    connexion[swagger-ui]
