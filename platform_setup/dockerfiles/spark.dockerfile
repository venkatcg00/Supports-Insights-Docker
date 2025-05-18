FROM bitnami/spark:3.4.1

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk netcat-openbsd curl python3-lxml && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    pymongo \
    aiohttp \
    polars \
    boto3 \
    psutil \
    requests \
    confluent-kafka \
    avro-python3 \
    pyarrow \
    pyiceberg \
    trino

USER 1001